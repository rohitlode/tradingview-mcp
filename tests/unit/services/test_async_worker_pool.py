"""``JobQueue`` / ``WorkerPool`` unit tests.

The two properties that actually matter, both proven with real concurrency
(real ``asyncio`` tasks, real ``asyncio.to_thread`` offload of the blocking
runner) rather than a mock that assumes the behaviour:

  1. **De-duplication** — two concurrent requests for the SAME cache key must
     collapse into exactly ONE call to the underlying service function, and
     both callers must receive that one result.
  2. **Bounded concurrency** — with ``concurrency=N`` and N+1 distinct jobs
     queued at once, no more than N runners may ever be executing
     simultaneously.

There is no ``pytest-asyncio`` in this repo's dev dependencies, so each async
scenario is driven from a plain sync test via ``asyncio.run`` — no new
dependency, no plugin configuration.
"""
from __future__ import annotations

import asyncio
import threading
import time

import pytest

from tradingview_mcp.core.services.result_cache import ResultCache, cache_key_for
from tradingview_mcp.core.services.async_worker_pool import JobQueue, WorkerPool


class _CallRecorder:
    """Thread-safe call counter + peak-concurrency tracker for a fake runner."""

    def __init__(self, delay_s: float = 0.0, result=None, exc: Exception | None = None):
        self.delay_s = delay_s
        self.result = result if result is not None else {"ok": True}
        self.exc = exc
        self.calls: list[dict] = []
        self.concurrent = 0
        self.max_concurrent = 0
        self._lock = threading.Lock()
        self.started = threading.Event()

    def __call__(self, **kwargs):
        with self._lock:
            self.calls.append(kwargs)
            self.concurrent += 1
            self.max_concurrent = max(self.max_concurrent, self.concurrent)
        self.started.set()
        try:
            if self.delay_s:
                time.sleep(self.delay_s)
            if self.exc is not None:
                raise self.exc
            return dict(self.result)
        finally:
            with self._lock:
                self.concurrent -= 1

    @property
    def call_count(self) -> int:
        return len(self.calls)


def _make_pool(tmp_path, runner, concurrency=2):
    cache = ResultCache(db_path=tmp_path / "tool_cache.db")
    pool = WorkerPool(cache=cache, runners={"fake_tool": runner}, concurrency=concurrency)
    return cache, pool


class TestDeDuplication:
    def test_concurrent_identical_requests_call_the_service_exactly_once(self, tmp_path):
        """THE critical property: N concurrent submits of the same key => ONE
        real call, and every caller gets that same result."""
        recorder = _CallRecorder(delay_s=0.25, result={"value": 42})
        cache, pool = _make_pool(tmp_path, recorder, concurrency=2)

        async def scenario():
            await pool.start()
            try:
                args = {"symbol": "AAPL", "exchange": "NASDAQ"}
                futures = [pool.submit("fake_tool", args, ttl_s=120.0) for _ in range(5)]
                # All five must be the SAME future object -- attached, not re-enqueued.
                assert all(f is futures[0] for f in futures)
                return await asyncio.gather(*futures)
            finally:
                await pool.stop()

        outcomes = asyncio.run(scenario())

        assert recorder.call_count == 1, f"expected 1 real call, got {recorder.call_count}"
        assert len(outcomes) == 5
        for outcome in outcomes:
            assert outcome.ok is True
            assert outcome.result == {"value": 42}

    def test_second_request_arriving_mid_flight_attaches_to_the_same_job(self, tmp_path):
        """The de-dup window must cover a job that is already RUNNING (popped
        off the queue), not just one still queued."""
        recorder = _CallRecorder(delay_s=0.3, result={"value": 7})
        cache, pool = _make_pool(tmp_path, recorder, concurrency=2)

        async def scenario():
            await pool.start()
            try:
                args = {"symbol": "AAPL"}
                first = pool.submit("fake_tool", args, ttl_s=120.0)
                # Wait until the runner has genuinely STARTED executing.
                await asyncio.to_thread(recorder.started.wait, 5.0)
                second = pool.submit("fake_tool", args, ttl_s=120.0)
                assert second is first
                return await asyncio.gather(first, second)
            finally:
                await pool.stop()

        a, b = asyncio.run(scenario())

        assert recorder.call_count == 1
        assert a.result == b.result == {"value": 7}

    def test_distinct_keys_are_not_de_duplicated(self, tmp_path):
        """De-dup must key on the real cache key -- different args => different jobs."""
        recorder = _CallRecorder(delay_s=0.05)
        cache, pool = _make_pool(tmp_path, recorder, concurrency=2)

        async def scenario():
            await pool.start()
            try:
                futures = [
                    pool.submit("fake_tool", {"symbol": s}, ttl_s=120.0)
                    for s in ("AAPL", "MSFT", "NVDA")
                ]
                return await asyncio.gather(*futures)
            finally:
                await pool.stop()

        asyncio.run(scenario())

        assert recorder.call_count == 3
        assert {c["symbol"] for c in recorder.calls} == {"AAPL", "MSFT", "NVDA"}

    def test_key_is_released_after_completion_so_a_later_refresh_runs_again(self, tmp_path):
        """Once a job finishes, its key must leave the in-flight map -- otherwise
        the cache could never be refreshed after the TTL expires."""
        recorder = _CallRecorder()
        cache, pool = _make_pool(tmp_path, recorder, concurrency=2)

        async def scenario():
            await pool.start()
            try:
                await pool.submit("fake_tool", {"symbol": "AAPL"}, ttl_s=120.0)
                assert not pool.is_inflight(cache_key_for("fake_tool", {"symbol": "AAPL"}))
                await pool.submit("fake_tool", {"symbol": "AAPL"}, ttl_s=120.0)
            finally:
                await pool.stop()

        asyncio.run(scenario())

        assert recorder.call_count == 2


class TestBoundedConcurrency:
    def test_never_more_than_n_runners_execute_at_once(self, tmp_path):
        """concurrency=2 with 5 slow distinct jobs => peak concurrency 2."""
        recorder = _CallRecorder(delay_s=0.2)
        cache, pool = _make_pool(tmp_path, recorder, concurrency=2)

        async def scenario():
            await pool.start()
            try:
                futures = [
                    pool.submit("fake_tool", {"symbol": f"S{i}"}, ttl_s=120.0)
                    for i in range(5)
                ]
                await asyncio.gather(*futures)
            finally:
                await pool.stop()

        asyncio.run(scenario())

        assert recorder.call_count == 5
        assert recorder.max_concurrent <= 2, f"peak concurrency was {recorder.max_concurrent}"
        # And it genuinely ran in parallel, not serially -- otherwise this test
        # would pass trivially with a concurrency of 1.
        assert recorder.max_concurrent == 2

    def test_concurrency_of_one_serializes_completely(self, tmp_path):
        recorder = _CallRecorder(delay_s=0.05)
        cache, pool = _make_pool(tmp_path, recorder, concurrency=1)

        async def scenario():
            await pool.start()
            try:
                await asyncio.gather(*[
                    pool.submit("fake_tool", {"symbol": f"S{i}"}, ttl_s=120.0)
                    for i in range(4)
                ])
            finally:
                await pool.stop()

        asyncio.run(scenario())

        assert recorder.max_concurrent == 1

    def test_pool_starts_exactly_n_worker_tasks(self, tmp_path):
        recorder = _CallRecorder()
        cache, pool = _make_pool(tmp_path, recorder, concurrency=3)

        async def scenario():
            await pool.start()
            n = len(pool.workers)
            await pool.stop()
            return n, len(pool.workers)

        started, after_stop = asyncio.run(scenario())
        assert started == 3
        assert after_stop == 0


class TestCacheWriteThrough:
    def test_successful_job_writes_the_result_to_the_cache(self, tmp_path):
        recorder = _CallRecorder(result={"value": 99})
        cache, pool = _make_pool(tmp_path, recorder, concurrency=2)

        async def scenario():
            await pool.start()
            try:
                await pool.submit("fake_tool", {"symbol": "AAPL"}, ttl_s=120.0)
            finally:
                await pool.stop()

        asyncio.run(scenario())

        assert cache.get("fake_tool", {"symbol": "AAPL"}) == {"value": 99}

    def test_error_envelope_is_cached_under_the_short_negative_ttl(self, tmp_path):
        """A transient upstream failure must not blank a symbol for the full
        success window -- it gets the short negative-cache TTL instead."""
        import sqlite3

        envelope = {"error": {"code": "ALL_BATCHES_FAILED", "message": "upstream cliff"}}
        recorder = _CallRecorder(result=envelope)
        cache = ResultCache(db_path=tmp_path / "tool_cache.db")
        pool = WorkerPool(
            cache=cache, runners={"fake_tool": recorder}, concurrency=1, error_ttl_s=15.0
        )

        async def scenario():
            await pool.start()
            try:
                return await pool.submit("fake_tool", {"symbol": "AAPL"}, ttl_s=120.0)
            finally:
                await pool.stop()

        outcome = asyncio.run(scenario())

        assert outcome.ok is True and outcome.result == envelope
        with sqlite3.connect(str(cache.db_path)) as conn:
            (stored_ttl,) = conn.execute("SELECT ttl_s FROM tool_result_cache").fetchone()
        assert stored_ttl == 15.0, "error envelope was cached under the success TTL"

    def test_successful_result_keeps_the_full_requested_ttl(self, tmp_path):
        """Control for the test above -- a real success is NOT shortened."""
        import sqlite3

        recorder = _CallRecorder(result={"alignment": {"status": "BULLISH"}})
        cache = ResultCache(db_path=tmp_path / "tool_cache.db")
        pool = WorkerPool(
            cache=cache, runners={"fake_tool": recorder}, concurrency=1, error_ttl_s=15.0
        )

        async def scenario():
            await pool.start()
            try:
                await pool.submit("fake_tool", {"symbol": "AAPL"}, ttl_s=120.0)
            finally:
                await pool.stop()

        asyncio.run(scenario())

        with sqlite3.connect(str(cache.db_path)) as conn:
            (stored_ttl,) = conn.execute("SELECT ttl_s FROM tool_result_cache").fetchone()
        assert stored_ttl == 120.0

    def test_cached_error_expires_quickly_while_a_success_stays_fresh(self, tmp_path):
        """End-to-end freshness consequence: after ~20s of wall-clock age, the
        error entry is a miss (retryable) but the success entry is still served."""
        import sqlite3

        cache = ResultCache(db_path=tmp_path / "tool_cache.db")

        def runner(**kwargs):
            if kwargs["symbol"] == "BROKEN":
                return {"error": {"code": "ALL_BATCHES_FAILED", "message": "blip"}}
            return {"ok": True}

        pool = WorkerPool(
            cache=cache, runners={"fake_tool": runner}, concurrency=2, error_ttl_s=15.0
        )

        async def scenario():
            await pool.start()
            try:
                await asyncio.gather(
                    pool.submit("fake_tool", {"symbol": "BROKEN"}, ttl_s=120.0),
                    pool.submit("fake_tool", {"symbol": "GOOD"}, ttl_s=120.0),
                )
            finally:
                await pool.stop()

        asyncio.run(scenario())

        # Age both rows by 20 seconds without sleeping.
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute("UPDATE tool_result_cache SET computed_at = computed_at - 20")

        assert cache.get("fake_tool", {"symbol": "BROKEN"}, ttl_s=120.0) is None
        assert cache.get("fake_tool", {"symbol": "GOOD"}, ttl_s=120.0) == {"ok": True}

    def test_negative_ttl_default_tracks_the_provider_failure_cooldown(self, tmp_path):
        """The default must come from the provider layer's own cooldown, not a
        second invented constant that can drift."""
        from tradingview_mcp.core.services import async_worker_pool as mod
        from tradingview_mcp.core.services.screener_provider import _failure_cooldown_s

        cache = ResultCache(db_path=tmp_path / "tool_cache.db")
        pool = WorkerPool(cache=cache, runners={"fake_tool": _CallRecorder()})

        assert pool.error_ttl_s == float(_failure_cooldown_s())
        assert mod.default_error_ttl_s() == float(_failure_cooldown_s())

    def test_failing_job_caches_nothing_and_reports_the_error(self, tmp_path):
        recorder = _CallRecorder(exc=RuntimeError("upstream cliff"))
        cache, pool = _make_pool(tmp_path, recorder, concurrency=2)

        async def scenario():
            await pool.start()
            try:
                return await pool.submit("fake_tool", {"symbol": "AAPL"}, ttl_s=120.0)
            finally:
                await pool.stop()

        outcome = asyncio.run(scenario())

        assert outcome.ok is False
        assert "upstream cliff" in outcome.error
        assert cache.get("fake_tool", {"symbol": "AAPL"}) is None
        # A failed key must be retryable, not permanently stuck in-flight.
        assert not pool.is_inflight(cache_key_for("fake_tool", {"symbol": "AAPL"}))

    def test_a_failing_job_never_kills_its_worker(self, tmp_path):
        """One bad job must not take a worker down -- the pool has to keep
        draining afterwards."""
        calls = {"n": 0}

        def flaky(**kwargs):
            calls["n"] += 1
            if kwargs.get("symbol") == "BAD":
                raise RuntimeError("boom")
            return {"symbol": kwargs["symbol"]}

        cache = ResultCache(db_path=tmp_path / "tool_cache.db")
        pool = WorkerPool(cache=cache, runners={"fake_tool": flaky}, concurrency=1)

        async def scenario():
            await pool.start()
            try:
                bad = await pool.submit("fake_tool", {"symbol": "BAD"}, ttl_s=120.0)
                good = await pool.submit("fake_tool", {"symbol": "GOOD"}, ttl_s=120.0)
                return bad, good
            finally:
                await pool.stop()

        bad, good = asyncio.run(scenario())

        assert bad.ok is False
        assert good.ok is True and good.result == {"symbol": "GOOD"}

    def test_unknown_tool_is_rejected_at_submit_time(self, tmp_path):
        recorder = _CallRecorder()
        cache, pool = _make_pool(tmp_path, recorder, concurrency=1)

        async def scenario():
            await pool.start()
            try:
                with pytest.raises(KeyError):
                    pool.submit("not_a_registered_tool", {"symbol": "AAPL"}, ttl_s=120.0)
            finally:
                await pool.stop()

        asyncio.run(scenario())
        assert recorder.call_count == 0


class TestJobQueue:
    def test_submit_returns_the_same_future_for_the_same_key(self, tmp_path):
        async def scenario():
            q = JobQueue()
            a = q.submit("fake_tool", {"symbol": "AAPL"}, ttl_s=1.0)
            b = q.submit("fake_tool", {"symbol": "AAPL"}, ttl_s=1.0)
            c = q.submit("fake_tool", {"symbol": "MSFT"}, ttl_s=1.0)
            # Only the two DISTINCT keys were actually enqueued.
            return a is b, a is c, q.qsize()

        same, different, qsize = asyncio.run(scenario())
        assert same is True
        assert different is False
        assert qsize == 2

    def test_queue_parks_when_empty_rather_than_spinning(self, tmp_path):
        """``await queue.get()`` on an empty queue must block, not busy-loop or
        raise -- a worker with nothing to do costs nothing."""
        async def scenario():
            q = JobQueue()
            try:
                await asyncio.wait_for(q.get(), timeout=0.1)
            except asyncio.TimeoutError:
                return "parked"
            return "returned"

        assert asyncio.run(scenario()) == "parked"
