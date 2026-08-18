"""``get_cached_analysis`` tool tests — the three states it can return.

  * ``fresh``   — the cache already holds an unexpired result; returned inline,
                  with zero upstream work.
  * ``pending`` — nothing cached (or stale); a background job is enqueued and
                  the call returns IMMEDIATELY without waiting for it.
  * ``pending -> fresh`` — a job that completes between two polls flips the
                  next poll to ``fresh``.

Plus the additive-only guarantee: registering this tool must not have changed
any existing tool.
"""
from __future__ import annotations

import asyncio
import inspect

import pytest

from tradingview_mcp.core.services.result_cache import ResultCache
from tradingview_mcp.core.services.async_worker_pool import WorkerPool
from tradingview_mcp.core.services import cached_analysis_service as svc


def _pool(tmp_path, runner, concurrency=2):
    cache = ResultCache(db_path=tmp_path / "tool_cache.db")
    return cache, WorkerPool(
        cache=cache,
        runners={"multi_timeframe_analysis": runner},
        concurrency=concurrency,
    )


class TestFreshState:
    def test_cache_hit_returns_fresh_without_touching_the_service(self, tmp_path):
        calls = {"n": 0}

        def runner(**kwargs):
            calls["n"] += 1
            return {"never": "called"}

        cache, pool = _pool(tmp_path, runner)
        cache.put(
            "multi_timeframe_analysis",
            {"symbol": "AAPL", "exchange": "NASDAQ"},
            {"alignment": {"status": "BULLISH"}},
            ttl_s=120.0,
        )

        async def scenario():
            await pool.start()
            try:
                return await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 120.0, pool=pool
                )
            finally:
                await pool.stop()

        out = asyncio.run(scenario())

        assert out["status"] == "fresh"
        assert out["result"] == {"alignment": {"status": "BULLISH"}}
        assert calls["n"] == 0

    def test_stale_cache_entry_is_not_served_as_fresh(self, tmp_path):
        import sqlite3

        from tradingview_mcp.core.services.result_cache import cache_key_for

        def runner(**kwargs):
            return {"fresh": True}

        cache, pool = _pool(tmp_path, runner)
        cache.put(
            "multi_timeframe_analysis",
            {"symbol": "AAPL", "exchange": "NASDAQ"},
            {"old": True},
            ttl_s=10.0,
        )
        key = cache_key_for("multi_timeframe_analysis", {"symbol": "AAPL", "exchange": "NASDAQ"})
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute(
                "UPDATE tool_result_cache SET computed_at = computed_at - 600 WHERE cache_key = ?",
                (key,),
            )

        async def scenario():
            await pool.start()
            try:
                return await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 10.0, pool=pool
                )
            finally:
                await pool.stop()

        out = asyncio.run(scenario())
        assert out["status"] == "pending"


    def test_caller_ttl_is_honoured_over_a_longer_stored_ttl(self, tmp_path):
        """The tool's documented contract: `ttl_s` is MY freshness requirement.
        An entry written under a long TTL must not be served as "fresh" to a
        caller asking for a short one."""
        import sqlite3

        from tradingview_mcp.core.services.result_cache import cache_key_for

        def runner(**kwargs):
            return {"refetched": True}

        cache, pool = _pool(tmp_path, runner)
        args = {"symbol": "AAPL", "exchange": "NASDAQ"}
        cache.put("multi_timeframe_analysis", args, {"stale": True}, ttl_s=3600.0)
        key = cache_key_for("multi_timeframe_analysis", args)
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute(
                "UPDATE tool_result_cache SET computed_at = computed_at - 60 WHERE cache_key = ?",
                (key,),
            )

        async def scenario():
            await pool.start()
            try:
                strict = await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 30.0, pool=pool
                )
                await pool.drain(timeout=5.0)
                relaxed_after_refresh = await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 30.0, pool=pool
                )
                return strict, relaxed_after_refresh
            finally:
                await pool.stop()

        strict, refreshed = asyncio.run(scenario())

        assert strict["status"] == "pending", "60s-old row served to a 30s-ttl caller"
        assert refreshed["status"] == "fresh"
        assert refreshed["result"] == {"refetched": True}

    def test_a_generous_caller_still_gets_the_existing_entry(self, tmp_path):
        """Control: the stricter-of-the-two rule must not turn every hit into a
        miss -- a caller whose window covers the row still gets it."""
        import sqlite3

        from tradingview_mcp.core.services.result_cache import cache_key_for

        calls = {"n": 0}

        def runner(**kwargs):
            calls["n"] += 1
            return {}

        cache, pool = _pool(tmp_path, runner)
        args = {"symbol": "AAPL", "exchange": "NASDAQ"}
        cache.put("multi_timeframe_analysis", args, {"cached": True}, ttl_s=3600.0)
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute(
                "UPDATE tool_result_cache SET computed_at = computed_at - 60 WHERE cache_key = ?",
                (cache_key_for("multi_timeframe_analysis", args),),
            )

        async def scenario():
            await pool.start()
            try:
                return await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 300.0, pool=pool
                )
            finally:
                await pool.stop()

        out = asyncio.run(scenario())

        assert out["status"] == "fresh"
        assert out["result"] == {"cached": True}
        assert calls["n"] == 0


class TestNegativeCaching:
    def test_upstream_failure_is_retried_soon_not_after_the_full_ttl(self, tmp_path):
        """A transient ALL_BATCHES_FAILED must not blank the symbol for 120s."""
        import sqlite3

        from tradingview_mcp.core.errors import BatchExecutionError
        from tradingview_mcp.core.services import cached_analysis_service as mod

        attempts = {"n": 0}

        def flaky_underlying(full_symbol, exchange):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise BatchExecutionError(
                    batches_attempted=5, batches_failed=5, first_error="Expecting value",
                )
            return {"alignment": {"status": "BULLISH"}}

        cache = ResultCache(db_path=tmp_path / "tool_cache.db")
        pool = WorkerPool(
            cache=cache,
            runners={"multi_timeframe_analysis": mod.run_multi_timeframe_job},
            concurrency=1,
            error_ttl_s=15.0,
        )

        async def scenario(monkeypatched):
            await pool.start()
            try:
                await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 120.0, pool=pool
                )
                await pool.drain(timeout=5.0)
                errored = await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 120.0, pool=pool
                )
                # 20s later the failure entry is dead even though the caller's
                # own TTL is 120s -- so the symbol is retried, not blanked.
                with sqlite3.connect(str(cache.db_path)) as conn:
                    conn.execute("UPDATE tool_result_cache SET computed_at = computed_at - 20")
                retried = await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 120.0, pool=pool
                )
                await pool.drain(timeout=5.0)
                recovered = await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 120.0, pool=pool
                )
                return errored, retried, recovered
            finally:
                await pool.stop()

        import unittest.mock as um

        with um.patch.object(mod, "run_multi_timeframe_analysis", flaky_underlying):
            errored, retried, recovered = asyncio.run(scenario(True))

        # The failure was served from cache immediately after it happened...
        assert errored["status"] == "fresh"
        assert errored["result"]["error"]["code"] == "ALL_BATCHES_FAILED"
        # ...but expired ~15s later, triggering a real retry...
        assert retried["status"] == "pending"
        # ...which succeeded.
        assert recovered["status"] == "fresh"
        assert recovered["result"] == {"alignment": {"status": "BULLISH"}}
        assert attempts["n"] == 2


class TestPendingState:
    def test_cache_miss_returns_pending_immediately_and_enqueues_a_real_job(self, tmp_path):
        import time

        started = asyncio.Event()

        def slow_runner(**kwargs):
            time.sleep(0.4)
            return {"symbol": kwargs["symbol"]}

        cache, pool = _pool(tmp_path, slow_runner)

        async def scenario():
            await pool.start()
            try:
                t0 = time.monotonic()
                out = await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 120.0, pool=pool
                )
                elapsed = time.monotonic() - t0
                # The job really is in flight -- we did not just drop it.
                inflight = pool.inflight_count
                return out, elapsed, inflight
            finally:
                await pool.stop()

        out, elapsed, inflight = asyncio.run(scenario())

        assert out["status"] == "pending"
        assert "result" not in out
        assert elapsed < 0.2, f"call blocked for {elapsed:.2f}s -- it must return immediately"
        assert inflight == 1

    def test_repeat_poll_while_pending_does_not_enqueue_a_second_job(self, tmp_path):
        import time

        calls = {"n": 0}

        def slow_runner(**kwargs):
            calls["n"] += 1
            time.sleep(0.3)
            return {"v": 1}

        cache, pool = _pool(tmp_path, slow_runner)

        async def scenario():
            await pool.start()
            try:
                outs = []
                for _ in range(4):
                    outs.append(
                        await svc.serve_cached_analysis(
                            "multi_timeframe_analysis", "AAPL", "NASDAQ", 120.0, pool=pool
                        )
                    )
                    await asyncio.sleep(0.02)
                return outs
            finally:
                await pool.stop()

        outs = asyncio.run(scenario())

        assert all(o["status"] == "pending" for o in outs)
        assert calls["n"] == 1, f"polling re-enqueued the job {calls['n']} times"


class TestPendingBecomesFresh:
    def test_next_poll_after_the_job_completes_is_fresh(self, tmp_path):
        def runner(**kwargs):
            return {"symbol": kwargs["symbol"], "exchange": kwargs["exchange"]}

        cache, pool = _pool(tmp_path, runner)

        async def scenario():
            await pool.start()
            try:
                first = await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 120.0, pool=pool
                )
                # Let the worker drain the queue (bounded wait, no fixed sleep).
                await pool.drain(timeout=5.0)
                second = await svc.serve_cached_analysis(
                    "multi_timeframe_analysis", "AAPL", "NASDAQ", 120.0, pool=pool
                )
                return first, second
            finally:
                await pool.stop()

        first, second = asyncio.run(scenario())

        assert first["status"] == "pending"
        assert second["status"] == "fresh"
        assert second["result"] == {"symbol": "AAPL", "exchange": "NASDAQ"}


class TestUnsupportedTool:
    def test_unknown_tool_returns_a_structured_error_not_a_wrong_answer(self, tmp_path):
        def runner(**kwargs):
            return {}

        cache, pool = _pool(tmp_path, runner)

        async def scenario():
            await pool.start()
            try:
                return await svc.serve_cached_analysis(
                    "market_sentiment", "AAPL", "NASDAQ", 120.0, pool=pool
                )
            finally:
                await pool.stop()

        out = asyncio.run(scenario())

        assert "error" in out
        assert out["error"]["code"] == "INVALID_PARAMETER"
        assert "multi_timeframe_analysis" in out["error"]["message"]


class TestServerRegistration:
    def test_tool_is_registered_and_async(self):
        from tradingview_mcp import server

        assert inspect.iscoroutinefunction(server.get_cached_analysis)

    def test_existing_tools_are_untouched(self):
        """Purely additive: the signatures of the tools this change sits next to
        must be byte-identical to before."""
        from tradingview_mcp import server

        assert str(inspect.signature(server.multi_timeframe_analysis)) == \
            "(symbol: 'str', exchange: 'str' = 'KUCOIN') -> 'dict'"
        assert str(inspect.signature(server.market_sentiment)) == \
            "(symbol: 'str', category: 'str' = 'all', limit: 'int' = 20) -> 'dict'"
        assert not inspect.iscoroutinefunction(server.multi_timeframe_analysis)
        assert not inspect.iscoroutinefunction(server.market_sentiment)

    def test_fastmcp_lists_the_new_tool_alongside_the_old_ones(self):
        from tradingview_mcp import server

        names = {t.name for t in asyncio.run(server.mcp.list_tools())}
        assert "get_cached_analysis" in names
        assert "multi_timeframe_analysis" in names
        assert "market_sentiment" in names
        assert "compare_strategies" in names


class TestRealRunnerBinding:
    def test_default_runner_delegates_to_the_real_service_function(self, monkeypatch):
        """The registered runner must call the REAL
        ``run_multi_timeframe_analysis`` with a normalised symbol -- not a
        re-implementation."""
        from tradingview_mcp.core.services import cached_analysis_service as mod

        seen = {}

        def fake(full_symbol, exchange):
            seen["symbol"] = full_symbol
            seen["exchange"] = exchange
            return {"ok": True}

        monkeypatch.setattr(mod, "run_multi_timeframe_analysis", fake)

        out = mod.run_multi_timeframe_job(symbol="AAPL", exchange="NASDAQ")

        assert out == {"ok": True}
        # Exactly what the existing multi_timeframe_analysis tool passes down:
        # sanitize_exchange() lower-cases, normalize_tradingview_symbol()
        # produces the prefixed symbol.
        assert seen == {"symbol": "NASDAQ:AAPL", "exchange": "nasdaq"}

    def test_batch_execution_error_becomes_the_same_error_envelope_the_tool_returns(self, monkeypatch):
        from tradingview_mcp.core.errors import BatchExecutionError
        from tradingview_mcp.core.services import cached_analysis_service as mod

        def boom(full_symbol, exchange):
            raise BatchExecutionError(
                batches_attempted=5, batches_failed=5, first_error="Expecting value",
            )

        monkeypatch.setattr(mod, "run_multi_timeframe_analysis", boom)

        out = mod.run_multi_timeframe_job(symbol="AAPL", exchange="NASDAQ")

        assert out["error"]["code"] == "ALL_BATCHES_FAILED"
        assert out["error"]["batches_attempted"] == 5
