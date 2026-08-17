"""
Async job queue + bounded worker pool for expensive tool fetches.

Purpose
-------
The live half of the cache design (``result_cache`` is the durable half).
A caller that misses the cache does not sit through a 20-30s degraded upstream
fetch: the fetch becomes a background job, the caller returns ``pending``
immediately, and a later poll finds the completed result in the cache.

Two properties this module exists to guarantee
----------------------------------------------
1. **De-duplication.** A second request for the same ``cache_key`` while a job
   for it is queued or running must NOT start a second upstream fetch. It
   attaches to the same ``asyncio.Future`` and receives the same outcome.

   Correctness argument: ``JobQueue.submit`` does the "is it in flight?" check
   and the in-flight registration with **no ``await`` between them**, and the
   enqueue is ``put_nowait`` on an unbounded queue (also non-awaiting). Because
   asyncio is single-threaded and cooperative, no other coroutine can be
   scheduled inside that window, so the check-then-register sequence is
   effectively atomic — no lock needed, and no lost-update race is possible.
   The key is removed from the in-flight map only after the job is fully
   resolved, so the window covers queued AND running jobs.

2. **Bounded concurrency.** Exactly ``concurrency`` worker tasks exist, and each
   handles one job at a time, so at most ``concurrency`` upstream fetches run
   simultaneously regardless of how many callers arrive. This protects a
   shared, already-rate-limited upstream (the underlying provider layer keeps
   its own ``TRADINGVIEW_MCP_MAX_INFLIGHT`` thread semaphore — this pool sits
   above it and stops work from piling up in the first place).

Blocking runners
----------------
The real service functions (e.g. ``run_multi_timeframe_analysis``) are ordinary
synchronous, blocking functions. Workers call them via ``asyncio.to_thread`` so
the event loop stays responsive. This is safe here: the provider layer those
functions sit on is explicitly thread-guarded already (``threading.RLock`` on
its response cache, a ``threading.Semaphore`` bounding in-flight upstream
calls, a lock around its min-interval pacing) — it is designed to be called
from several threads at once.

Environment
-----------
``TRADINGVIEW_MCP_CACHE_WORKERS``  Worker count. Default 2, matching the
                                   existing ``TRADINGVIEW_MCP_MAX_INFLIGHT``
                                   precedent for upstream concurrency.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Any, Callable, Dict, NamedTuple, Optional

from tradingview_mcp.core.services.result_cache import ResultCache, cache_key_for

__all__ = ["Job", "JobOutcome", "JobQueue", "WorkerPool", "default_worker_count"]


DEFAULT_WORKER_COUNT = 2


def default_worker_count() -> int:
    try:
        return max(1, int(os.environ.get("TRADINGVIEW_MCP_CACHE_WORKERS", str(DEFAULT_WORKER_COUNT))))
    except (TypeError, ValueError):
        return DEFAULT_WORKER_COUNT


class Job(NamedTuple):
    """A unit of queued work."""

    cache_key: str
    tool: str
    args: Dict[str, Any]
    ttl_s: float


class JobOutcome(NamedTuple):
    """Result of a finished job.

    A failure is reported as ``ok=False`` + ``error`` rather than as a future
    exception. Callers of the cached-analysis path deliberately do NOT await
    the future (that is the whole point of returning ``pending``), and an
    un-retrieved future exception produces a spurious "exception was never
    retrieved" warning at GC time. Carrying the error in the value keeps the
    fire-and-forget path clean while still telling any real waiter what broke.
    """

    ok: bool
    result: Optional[Dict[str, Any]]
    error: Optional[str]


class JobQueue:
    """FIFO queue of pending jobs with in-flight de-duplication."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Job] = asyncio.Queue()
        self._inflight: Dict[str, asyncio.Future] = {}

    # ── submission ─────────────────────────────────────────────────────────

    def submit(self, tool: str, args: Dict[str, Any], ttl_s: float) -> asyncio.Future:
        """Enqueue a job, or attach to the identical one already in flight.

        Returns the future that will hold this job's :class:`JobOutcome`.
        Never blocks and never awaits — see the module docstring's correctness
        argument for why that is what makes the de-dup race-free.
        """
        key = cache_key_for(tool, args)
        existing = self._inflight.get(key)
        if existing is not None:
            return existing

        # get_running_loop (not get_event_loop): submitting off-loop is a bug we
        # want to fail loudly on, not paper over by creating a stray loop.
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._inflight[key] = future
        self._queue.put_nowait(Job(cache_key=key, tool=tool, args=dict(args), ttl_s=ttl_s))
        return future

    # ── worker side ────────────────────────────────────────────────────────

    async def get(self) -> Job:
        """Park until a job is available (``asyncio.Queue`` blocks correctly)."""
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    def resolve(self, cache_key: str, outcome: JobOutcome) -> None:
        """Release *cache_key* and hand *outcome* to every attached waiter.

        The key is released BEFORE the future is resolved so that a waiter
        woken by the result can immediately submit a fresh job if it wants one
        (there is no window in which the key is both resolved and still
        considered in flight).
        """
        future = self._inflight.pop(cache_key, None)
        if future is not None and not future.done():
            future.set_result(outcome)

    async def join(self) -> None:
        await self._queue.join()

    # ── introspection ──────────────────────────────────────────────────────

    def is_inflight(self, cache_key: str) -> bool:
        return cache_key in self._inflight

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    def qsize(self) -> int:
        return self._queue.qsize()


class WorkerPool:
    """``concurrency`` asyncio workers draining a :class:`JobQueue`."""

    def __init__(
        self,
        cache: ResultCache,
        runners: Dict[str, Callable[..., Dict[str, Any]]],
        concurrency: Optional[int] = None,
        queue: Optional[JobQueue] = None,
    ) -> None:
        self.cache = cache
        self.runners = dict(runners)
        self.concurrency = max(1, int(concurrency if concurrency is not None else default_worker_count()))
        self.queue = queue if queue is not None else JobQueue()
        self.workers: list[asyncio.Task] = []
        # Last failure per cache key — surfaced to pollers so a repeatedly
        # failing fetch is visible instead of looking like an endless "pending".
        self.last_errors: Dict[str, str] = {}

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self.workers:
            return
        self.workers = [
            asyncio.create_task(self._worker_loop(i), name=f"tvmcp-cache-worker-{i}")
            for i in range(self.concurrency)
        ]

    async def stop(self) -> None:
        for task in self.workers:
            task.cancel()
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers = []

    async def drain(self, timeout: float = 30.0) -> bool:
        """Wait until every queued job has finished. True if it drained in time."""
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    @property
    def running(self) -> bool:
        return bool(self.workers)

    # ── submission passthrough ─────────────────────────────────────────────

    def submit(self, tool: str, args: Dict[str, Any], ttl_s: float) -> asyncio.Future:
        """Submit a job. Raises ``KeyError`` for a tool with no bound runner.

        Rejecting an unknown tool at submit time (rather than failing inside a
        worker) keeps an unsupported request from silently occupying a queue
        slot and a de-dup key.
        """
        if tool not in self.runners:
            raise KeyError(f"no runner registered for tool {tool!r}")
        return self.queue.submit(tool, args, ttl_s)

    def is_inflight(self, cache_key: str) -> bool:
        return self.queue.is_inflight(cache_key)

    @property
    def inflight_count(self) -> int:
        return self.queue.inflight_count

    # ── worker body ────────────────────────────────────────────────────────

    async def _worker_loop(self, index: int) -> None:
        while True:
            job = await self.queue.get()
            try:
                await self._run_job(job)
            except asyncio.CancelledError:
                # Cancelled mid-job: release the key so a restarted pool can
                # redo the work rather than seeing a permanently in-flight key.
                self.queue.resolve(
                    job.cache_key, JobOutcome(False, None, "cancelled")
                )
                self.queue.task_done()
                raise
            except Exception as exc:  # pragma: no cover - _run_job is total
                self._warn(f"worker {index} unexpected failure: {exc!r}")
                self.queue.resolve(job.cache_key, JobOutcome(False, None, repr(exc)))
                self.queue.task_done()
            else:
                self.queue.task_done()

    async def _run_job(self, job: Job) -> None:
        """Execute one job. Never raises except on cancellation."""
        runner = self.runners.get(job.tool)
        if runner is None:
            self.queue.resolve(
                job.cache_key, JobOutcome(False, None, f"no runner for tool {job.tool!r}")
            )
            return

        started = time.monotonic()
        try:
            result = await asyncio.to_thread(runner, **job.args)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.last_errors[job.cache_key] = error
            self._warn(
                f"job failed tool={job.tool} args={job.args} "
                f"after {time.monotonic() - started:.1f}s: {error}"
            )
            self.queue.resolve(job.cache_key, JobOutcome(False, None, error))
            return

        if not isinstance(result, dict):
            error = f"runner for {job.tool!r} returned {type(result).__name__}, expected dict"
            self.last_errors[job.cache_key] = error
            self.queue.resolve(job.cache_key, JobOutcome(False, None, error))
            return

        # Cache write happens off-loop too: sqlite3 is blocking IO.
        await asyncio.to_thread(self.cache.put, job.tool, job.args, result, job.ttl_s)
        self.last_errors.pop(job.cache_key, None)
        self.queue.resolve(job.cache_key, JobOutcome(True, result, None))

    @staticmethod
    def _warn(message: str) -> None:
        try:
            print(f"[tradingview_mcp.worker_pool] {message}", file=sys.stderr, flush=True)
        except Exception:
            pass
