"""
Cached-analysis service — the logic behind the ``get_cached_analysis`` MCP tool.

``server.py`` stays a routing layer (see its module docstring); everything the
new tool actually does lives here.

Flow
----
    ResultCache.get(tool, args)
        fresh?  -> {"status": "fresh", "result": {...}}
        miss/stale -> WorkerPool.submit(...)  (de-duplicated)
                   -> {"status": "pending"}   returned IMMEDIATELY

The caller polls again later; once the background job has written the result
to the cache, that same call returns ``fresh``. Nothing here ever waits for a
job to finish — non-blocking is the entire point.

Supported tools
---------------
Only ``multi_timeframe_analysis`` today — the call under real latency pressure.
Any other tool name returns an ``INVALID_PARAMETER`` error envelope rather than
silently doing something else. Adding another tool is one entry in
``TOOL_RUNNERS`` plus its ``run_*_job`` wrapper.
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, Optional

from tradingview_mcp.core.errors import BatchExecutionError, ErrorCode, make_error
from tradingview_mcp.core.services.async_worker_pool import WorkerPool
from tradingview_mcp.core.services.result_cache import ResultCache
from tradingview_mcp.core.services.screener_service import run_multi_timeframe_analysis
from tradingview_mcp.core.utils.validators import (
    normalize_tradingview_symbol,
    sanitize_exchange,
)

__all__ = [
    "TOOL_RUNNERS",
    "SUPPORTED_TOOLS",
    "run_multi_timeframe_job",
    "serve_cached_analysis",
    "get_pool",
    "shutdown_pool",
]


# ── Runners: the REAL service functions, called exactly as the existing tools do ──

def run_multi_timeframe_job(symbol: str, exchange: str) -> Dict[str, Any]:
    """Background-job form of the ``multi_timeframe_analysis`` tool.

    Deliberately identical to that tool's body — same ``sanitize_exchange`` /
    ``normalize_tradingview_symbol`` / ``run_multi_timeframe_analysis`` calls,
    and the same ``BatchExecutionError`` -> error-envelope conversion — so a
    cached result is byte-for-byte what a direct call would have returned. No
    logic is duplicated: this delegates to the same service function.

    The error envelope IS a legitimate documented return value of that tool, so
    it is cached like any other result; that stops an upstream cliff from being
    re-hammered once per poll for the whole TTL window.
    """
    exchange = sanitize_exchange(exchange, "KUCOIN")
    full_symbol = normalize_tradingview_symbol(symbol, exchange)
    try:
        return run_multi_timeframe_analysis(full_symbol, exchange)
    except BatchExecutionError as e:
        return make_error(
            ErrorCode.ALL_BATCHES_FAILED, str(e),
            batches_attempted=e.batches_attempted,
            batches_failed=e.batches_failed,
            first_error=e.first_error,
        )


TOOL_RUNNERS: Dict[str, Callable[..., Dict[str, Any]]] = {
    "multi_timeframe_analysis": run_multi_timeframe_job,
}

SUPPORTED_TOOLS = tuple(TOOL_RUNNERS)


# ── Process-wide pool, bound lazily to the running event loop ──────────────────

_pool: Optional[WorkerPool] = None
_pool_loop: Optional[asyncio.AbstractEventLoop] = None


async def get_pool() -> WorkerPool:
    """Return the started pool for the CURRENT event loop, building it on demand.

    Built lazily inside a coroutine (never at import time) for two reasons:
    ``asyncio.Queue``/``Future`` must be created on the loop that will drive
    them, and a server that never receives a ``get_cached_analysis`` call
    should pay nothing — no tasks, no DB file.

    If the running loop differs from the one the pool was built on (test runs
    using ``asyncio.run`` per case, or a re-created server loop), the pool is
    rebuilt rather than being reused across loops, which would raise.
    """
    global _pool, _pool_loop
    loop = asyncio.get_running_loop()
    if _pool is not None and _pool_loop is loop and _pool.running:
        return _pool

    _pool = WorkerPool(cache=ResultCache(), runners=TOOL_RUNNERS)
    _pool_loop = loop
    await _pool.start()
    return _pool


async def shutdown_pool() -> None:
    """Stop and forget the process-wide pool (used by tests and shutdown paths)."""
    global _pool, _pool_loop
    if _pool is not None:
        await _pool.stop()
    _pool = None
    _pool_loop = None


# ── The tool body ─────────────────────────────────────────────────────────────

async def serve_cached_analysis(
    tool: str,
    symbol: str,
    exchange: str = "NASDAQ",
    ttl_s: float = 120.0,
    pool: Optional[WorkerPool] = None,
) -> Dict[str, Any]:
    """Cache-first, non-blocking accessor. See the module docstring for the flow.

    Args:
        tool: Name of the underlying tool to serve. Only the names in
            :data:`SUPPORTED_TOOLS` are accepted.
        symbol / exchange: Passed through to the underlying tool verbatim.
        ttl_s: Freshness window for this caller. Also the TTL stored with a
            result the resulting job computes.
        pool: Injection seam for tests. Production passes nothing and gets the
            process-wide pool.

    Returns:
        ``{"status": "fresh", "result": {...}, "tool": ..., "args": {...}}``,
        ``{"status": "pending", ...}`` (optionally with ``last_error``), or an
        error envelope for an unsupported tool.
    """
    if tool not in TOOL_RUNNERS:
        return make_error(
            ErrorCode.INVALID_PARAMETER,
            f"tool {tool!r} is not available through get_cached_analysis; "
            f"supported: {', '.join(SUPPORTED_TOOLS)}. "
            f"Call the tool directly instead.",
            supported_tools=list(SUPPORTED_TOOLS),
        )

    try:
        ttl = max(0.0, float(ttl_s))
    except (TypeError, ValueError):
        return make_error(
            ErrorCode.INVALID_PARAMETER, f"ttl_s must be a number, got {ttl_s!r}"
        )

    args: Dict[str, Any] = {"symbol": symbol, "exchange": exchange}
    active = pool if pool is not None else await get_pool()

    # sqlite read is blocking IO; keep it off the loop like every other IO here.
    cached = await asyncio.to_thread(active.cache.get, tool, args)
    if cached is not None:
        return {"status": "fresh", "tool": tool, "args": args, "result": cached}

    active.submit(tool, args, ttl)  # de-duplicated; never awaited on purpose

    from tradingview_mcp.core.services.result_cache import cache_key_for

    response: Dict[str, Any] = {"status": "pending", "tool": tool, "args": args}
    last_error = active.last_errors.get(cache_key_for(tool, args))
    if last_error:
        # The previous attempt for this key failed. Surface it so a caller
        # polling forever can tell "still working" from "upstream is down",
        # while a fresh retry is already queued.
        response["last_error"] = last_error
    return response
