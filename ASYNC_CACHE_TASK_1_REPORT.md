# Async result cache + bounded worker pool — build report

**Date:** 2026-08-17
**Repo:** `/Users/pessi/Projects/tradingview-mcp`
**Branch:** `feature/async-result-cache` (branched off `main`; NOT merged, NOT pushed)
**Spec:** `tradesignalservice/.../docs/superpowers/specs/2026-08-17-tradingview-mcp-async-cache-design.md`, sections 1–3 and 5
**Live service on :8100:** untouched — never restarted, never redeployed.

---

## 1. What was built

Three new modules plus one new MCP tool. Everything is additive; no existing
file other than `server.py` (one import + one new tool) and `.gitignore` was
modified.

### `src/tradingview_mcp/core/services/result_cache.py`

SQLite-backed store of completed tool results, stdlib `sqlite3` only — **no new
dependency** was added to `pyproject.toml`.

Schema exactly as specified:

```sql
tool_result_cache(cache_key TEXT PRIMARY KEY, tool TEXT, args_json TEXT,
                  result_json TEXT, computed_at REAL, ttl_s REAL)
```

`cache_key = sha256(tool + canonical_json(args))` where canonical JSON is
`sort_keys=True, separators=(",",":")` — so argument ordering can never change
the key. `get()` returns `None` for missing, expired (`now - computed_at >
ttl_s`), or corrupt rows. `put()` is `INSERT OR REPLACE`. A `purge_expired()`
helper exists but is not yet called on any schedule (see §6).

### `src/tradingview_mcp/core/services/async_worker_pool.py`

`Job`, `JobOutcome`, `JobQueue`, `WorkerPool`.

- `JobQueue` — unbounded `asyncio.Queue` of `Job(cache_key, tool, args, ttl_s)`
  plus a `dict[cache_key, asyncio.Future]` in-flight map. `submit()` returns the
  **existing** future if the key is already queued or running.
- `WorkerPool` — exactly `concurrency` `asyncio.Task` workers, each looping
  `await queue.get()` → `await asyncio.to_thread(runner, **args)` →
  `cache.put(...)` on success → resolve the future → `task_done()`. Default
  worker count **2**, overridable via `TRADINGVIEW_MCP_CACHE_WORKERS`.

### `src/tradingview_mcp/core/services/cached_analysis_service.py`

The tool's logic (this repo's `server.py` is documented as a routing layer only,
so no logic went into it). Holds the `TOOL_RUNNERS` registry
(`multi_timeframe_analysis` → `run_multi_timeframe_job`), the lazily-built
process-wide pool, and `serve_cached_analysis()`.

`run_multi_timeframe_job` is a deliberate mirror of the existing
`multi_timeframe_analysis` tool body — same `sanitize_exchange` →
`normalize_tradingview_symbol` → `run_multi_timeframe_analysis` calls, same
`BatchExecutionError` → `make_error(ALL_BATCHES_FAILED, ...)` conversion — so a
cached result is byte-for-byte what a direct call returns. It delegates to the
same service function; nothing is re-implemented.

### `server.py` — new tool

```python
@mcp.tool()
async def get_cached_analysis(tool: str, symbol: str,
                              exchange: str = "NASDAQ",
                              ttl_s: float = 120.0) -> dict
```

Fresh → `{"status": "fresh", "tool", "args", "result": {...}}`.
Miss/stale → submits (de-duplicated) and returns `{"status": "pending", ...}`
immediately. Unsupported `tool` → `INVALID_PARAMETER` error envelope.

---

## 2. Verified assumptions (checked, not guessed)

| Assumption | How it was verified | Result |
|---|---|---|
| FastMCP in the **installed** SDK supports async tool handlers | Read `mcp/server/fastmcp/tools/base.py::Tool.from_function` in `.venv` (mcp 1.12.4) — it calls `_is_async_callable(fn)` and stores `is_async` | ✅ supported; no substitution needed |
| `run_multi_timeframe_analysis` is synchronous/blocking | Read `screener_service.py:904` — plain `def`, no `await` | ✅ confirmed |
| It is safe to call from a worker thread via `asyncio.to_thread` | Read `screener_provider.py`: its response cache is guarded by `threading.RLock`, upstream in-flight calls by a `threading.Semaphore(TRADINGVIEW_MCP_MAX_INFLIGHT)`, min-interval pacing by a `threading.Lock`. The layer is explicitly built for multi-threaded callers. Then proven end-to-end with a real live call (§5) | ✅ safe |
| Worker default of 2 matches an existing precedent here | `TRADINGVIEW_MCP_MAX_INFLIGHT` defaults to `2` in `screener_provider.py` | ✅ same number, same reason |
| Repo has no DB dependency / data dir | `pyproject.toml` deps are feedparser/mcp/requests/tradingview-screener/tradingview-ta; only `logs/` exists at top level | ✅ stdlib sqlite3, new `data/` dir mirroring `logs/` |
| No `pytest-asyncio` available | `.venv` has pytest 9.0.3 + anyio, no asyncio plugin, no pytest config in `pyproject.toml` | ✅ tests driven via plain `asyncio.run` — no new dev dependency |

---

## 3. Judgment calls (the review list)

1. **De-dup needs no lock.** `JobQueue.submit()` does the in-flight check, the
   registration, and the `put_nowait` enqueue with **no `await` between them**.
   asyncio is single-threaded and cooperative, so no other coroutine can be
   scheduled inside that window — the check-then-register is effectively atomic.
   This is why `submit()` is a plain `def`, not a coroutine: making it async
   would introduce exactly the suspension point that could open a race.
   Documented in the module docstring so a future edit doesn't casually add an
   `await` there.

2. **Failures are carried in the future's *value*, not as an exception.**
   `JobOutcome(ok, result, error)`. The production path deliberately never
   awaits the future (that's what makes it non-blocking), and an unretrieved
   future *exception* produces a spurious "exception was never retrieved"
   warning at GC time. Value-carried errors keep the fire-and-forget path clean
   while still telling a real waiter (the tests) what broke.

3. **A failed job caches nothing and is immediately retryable.** The key is
   released from the in-flight map either way, so the next poll re-enqueues.
   The failure is recorded in `WorkerPool.last_errors[cache_key]` and surfaced
   as an extra `"last_error"` field on the next `pending` response — otherwise a
   permanently-down upstream is indistinguishable from "still working". The
   spec only names `fresh`/`pending`; I added a field rather than a third
   status so the documented contract is unchanged.

4. **`BatchExecutionError` results ARE cached.** That error envelope is a
   documented legitimate return value of `multi_timeframe_analysis`. Caching it
   stops an upstream cliff from being re-hammered once per poll for the whole
   TTL. Genuine unexpected exceptions (item 3) are *not* cached.

5. **`asyncio.to_thread` also wraps the sqlite reads/writes**, not just the
   upstream fetch. sqlite3 is blocking IO; keeping it off the loop is consistent
   and costs nothing.

6. **One sqlite connection per operation** rather than a shared one. sqlite3
   connections aren't safe across threads and cache ops are trivial next to a
   20 s upstream fetch. WAL mode is enabled so a poll can read while a worker
   writes.

7. **The cache never raises.** Locked DB, corrupt row, read-only FS → miss (or a
   dropped write) plus a stderr warning. A cache is an optimisation; it must
   never take down a tool call.

8. **Pool is built lazily inside a coroutine, bound to the running loop**, and
   rebuilt if the loop differs. `asyncio.Queue`/`Future` must live on the loop
   that drives them, and a server that never receives a `get_cached_analysis`
   call then pays nothing — no tasks, no DB file created. (Confirmed: running
   the full test suite creates no `data/` directory.)

9. **DB location:** `TRADINGVIEW_MCP_CACHE_DB` env override → else
   `<repo>/data/tool_cache.db` (created on demand, mirroring the existing
   top-level `logs/` convention rather than inventing a scattered layout) →
   else `~/.tradingview-mcp/tool_cache.db` if the repo dir isn't writable.
   `data/` was added to `.gitignore`.

10. **Cache key uses the caller's raw `symbol`/`exchange`**, not normalised
    forms. So `exchange="nasdaq"` and `"NASDAQ"` are separate cache entries.
    Chosen for predictability (the key is exactly what the caller asked for);
    the cost is at most one duplicate fetch per casing variant. Easy to change
    later by normalising in `serve_cached_analysis` — flagged because a
    reasonable person could pick the other option.

11. **Unknown tool is rejected at `WorkerPool.submit()` with `KeyError`**, and
    separately at the service layer with an error envelope, so an unsupported
    request can never occupy a queue slot or a de-dup key.

12. **Scope held to `multi_timeframe_analysis` only**, per the brief. Adding
    another tool is one `TOOL_RUNNERS` entry plus a `run_*_job` wrapper —
    deliberately not done tonight so the one that matters is right (§6).

---

## 4. TDD evidence

Tests were written first and run against a non-existent implementation.

**Initial RED** (all three new test modules):

```
E   ModuleNotFoundError: No module named 'tradingview_mcp.core.services.result_cache'
3 errors in 0.09s
```

**RED proof that the de-dup test is non-vacuous.** After implementing, the
in-flight check in `JobQueue.submit` was temporarily replaced with
`existing = None` and the suite re-run:

```
FAILED test_async_worker_pool.py::TestDeDuplication::test_concurrent_identical_requests_call_the_service_exactly_once - assert False
FAILED test_async_worker_pool.py::TestDeDuplication::test_second_request_arriving_mid_flight_attaches_to_the_same_job - assert <Future pending> is <Future pending>
FAILED test_async_worker_pool.py::TestJobQueue::test_submit_returns_the_same_future_for_the_same_key - assert False is True
FAILED test_get_cached_analysis.py::TestPendingState::test_repeat_poll_while_pending_does_not_enqueue_a_second_job - AssertionError: polling re-enqueued the job 2 times
4 failed, 20 passed
```

The de-dup assertion is on the **real call count of the underlying service
function** (a thread-safe recorder), not on a mock's arrangement — with the
guard removed the recorder genuinely observed 2 calls. Code restored, GREEN.

**RED proof that the bounded-concurrency test is non-vacuous.** Worker creation
was temporarily changed to `range(self.concurrency + 3)`:

```
FAILED TestBoundedConcurrency::test_never_more_than_n_runners_execute_at_once - AssertionError: peak concurrency was 5
FAILED TestBoundedConcurrency::test_concurrency_of_one_serializes_completely - assert 4 == 1
FAILED TestBoundedConcurrency::test_pool_starts_exactly_n_worker_tasks - assert 6 == 3
3 failed, 10 deselected
```

Peak concurrency is measured by a `threading.Lock`-guarded counter inside the
fake runner, which really does run in worker threads. The test asserts both
`<= 2` (the bound) **and** `== 2` (that it genuinely parallelises — otherwise a
broken pool with 1 worker would pass the bound trivially). Code restored, GREEN.

---

## 5. Live end-to-end verification (real upstream, real market data)

Run in-process with `TRADINGVIEW_MCP_CACHE_DB=/tmp/tvmcp_smoke.db`. The live
:8100 service was not started, stopped, or touched.

```
call1 0.002s pending                     <- non-blocking: returned in 2 ms
statuses while pending: ['pending','pending','pending'] inflight: 1 qsize: 0
                                         <- 4 concurrent requests, ONE in-flight job
 poll: pending ... (x9)
became FRESH after ~10.0s                <- real upstream fetch took ~10 s
alignment: {'status': 'LEAN BULLISH', 'confidence': 'Medium', 'net_score': 2,
            'scores_by_tf': {'1W': 1, '1D': 1, '4h': -1, '1h': 1, '15m': 0},
            'divergent_timeframes': ['4h']}
cached call 0.0007s status=fresh         <- 0.7 ms vs 10 s: ~14,000x faster
unsupported: {'error': {'code': 'INVALID_PARAMETER', ...}}
```

That is the whole value proposition demonstrated against live data: a 10 s
blocking call became a 2 ms `pending` + a 0.7 ms `fresh`, and four concurrent
requests for the same symbol produced exactly one upstream fetch.

---

## 6. Test suite result

```
$ .venv/bin/python -m pytest tests/ -q
174 passed in 5.72s
```

- Pre-existing: 139, all still green.
- New: 35 (`test_result_cache.py` 11, `test_async_worker_pool.py` 13,
  `test_get_cached_analysis.py` 11).
- No new dependency; no pytest config change.

Existing-tools-untouched is asserted **in code**, not just by eyeballing the
diff: `TestServerRegistration::test_existing_tools_are_untouched` pins the exact
signatures of `multi_timeframe_analysis` and `market_sentiment` and asserts both
are still non-coroutine functions;
`test_fastmcp_lists_the_new_tool_alongside_the_old_ones` asserts FastMCP's own
`list_tools()` contains the new tool alongside the old ones. The `git diff` of
`server.py` is one import line plus one new tool block — no existing line
changed.

---

## 7. Files changed

| File | Change |
|---|---|
| `src/tradingview_mcp/core/services/result_cache.py` | new |
| `src/tradingview_mcp/core/services/async_worker_pool.py` | new |
| `src/tradingview_mcp/core/services/cached_analysis_service.py` | new |
| `src/tradingview_mcp/server.py` | +1 import, +1 `get_cached_analysis` tool (additive only) |
| `.gitignore` | ignore `data/` |
| `tests/unit/services/test_result_cache.py` | new |
| `tests/unit/services/test_async_worker_pool.py` | new |
| `tests/unit/services/test_get_cached_analysis.py` | new |

Untracked files `com.tss.tradingview-mcp.plist` and `run-tradingview-mcp.sh`
were already untracked on `main` before this session and were **not** committed.

---

## 8. Concerns / deferred

1. **The service must be restarted to expose the new tool.** Operator's own
   decision, deliberately not done. Until then, TSS's side has nothing to call.
2. **Only `multi_timeframe_analysis` is cached.** `market_sentiment` and
   `compare_strategies` (the other two calls in TSS's `TechnicalCheckStage`) go
   through the direct path unchanged. They're advisory/fail-soft on the TSS
   side, so this was the right thing to leave. Generalising is a small,
   mechanical follow-up (`TOOL_RUNNERS` + a wrapper each).
3. **No cache eviction is scheduled.** `purge_expired()` exists but nothing
   calls it. Row count grows with distinct (tool, symbol, exchange) triples;
   for a few thousand symbols that's a few MB, so this is a housekeeping item,
   not a risk. Suggested follow-up: purge on pool start.
4. **`WorkerPool.stop()` is never wired to a server-shutdown hook.** FastMCP's
   lifespan wasn't touched; on process exit the tasks die with the loop. A
   cancelled mid-flight job resolves its future as `cancelled` and releases its
   key, so nothing is left permanently in-flight, but a fetch in progress at
   shutdown is simply lost (its worker thread finishes and the result is
   discarded). Acceptable; noted.
5. **Cache-key casing** — see judgment call #10.
6. **Poll cadence is the caller's problem.** A caller that polls in a tight loop
   won't amplify upstream load (de-dup + bounded pool prevent that), but it will
   do one sqlite read per poll. TSS's planned ~2 s cadence is fine.
7. **Section 4/6 of the spec (the TSS-side `TechnicalCheckStage` change) was
   explicitly out of scope** for this task and was not touched.
