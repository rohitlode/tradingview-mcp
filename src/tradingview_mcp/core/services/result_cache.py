"""
Result Cache — SQLite-backed store of completed MCP tool results.

Purpose
-------
tradingview-mcp's upstream TA source intermittently returns invalid/empty
responses, and its own retry + cooldown logic means a single cold
``multi_timeframe_analysis`` call can burn 20-30s under degraded conditions.
Every consumer used to pay that cost on every call, with no sharing between
callers and no sharing across process restarts.

This module is the durable half of the fix (the other half is
``async_worker_pool``): a completed result is written here keyed by
``sha256(tool + canonical_json(args))`` together with the TTL it was computed
under, so a later request for the same tool+args can be answered instantly for
as long as that TTL holds.

Design notes
------------
* **stdlib only.** ``sqlite3`` ships with Python — this adds no dependency to
  ``pyproject.toml``.
* **One connection per operation.** ``sqlite3`` connections are not safe to
  share across threads, and cache operations here are short and infrequent
  relative to a 20s upstream fetch. Opening per call removes a whole class of
  threading bug for a cost that does not matter at this call rate.
* **Never raises.** A cache is an optimisation. A locked DB, a corrupt row, or
  a read-only filesystem degrades to "miss" (or a dropped write) and the caller
  falls back to a real fetch — it must never take down a tool call.
* **TTL travels with the row.** ``ttl_s`` is stored per entry rather than being
  a global setting, so different callers can ask for different freshness
  windows against the same table without invalidating each other.

Environment
-----------
``TRADINGVIEW_MCP_CACHE_DB``  Absolute path to the SQLite file.
                             Default: ``<repo>/data/tool_cache.db``, falling
                             back to ``~/.tradingview-mcp/tool_cache.db`` when
                             the repo directory is not writable.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Optional

__all__ = ["ResultCache", "cache_key_for", "canonical_args_json", "default_db_path"]


_SCHEMA = """
CREATE TABLE IF NOT EXISTS tool_result_cache (
    cache_key   TEXT PRIMARY KEY,
    tool        TEXT NOT NULL,
    args_json   TEXT NOT NULL,
    result_json TEXT NOT NULL,
    computed_at REAL NOT NULL,
    ttl_s       REAL NOT NULL
)
"""

# Short-but-real wait if another process/thread holds the write lock. The cache
# is never on a hot loop, so a bounded wait is cheaper than a spurious miss.
_SQLITE_TIMEOUT_S = 5.0


def canonical_args_json(args: dict[str, Any]) -> str:
    """Deterministic JSON encoding of *args* — key order can never change the key."""
    return json.dumps(args, sort_keys=True, separators=(",", ":"), default=str)


def cache_key_for(tool: str, args: dict[str, Any]) -> str:
    """``sha256(tool + canonical_json(args))``, hex-encoded."""
    payload = f"{tool}{canonical_args_json(args)}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def default_db_path() -> Path:
    """Resolve the cache DB location.

    Order: explicit env var, then this repo's own ``data/`` directory (created
    on demand — it mirrors the existing top-level ``logs/`` convention rather
    than inventing a scattered layout), then a per-user fallback for installs
    where the package directory is not writable.
    """
    override = os.environ.get("TRADINGVIEW_MCP_CACHE_DB")
    if override:
        return Path(override).expanduser()

    # src/tradingview_mcp/core/services/result_cache.py -> repo root
    repo_root = Path(__file__).resolve().parents[4]
    candidate = repo_root / "data" / "tool_cache.db"
    try:
        candidate.parent.mkdir(parents=True, exist_ok=True)
        if os.access(candidate.parent, os.W_OK):
            return candidate
    except OSError:
        pass

    return Path.home() / ".tradingview-mcp" / "tool_cache.db"


class ResultCache:
    """SQLite-backed ``(tool, args) -> result`` cache with per-entry TTL."""

    def __init__(self, db_path: Optional[os.PathLike[str] | str] = None) -> None:
        self.db_path = Path(db_path) if db_path is not None else default_db_path()
        self._ensure_schema()

    # ── internals ──────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=_SQLITE_TIMEOUT_S)
        # WAL lets a reader proceed while a worker is writing — the exact
        # read/write overlap this cache is built to have.
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            pass
        return conn

    def _ensure_schema(self) -> None:
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as conn:
                conn.execute(_SCHEMA)
        except (sqlite3.Error, OSError) as exc:
            self._warn(f"schema init failed for {self.db_path}: {exc!r}")

    @staticmethod
    def _warn(message: str) -> None:
        try:
            print(f"[tradingview_mcp.result_cache] {message}", file=sys.stderr, flush=True)
        except Exception:
            pass

    # ── public API ─────────────────────────────────────────────────────────

    def get(self, tool: str, args: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Return the cached result, or ``None`` if missing, expired or unreadable."""
        key = cache_key_for(tool, args)
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT result_json, computed_at, ttl_s FROM tool_result_cache "
                    "WHERE cache_key = ?",
                    (key,),
                ).fetchone()
        except sqlite3.Error as exc:
            self._warn(f"get failed ({tool}): {exc!r}")
            return None

        if row is None:
            return None

        result_json, computed_at, ttl_s = row
        if (time.time() - float(computed_at)) > float(ttl_s):
            return None

        try:
            return json.loads(result_json)
        except (ValueError, TypeError) as exc:
            # A corrupt row is a miss, never an exception into the tool layer.
            self._warn(f"corrupt cached result for {tool}: {exc!r}")
            return None

    def put(
        self,
        tool: str,
        args: dict[str, Any],
        result: dict[str, Any],
        ttl_s: float,
    ) -> None:
        """Store *result*, replacing any existing entry for the same key."""
        key = cache_key_for(tool, args)
        try:
            payload = json.dumps(result, default=str)
        except (TypeError, ValueError) as exc:
            self._warn(f"result for {tool} is not JSON-serialisable: {exc!r}")
            return

        try:
            with self._connect() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO tool_result_cache "
                    "(cache_key, tool, args_json, result_json, computed_at, ttl_s) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (key, tool, canonical_args_json(args), payload, time.time(), float(ttl_s)),
                )
        except sqlite3.Error as exc:
            self._warn(f"put failed ({tool}): {exc!r}")

    def purge_expired(self) -> int:
        """Delete every entry past its own TTL. Returns the number removed."""
        try:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM tool_result_cache WHERE (? - computed_at) > ttl_s",
                    (time.time(),),
                )
                return cur.rowcount or 0
        except sqlite3.Error as exc:
            self._warn(f"purge failed: {exc!r}")
            return 0
