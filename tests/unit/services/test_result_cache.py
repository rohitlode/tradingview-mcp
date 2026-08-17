"""``ResultCache`` unit tests — SQLite-backed tool result cache.

Covers the three behaviours the cache exists to guarantee:
  * a stored result round-trips unchanged (put -> get),
  * a result older than its own ``ttl_s`` is treated as missing,
  * an unknown key is a miss (never a fabricated empty result).
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from tradingview_mcp.core.services.result_cache import ResultCache, cache_key_for


@pytest.fixture()
def cache(tmp_path):
    return ResultCache(db_path=tmp_path / "tool_cache.db")


class TestCacheKey:
    def test_key_is_stable_across_arg_ordering(self):
        a = cache_key_for("multi_timeframe_analysis", {"symbol": "AAPL", "exchange": "NASDAQ"})
        b = cache_key_for("multi_timeframe_analysis", {"exchange": "NASDAQ", "symbol": "AAPL"})
        assert a == b

    def test_key_differs_per_tool_and_per_args(self):
        base = cache_key_for("multi_timeframe_analysis", {"symbol": "AAPL"})
        assert base != cache_key_for("market_sentiment", {"symbol": "AAPL"})
        assert base != cache_key_for("multi_timeframe_analysis", {"symbol": "MSFT"})

    def test_key_is_a_sha256_hex_digest(self):
        key = cache_key_for("t", {"a": 1})
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)


class TestResultCache:
    def test_put_then_get_round_trips(self, cache):
        payload = {"alignment": {"status": "BULLISH", "net_score": 4}, "timeframes": {"1D": {}}}
        cache.put("multi_timeframe_analysis", {"symbol": "AAPL", "exchange": "NASDAQ"}, payload, ttl_s=120.0)

        got = cache.get("multi_timeframe_analysis", {"symbol": "AAPL", "exchange": "NASDAQ"})

        assert got == payload

    def test_missing_key_returns_none(self, cache):
        assert cache.get("multi_timeframe_analysis", {"symbol": "NOPE"}) is None

    def test_expired_entry_returns_none(self, cache):
        cache.put("t", {"symbol": "AAPL"}, {"v": 1}, ttl_s=60.0)
        # Backdate the row well past its ttl_s without sleeping.
        key = cache_key_for("t", {"symbol": "AAPL"})
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute(
                "UPDATE tool_result_cache SET computed_at = computed_at - 3600 WHERE cache_key = ?",
                (key,),
            )

        assert cache.get("t", {"symbol": "AAPL"}) is None

    def test_entry_inside_its_ttl_is_still_fresh(self, cache):
        cache.put("t", {"symbol": "AAPL"}, {"v": 1}, ttl_s=3600.0)
        key = cache_key_for("t", {"symbol": "AAPL"})
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute(
                "UPDATE tool_result_cache SET computed_at = computed_at - 60 WHERE cache_key = ?",
                (key,),
            )

        assert cache.get("t", {"symbol": "AAPL"}) == {"v": 1}

    def test_requested_ttl_stricter_than_stored_ttl_wins(self, cache):
        """A caller asking for a 30s window must NOT be handed a 60s-old row
        just because the writer stored it under a 3600s TTL."""
        cache.put("t", {"symbol": "AAPL"}, {"v": 1}, ttl_s=3600.0)
        key = cache_key_for("t", {"symbol": "AAPL"})
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute(
                "UPDATE tool_result_cache SET computed_at = computed_at - 60 WHERE cache_key = ?",
                (key,),
            )

        assert cache.get("t", {"symbol": "AAPL"}, ttl_s=30.0) is None
        # ...and the same row is still fresh for a caller that asked for more.
        assert cache.get("t", {"symbol": "AAPL"}, ttl_s=300.0) == {"v": 1}

    def test_stored_ttl_stricter_than_requested_ttl_wins(self, cache):
        """The reverse direction: a short-lived (e.g. negatively cached) row is
        not resurrected by a caller asking for a long window."""
        cache.put("t", {"symbol": "AAPL"}, {"v": 1}, ttl_s=15.0)
        key = cache_key_for("t", {"symbol": "AAPL"})
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute(
                "UPDATE tool_result_cache SET computed_at = computed_at - 60 WHERE cache_key = ?",
                (key,),
            )

        assert cache.get("t", {"symbol": "AAPL"}, ttl_s=3600.0) is None

    def test_omitting_ttl_falls_back_to_the_stored_ttl(self, cache):
        cache.put("t", {"symbol": "AAPL"}, {"v": 1}, ttl_s=3600.0)
        key = cache_key_for("t", {"symbol": "AAPL"})
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute(
                "UPDATE tool_result_cache SET computed_at = computed_at - 600 WHERE cache_key = ?",
                (key,),
            )

        assert cache.get("t", {"symbol": "AAPL"}) == {"v": 1}

    def test_unusable_requested_ttl_falls_back_to_stored(self, cache):
        cache.put("t", {"symbol": "AAPL"}, {"v": 1}, ttl_s=3600.0)
        assert cache.get("t", {"symbol": "AAPL"}, ttl_s="not-a-number") == {"v": 1}

    def test_purge_expired_removes_only_dead_rows(self, cache):
        cache.put("t", {"symbol": "OLD"}, {"v": 1}, ttl_s=10.0)
        cache.put("t", {"symbol": "NEW"}, {"v": 2}, ttl_s=3600.0)
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute(
                "UPDATE tool_result_cache SET computed_at = computed_at - 600 "
                "WHERE cache_key = ?",
                (cache_key_for("t", {"symbol": "OLD"}),),
            )

        removed = cache.purge_expired()

        assert removed == 1
        assert cache.get("t", {"symbol": "OLD"}) is None
        assert cache.get("t", {"symbol": "NEW"}) == {"v": 2}

    def test_put_overwrites_same_key(self, cache):
        cache.put("t", {"symbol": "AAPL"}, {"v": 1}, ttl_s=120.0)
        cache.put("t", {"symbol": "AAPL"}, {"v": 2}, ttl_s=120.0)

        assert cache.get("t", {"symbol": "AAPL"}) == {"v": 2}

    def test_schema_matches_the_designed_columns(self, cache):
        cache.put("t", {"symbol": "AAPL"}, {"v": 1}, ttl_s=120.0)
        with sqlite3.connect(str(cache.db_path)) as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(tool_result_cache)")}
        assert cols == {"cache_key", "tool", "args_json", "result_json", "computed_at", "ttl_s"}

    def test_row_stores_the_canonical_args_json(self, cache):
        cache.put("t", {"symbol": "AAPL", "exchange": "NASDAQ"}, {"v": 1}, ttl_s=120.0)
        with sqlite3.connect(str(cache.db_path)) as conn:
            (args_json,) = conn.execute("SELECT args_json FROM tool_result_cache").fetchone()
        assert json.loads(args_json) == {"symbol": "AAPL", "exchange": "NASDAQ"}

    def test_corrupt_result_json_is_a_miss_not_a_crash(self, cache):
        cache.put("t", {"symbol": "AAPL"}, {"v": 1}, ttl_s=120.0)
        with sqlite3.connect(str(cache.db_path)) as conn:
            conn.execute("UPDATE tool_result_cache SET result_json = '{not json'")

        assert cache.get("t", {"symbol": "AAPL"}) is None
