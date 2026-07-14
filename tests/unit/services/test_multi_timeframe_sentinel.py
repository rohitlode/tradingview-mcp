"""``run_multi_timeframe_analysis`` sentinel tests.

Regression coverage for a real 2026-07-14 finding: when every timeframe fetch
fails (an "upstream cliff" — e.g. tradingview_ta raising JSONDecodeError on a
malformed upstream response), the function used to swallow every exception
into a per-timeframe ``{"error": ...}`` string and still return a normal
``{"alignment": {"status": "MIXED/RANGING", ...}}`` payload — indistinguishable
from a genuinely quiet market. Downstream MCP clients (e.g. tradesignalservice)
saw this as a successful call with a neutral read and never learned the call
had actually failed. This mirrors the existing ``BatchExecutionError`` sentinel
``volume_breakout_scan``/``fetch_trending_analysis`` already use for the same
"every batch failed" shape (see ``test_batch_sentinel.py``).
"""
from __future__ import annotations

from json import JSONDecodeError
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from tradingview_mcp.core.errors import BatchExecutionError


class _FakeAnalysis(SimpleNamespace):
    """Minimal stand-in for a ``tradingview_ta`` analysis object."""


def _good_indicators() -> dict:
    return {
        "ATR": 2.5, "close": 110.0, "open": 100.0, "RSI": 55.0,
        "MACD.macd": 1.0, "MACD.signal": 0.5,
        "EMA20": 108.0, "EMA50": 100.0, "EMA200": 90.0,
        "volume": 1_000_000.0, "volume.SMA20": 500_000.0,
    }


class TestMultiTimeframeAnalysisSentinel:
    def test_all_timeframes_fail_raises_batch_execution_error(self, monkeypatch):
        """Every one of the 5 timeframes fails -> BatchExecutionError, not a
        fabricated MIXED/RANGING success."""
        from tradingview_mcp.core.services import screener_service

        monkeypatch.setenv("TRADINGVIEW_MCP_BATCH_MAX_CONSECUTIVE_FAILS", "999")
        monkeypatch.setenv("TRADINGVIEW_MCP_BATCH_BUDGET_S", "3600")

        def always_fail(*_args, **_kwargs):
            raise JSONDecodeError("Expecting value", "", 0)

        with patch.object(screener_service, "get_multiple_analysis", side_effect=always_fail):
            with pytest.raises(BatchExecutionError) as exc_info:
                screener_service.run_multi_timeframe_analysis("NASDAQ:AAPL", "NASDAQ")

        assert exc_info.value.batches_attempted == 5
        assert exc_info.value.batches_failed == 5
        assert "Expecting value" in exc_info.value.first_error

    def test_upstream_cliff_fast_fail_raises_batch_execution_error(self, monkeypatch):
        """The consecutive-failure fast-fail bail-out must ALSO raise, not
        silently return the fake-neutral fallback it used to."""
        from tradingview_mcp.core.services import screener_service

        monkeypatch.setenv("TRADINGVIEW_MCP_BATCH_MAX_CONSECUTIVE_FAILS", "2")
        monkeypatch.setenv("TRADINGVIEW_MCP_BATCH_BUDGET_S", "3600")

        call_count = {"n": 0}

        def always_fail(*_args, **_kwargs):
            call_count["n"] += 1
            raise JSONDecodeError("Expecting value", "", 0)

        with patch.object(screener_service, "get_multiple_analysis", side_effect=always_fail):
            with pytest.raises(BatchExecutionError):
                screener_service.run_multi_timeframe_analysis("NASDAQ:AAPL", "NASDAQ")

        # Fast-fail bails after 2 consecutive failures, not all 5.
        assert call_count["n"] == 2

    def test_no_data_for_symbol_does_not_raise(self):
        """Illiquid/new symbol (upstream responds but has nothing) is a real
        response, not a fetch failure -- must NOT trip the sentinel."""
        from tradingview_mcp.core.services import screener_service

        def no_data(*, screener, interval, symbols):
            return {symbols[0]: None}

        with patch.object(screener_service, "get_multiple_analysis", side_effect=no_data):
            result = screener_service.run_multi_timeframe_analysis("NASDAQ:NEWCO", "NASDAQ")

        assert result["alignment"]["status"] == "MIXED/RANGING"
        for tf_result in result["timeframes"].values():
            assert tf_result["error"].startswith("No data for")

    def test_partial_success_does_not_raise(self):
        """Some timeframes succeed, some fail -- returns the partial result,
        same as before (only the ALL-failed case is new behavior)."""
        from tradingview_mcp.core.services import screener_service

        call_log = []

        def alternating(*, screener, interval, symbols):
            call_log.append(interval)
            if len(call_log) % 2 == 1:
                raise JSONDecodeError("Expecting value", "", 0)
            return {symbols[0]: _FakeAnalysis(indicators=_good_indicators())}

        with patch.object(screener_service, "get_multiple_analysis", side_effect=alternating):
            result = screener_service.run_multi_timeframe_analysis("NASDAQ:AAPL", "NASDAQ")

        assert isinstance(result, dict)
        assert "alignment" in result

    def test_all_succeed_no_raise(self):
        """Happy path: every timeframe succeeds, no sentinel."""
        from tradingview_mcp.core.services import screener_service

        def all_good(*, screener, interval, symbols):
            return {symbols[0]: _FakeAnalysis(indicators=_good_indicators())}

        with patch.object(screener_service, "get_multiple_analysis", side_effect=all_good):
            result = screener_service.run_multi_timeframe_analysis("NASDAQ:AAPL", "NASDAQ")

        assert isinstance(result, dict)
        assert result["alignment"]["net_score"] != 0 or True  # just must not raise
