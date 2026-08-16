# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Fixed
- **Webshare proxy was only wired to 3 of 9 upstream-calling services** —
  `yahoo_finance_service.py`, `sentiment_service.py`, and (partially,
  direct-first-then-proxy-fallback) `backtest_service.py` used it;
  `screener_provider.py` (the module every scanner-backed tool routes
  through — 20 of the 30 MCP tools, including all EGX tools and every
  crypto screener), `options_service.py`, `extended_hours_service.py`,
  `bitcoin_market_service.py`, and `news_service.py` all hit their
  upstream directly off the host's own IP. Confirmed live: a sequential
  single-tool-at-a-time test cliffed the unproxied scanner path after
  ~16 calls with a hard `scanner.tradingview.com` empty-body failure that
  did not clear even 90+ seconds later — while every proxied/Yahoo/
  CoinGecko/RSS call kept succeeding throughout, proving the failure was
  scoped to that one unproxied egress. Every upstream-calling service now
  routes through the proxy when configured (`PROXY_ENABLED=true`):
  `screener_provider.py`'s `_scan_with_retry`/`resilient_get_multiple_analysis`/
  `fetch_atr_for_tickers` now pass `proxies=get_proxy()`;
  `options_service.py`'s cookie-jar opener and `feedparser.parse()` calls
  in `news_service.py` now include the new `proxy_manager.get_proxy_handlers()`;
  `extended_hours_service.py` and `bitcoin_market_service.py` now build
  their request via `build_opener_with_proxy()` instead of raw
  `urllib.request.urlopen()`; `backtest_service.py`'s direct-first
  fallback-to-proxy pattern was simplified to always use the proxy,
  matching every other service. Degrades gracefully to a direct request
  when `PROXY_ENABLED=false` or credentials are unset — unchanged for
  users running without a proxy.
- **`coin_analysis` ATR null bug**: `tradingview_ta` omits the `ATR` column from
  its analysis payload, which left `indicators["ATR"]` (and every downstream
  consumer — stop-loss sizing, trade-quality score, volatility metrics) at
  `None` on every call. `analyze_coin` now falls back to a direct
  `scanner.tradingview.com/<market>/scan` request via the new
  `fetch_atr_for_ticker()` helper in `screener_provider.py`. The lookup is
  best-effort (silent `None` on any network / parse failure) and only fires
  when `tradingview_ta` returned no ATR, so it does not regress healthy
  payloads. Timeframe → resolution mapping is shared with the existing
  screener column logic (`5m→5`, `15m→15`, `1h→60`, `4h→240`, `1D/1W/1M`
  unchanged); unknown timeframes degrade to the unsuffixed `ATR` column.

### Changed
- `requests` is now an explicit dependency in `pyproject.toml`. It was already
  pulled in transitively by `tradingview-screener` / `tradingview-ta`, but the
  new ATR injection path uses it directly, so it is no longer safe to rely on
  the transitive resolution.

## [0.7.1] - 2026-04-14

### Added
- **MEXC Exchange Support** (`exchange="MEXC"`):
  - 420 MEXC trading pairs added — including many unique pairs not available on Binance, KuCoin, or Bybit
  - Fully supported in all tools: `top_gainers`, `top_losers`, `bollinger_scan`, `coin_analysis`, `multi_agent_analysis`, `volume_breakout_scanner`, `consecutive_candles_scan`, `advanced_candle_pattern`, and more
  - MEXC correctly categorized as a crypto screener market

---

## [0.7.0] - 2026-03-29

### Added
- **Walk-Forward Backtesting** (`walk_forward_backtest_strategy`):
  - Splits data into N folds (train/test) to validate strategy on unseen forward data
  - Per-fold in-sample vs out-of-sample return comparison
  - **Robustness score** (test/train ratio): ROBUST ≥ 0.8 | MODERATE ≥ 0.5 | WEAK ≥ 0.2 | OVERFITTED < 0.2
  - Aggregate out-of-sample metrics: Sharpe, win rate, max drawdown, total return
  - Supports 2–10 splits, configurable train ratio, both 1d and 1h intervals
- **Full Trade Log** (`include_trade_log=True`):
  - Per-trade breakdown: entry/exit date & price, holding days, gross/net return %, cost %
  - Running capital and cumulative return at each trade
- **Equity Curve** (`include_equity_curve=True`):
  - Capital value + drawdown % at each trade exit — ready for charting
- **1h (Hourly) Timeframe** (`interval="1h"`):
  - All strategies and compare now support intraday hourly data
  - Sharpe ratio annualization corrected for 1h bars (252 × 6 trading hours)
  - Works on `backtest_strategy`, `compare_strategies`, and `walk_forward_backtest_strategy`

### Changed
- `backtest_strategy` tool: added `interval`, `include_trade_log`, `include_equity_curve` params
- `compare_strategies` tool: added `interval` param; now documents all 6 strategies (was 4)
- `run_backtest()` now returns last 5 trades always (`recent_trades`) for quick inspection
- Sharpe ratio calculation now uses interval-aware annualization factor

---

## [0.6.0] - 2026-03-29

### Added
- **Backtesting Engine v2** (`backtest_strategy`, `compare_strategies`):
  - 6 trading strategies: RSI, Bollinger Band, MACD, EMA Cross, **Supertrend** (🔥 trending 2025), **Donchian Channel** (Turtle Trader classic)
  - Institutional-grade metrics: Sharpe Ratio, Calmar Ratio, Expectancy, Profit Factor, Max Drawdown
  - Transaction cost simulation: per-trade commission + slippage
  - Buy-and-hold benchmark comparison
  - Single OHLCV fetch for `compare_strategies` (all 6 strategies in ~0.3s)
- **Yahoo Finance Integration** (`yahoo_price`, `market_snapshot`):
  - Real-time quotes for stocks, crypto, ETFs, indices (S&P500, NASDAQ, VIX), FX
  - Global market snapshot with 14 instruments across 4 asset classes
  - Turkish stocks supported (THYAO.IS, SASA.IS...)
- **Webshare Rotating Proxy Manager**:
  - 250 sticky sessions for rate-limit bypass
  - Direct-first + proxy-fallback architecture for reliability
  - Zero-config for users (optional env-based configuration)
- **Technical Indicators (pure Python, zero deps)**:
  - ATR (Average True Range)
  - Supertrend
  - Donchian Channel

### Changed
- `compare_strategies` now fetches OHLCV once and runs all strategies on cached data (5x faster)
- Yahoo Finance data fetching uses direct connection first, proxy fallback only on failure

## [0.5.0] - 2026-03-29

### Added
- **Real-Time Market Sentiment (Agent-Reach Integration)**: Integrated Reddit JSON API to track symbol sentiment across finance communities (`market_sentiment`).
- **Live Financial News RSS**: Added `fetch_news` service via `feedparser` to track real-time headlines across Reuters, CoinDesk, and CoinTelegraph (`financial_news`).
- **Combined Analysis Power Tool**: The new `combined_analysis` tool merges TradingView technicals, Reddit sentiment, and live news into a single confluence analysis (signals agree/conflict, confidence score, full recommendation).
- Added `feedparser` dependency to `pyproject.toml`.

## [0.4.0] - 2026-03-29

### Added
- **EGX (Egyptian Exchange) Full Support**: Complete trading infrastructure for the Egyptian Stock Market.
  - `egx_market_overview`: Top gainers, losers, most active stocks, and market breadth stats (advancing/declining/unchanged).
  - `egx_sector_scan`: Scan across 18 EGX sectors (banks, real_estate, healthcare_and_pharma, technology, etc.).
  - `egx_stock_screener`: Cross-sectional ranking with auto trade plan generation.
  - `egx_trade_plan`: Single-stock detailed trade setup (entry, stop-loss, targets, R:R).
  - `egx_fibonacci_retracement`: Fibonacci retracement + extension levels with golden pocket detection.
  - 6 EGX indices: EGX30, EGX70, EGX100, SHARIAH33, EGX35LV, TAMAYUZ (200+ symbols).
- **3-Layer Stock Decision Engine**:
  - Layer A: 100-point stock ranking model (trend, momentum, risk, fundamentals).
  - Layer B: Trade setup engine (entry points, stop-loss, targets, support/resistance, R:R).
  - Layer C: Trade quality scoring (structure, risk/reward, volume, liquidity).
- **Liquidity-Aware Scoring**: Hard grade caps prevent illiquid stocks from ever receiving "Strong" or "Elite" grades.
- **23 Technical Indicators**: Expanded from 5 → 23: CCI, Williams %R, Awesome Oscillator, Momentum, Parabolic SAR, Ichimoku, Stoch RSI, ADX +DI/-DI, Hull MA, VWMA, Ultimate Oscillator, full EMA/SMA suites.
- **Multi-Timeframe Alignment**: Weekly→Daily→4H→1H→15m bias analysis with timeframe-specific advice.

### Fixed
- Hardcoded `"crypto"` market type in `_fetch_multi_changes`, `_fetch_multi_timeframe_patterns`, and `screener_provider` — now dynamically resolved per exchange (egypt, turkey, america, etc.).
- `volume_confirmation_analysis` was appending `"USDT"` to stock symbols — now exchange-aware with proper `EGX:SYMBOL` formatting.

### Changed
- MCP server name updated to "TradingView Multi-Market Screener".
- All tool descriptions updated to reference both crypto and stock exchanges.
- Added `is_stock_exchange()`, `get_market_type()`, and `STOCK_EXCHANGES` helpers to `validators.py`.

## [0.3.0] - 2026-03-24

### Added
- **Docker Support**: Official Dockerfile and docker-compose.yml for easy 1-click self-hosting.
- **PyPI Release**: Added proper metadata and structuring for PyPI distribution (`pip install tradingview-mcp`).

## [0.2.0] - 2026-03-24

### Added
- **Multi-Agent Trading Framework**: Introduced `multi_agent_analysis` MCP tool.
  - **Technical Analyst Agent**: Analyzes RSI, MACD, and Bollinger Bands.
  - **Sentiment Analyst Agent**: Calculates momentum and produces a sentiment score.
  - **Risk Manager Agent**: Evaluates volatility (BBW) and mean reversion risk.
- **Debate System**: Agents combine their scores to provide a single, logical Framework Decision (Strong Buy, Buy, Hold, Sell, Strong Sell) with confidence levels.

### Changed
- Repositioned the project from a "screener" to an "AI Trading Intelligence Framework".
- Updated `README.md` to reflect the new architecture.

## [0.1.0] - Initial Release
- Basic MCP Server setup.
- Bollinger Band squeeze detection.
- Consecutive candle pattern detection.
- Real-time market screening (gainers, losers).
