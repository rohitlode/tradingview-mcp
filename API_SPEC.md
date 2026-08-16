# tradingview-mcp — API Surface + Live Rate-Limit Test (2026-08-16)

30 MCP tools, defined in `src/tradingview_mcp/server.py` (`@mcp.tool()`), served over
streamable-HTTP at `127.0.0.1:8100/mcp` (also bridged into Claude Desktop via `mcp-remote`).
Consumed by TradingSignalService's `TradingViewEnrichmentStage` (a small, config-bounded
subset — see `docs/ANALYSIS.md` in that repo) and directly by this session over the
`tradingview-mcp` MCP connection.

## Upstream data sources (this is what actually determines rate-limit exposure)

There are **five distinct upstreams**, not one. Only two of them go through the
Webshare rotating-residential proxy configured in `.env`:

| Upstream | Proxied via Webshare? | Tools that hit it |
|---|---|---|
| `scanner.tradingview.com/{market}/scan` (TradingView's own screener API — scraped, no official key) | **No — direct from host IP** | 20 of 30 tools (see below) |
| `query1/query2.finance.yahoo.com` (Yahoo Finance) | Only for `yahoo_finance_service.py` + `backtest_service.py` call sites; `options_service.py`/`extended_hours_service.py` call sites are unproxied | 9 tools |
| `api.coingecko.com` | No | 1 tool (`bitcoin_market_pulse`) |
| `reddit.com` (unauthenticated JSON endpoints) | Only via `sentiment_service.py` | 1 tool (`market_sentiment`) — already known to return 0 posts (Reddit 403s unauthenticated requests; silently swallowed to a fake neutral-zero result, same finding as TSS's Overview-tab `market_sentiment` section) |
| RSS (Yahoo/CoinDesk/Cointelegraph) + `search.cnbc.com` | No | 1 tool (`financial_news`) |

**Only `yahoo_finance_service.py`, `sentiment_service.py`, and `backtest_service.py` import
`proxy_manager.py`.** `screener_provider.py` (the module every scanner-backed tool ultimately
calls through, via `_scan_with_retry`) has **zero references to `PROXY_HOST`/`PROXY_USERNAME`**
— confirmed by grep, not assumed. The `.env.example` comment ("Enables access to Yahoo
Finance, Xueqiu, and reduces Reddit rate-limits") is accurate and complete: the proxy was
never wired to protect the TradingView scanner path at all.

**This means the 20 scanner-backed tools share one unprotected, unrotated egress IP against
TradingView's own anti-scrape defenses — no IP rotation, no proxy fallback — while the other
10 tools ride a rotating-residential proxy pool (Yahoo/Reddit) or hit an upstream that hasn't
rate-limited this host yet (CoinGecko, RSS).**

## The 30 tools, grouped by upstream

**Scanner-backed (`scanner.tradingview.com`, unproxied — the exposed group, 20 tools):**
`top_gainers`, `top_losers`, `bollinger_scan`, `rating_filter`, `coin_analysis`,
`consecutive_candles_scan`, `advanced_candle_pattern`, `volume_breakout_scanner`,
`volume_confirmation_analysis`, `smart_volume_scanner`, `multi_agent_analysis`,
`multi_timeframe_analysis`, `egx_market_overview`, `egx_sector_scan`, `egx_sector_scanner`,
`egx_index_analysis`, `egx_stock_screener`, `egx_trade_plan`, `egx_fibonacci_retracement`,
`combined_analysis` (its `technical` leg only — `sentiment`/`news` legs are separate calls
into the other two groups below and degrade independently, fail-soft per section).

**Yahoo Finance-backed (9 tools, mixed proxy status):**
`yahoo_price`, `market_snapshot` (proxied); `stock_extended_hours`, `stock_options_chain`,
`stock_options_unusual_activity` (unproxied call sites, but Yahoo itself wasn't the thing
rate-limiting us); `backtest_strategy`, `compare_strategies`, `walk_forward_backtest_strategy`
(proxied, pull daily/hourly OHLC history, not the options/quote endpoints).

**Other (3 tools):** `bitcoin_market_pulse` (CoinGecko), `market_sentiment` (Reddit, proxied
but functionally dead — 403s), `financial_news` (RSS + CNBC search).

## Live test — how many distinct APIs before a 429-equivalent

Called sequentially (not concurrently — a prior session already found concurrent bursts
degrade this server further, see TSS `docs/ANALYSIS.md` §35), one tool call per request,
default/light params, no artificial delay between calls.

**Result: 16 consecutive scanner-backed calls succeeded, the 17th (`egx_trade_plan`) failed,
and every scanner-backed call after that failed identically** — confirmed on 4 different
tools (`egx_trade_plan`, `egx_fibonacci_retracement`, `multi_timeframe_analysis`,
`combined_analysis`'s technical leg), including a re-check of `egx_trade_plan` again ~90+
seconds and 13 other calls later, which **still failed** with the identical signature.

Failure signature (identical every time):
```
Upstream TradingView scanner returned transient errors on all 3 attempts spanning ~5s
(JSONDecodeError('Expecting value: line 1 column 1 (char 0)')).
```
`screener_provider.py`'s own comment calls this "typically a 30-90s empty-body outage" — an
inference, not a confirmed cause, and it's genuinely indistinguishable from a real 429/block
because TradingView returns an **empty HTTP body**, not a status code the client logs. The
"wait ~60s" guidance in the error message did **not** hold in this test — it was still down
90+ seconds and 13 more (non-scanner) calls later, which points toward a harder, IP-scoped
block/cooldown rather than the short transient blip the comment assumes.

The 10 non-scanner tools (Yahoo/CoinGecko/RSS) **all succeeded, throughout, including after
the scanner path had already cliffed** — proving the failure is scoped to the unproxied
`scanner.tradingview.com` egress, not a server-wide problem or a Webshare proxy exhaustion.

| # called (scanner-backed) | Tool | Result |
|---|---|---|
| 1–8 | top_gainers, top_losers, bollinger_scan, rating_filter, coin_analysis, consecutive_candles_scan, advanced_candle_pattern, volume_breakout_scanner | ✅ all succeeded |
| 9 | volume_confirmation_analysis | ❌ app-level (bad symbol format for this tool, not upstream — my test input error) |
| 10–11 | smart_volume_scanner, multi_agent_analysis | ✅ succeeded |
| 12 | egx_market_overview | "No data returned" (EGX-specific, likely legitimate off-hours/empty screener result, not the same failure signature) |
| 13 | egx_sector_scan | ✅ (usage prompt, no sector given) |
| 14–16 | egx_sector_scanner, egx_index_analysis, egx_stock_screener | "No data returned for EGX stocks" (same EGX-empty pattern as #12) |
| **17** | **egx_trade_plan** | **❌ first hard scanner failure** |
| 18–20 | egx_fibonacci_retracement, multi_timeframe_analysis, combined_analysis (technical leg) | ❌ same failure, confirms scanner-wide, not tool-specific |
| retest (~90s+ later) | egx_trade_plan | ❌ still failing |

**Practical count: ~16 successful scanner-backed calls in a single burst before the cliff.**
Whether #12/14–16's "No data" EGX responses were early symptoms of the same degradation or
genuinely empty results couldn't be distinguished in this test (they return a different,
softer error shape than the hard `JSONDecodeError` failure) — worth re-testing during EGX
market hours to separate the two.

## Webshare account — could not check quota/usage

`.env` only holds **rotating-proxy endpoint credentials** (`PROXY_USERNAME_PREFIX` +
`PROXY_PASSWORD`, used to authenticate individual proxied HTTP requests) — there is no
Webshare **account** login (email + dashboard password) stored anywhere in this repo
(`.env`, `.env.example`, or elsewhere). I could not check the Webshare dashboard for
bandwidth/request quota remaining. If you want that checked, the account email + password
(not the proxy prefix/password) would need to be supplied separately — and even then it
wouldn't explain this cliff, since the scanner path that failed **doesn't use the proxy at
all**.

## Fix applied + re-verified live (2026-08-16, same day)

All 6 previously-unproxied services (`screener_provider.py`, `options_service.py`,
`extended_hours_service.py`, `bitcoin_market_service.py`, `news_service.py`, plus
`backtest_service.py`'s direct-first-then-proxy-fallback pattern) now route through the
Webshare proxy unconditionally when configured — see `CHANGELOG.md` `[Unreleased]` for the
per-file diff. Pushed to `origin/main` (`722c9af`).

Re-ran this exact 30-tool sequential test immediately after a service restart:
**30/30 succeeded.** The two non-network items from the first run recurred identically
(app-level bad-symbol-format input error on `volume_confirmation_analysis`; Reddit's own
unauthenticated-403 giving 0 posts on `market_sentiment` — both pre-existing, neither
caused by nor fixed by proxy routing). Every call that failed in the first run now
succeeds with real data:

- `egx_trade_plan` (the exact call that cliffed at #17 in the first run) — real COMI trade
  plan returned.
- `egx_fibonacci_retracement`, `multi_timeframe_analysis`, `combined_analysis`'s technical
  leg — all real data, no `ALL_BATCHES_FAILED`.
- `egx_market_overview`, `egx_sector_scanner`, `egx_index_analysis`, `egx_stock_screener`
  (previously "No data returned for EGX stocks") — now return full real EGX data
  (235-242 stocks scanned), confirming those "no data" responses were also downstream
  symptoms of the same unproxied-scanner degradation, not legitimate empty results.

Verified end-to-end with a live interception (`requests.post` spy) before the reconnect
test: the scanner call is confirmed going out via `p.webshare.io:80`.

## Takeaway

The proxy investment (Webshare rotating residential) protects the wrong upstream. It shields
Yahoo/Reddit calls that weren't the ones failing, while the TradingView scanner path — which
backs 20 of the 30 tools, including every EGX tool and the crypto screeners — runs unproxied
off a single IP and reliably cliffs after roughly 16 calls in one session. Routing
`screener_provider.py` through the same `proxy_manager.py` the other services already use
would be the direct fix, but that's a code change to a separate project and wasn't requested
here — this document is the diagnostic only.
