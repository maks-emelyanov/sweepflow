# Usage and data

Install the core and optional Robinhood wrapper as described in [the README](../README.md#optional-integrations). Commands below use `--no-sync` so uv preserves separately installed integrations. Offline `replay`, `signals`, and `audit` need no credentials.

| Command | Purpose | Requires network or credentials |
| --- | --- | --- |
| `replay` | Simulate complete one-minute CSV sessions | No |
| `signals` | Analyze five-minute CSV candles | No |
| `audit` | Export journal events as JSON lines | No |
| `universe` | Download dated SPY holdings | Network; no broker credentials |
| `discover` | Inspect read-only Robinhood capabilities | Wrapper and Robinhood OAuth |
| `fetch` | Export Robinhood candles to CSV | Wrapper and Robinhood OAuth |
| `scan` / `watch` | Journal one scan / poll for signals | Wrapper and Robinhood OAuth |
| `alpaca-sync` | Reconcile the Alpaca paper account | Alpaca paper credentials; POSIX |
| `paper` | Scan and continuously manage paper brackets | Wrapper, Robinhood OAuth, Alpaca paper credentials; POSIX |
| `live` | Explain why real-money execution is unavailable | No |

Run `uv run --no-sync sweepflow COMMAND --help` for the full options.

## Robinhood and symbol selection

Authenticate through the installed wrapper when using Robinhood data:

```bash
uv run --no-sync robinhood-mcp-wrapper auth login
# Headless alternative: uv run --no-sync robinhood-mcp-wrapper auth login --manual
uv run --no-sync sweepflow discover

# Download a dated, reviewable constituent proxy:
uv run --no-sync sweepflow universe --output data/sp500.json

# Scan or monitor the entire saved universe during the regular session:
uv run --no-sync sweepflow --config config/strategy.toml scan --symbols-file data/sp500.json
uv run --no-sync sweepflow --config config/strategy.toml watch --symbols-file data/sp500.json

# Smaller universe alternatives (use separate journals for different universes):
uv run --no-sync sweepflow scan --etfs --db data/etfs.sqlite
uv run --no-sync sweepflow watch --symbols AAPL MSFT --db data/two-symbols.sqlite
```

Without a symbol override, monitoring downloads State Street's official daily SPY holdings and saves `data/sp500.json`. State Street currently delivers this download as XLSX; the loader reads its embedded holdings date and ticker column directly without an Excel dependency. Cash and non-equity corporate-action rows are excluded. Share classes mean the list need not contain exactly 500 symbols. `--etfs` selects SPY, VOO, and IVV. Explicit JSON snapshots, text lists, and CSV files with a `symbol` or `ticker` column are accepted. Use dated membership lists for historical studies; today's holdings introduce survivorship bias. Source: [State Street SPY](https://www.ssga.com/us/en/individual/etfs/state-street-spdr-sp-500-etf-trust-spy).

Global `--config` goes **before** the subcommand. Settings are strictly validated; misspelled keys fail. All prices and account-risk arithmetic use `Decimal`.

## Data and execution

The adapter discovers Robinhood's actual tools through `RobinhoodMCPClient`, then calls `get_equity_historicals` with explicit `interval="minute"`, `"5minute"`, or `"day"`, `bounds="regular"`, and `adjustment_type="split"`. It batches at most ten symbols and chunks long intraday date ranges. It rejects malformed prices, interval/bounds mismatches, and upstream errors; forming or interpolated bars are excluded. It also exposes read-only account, portfolio, position, and order queries for library consumers. Shadow signals do not represent reconciliation with brokerage positions or executable order proposals.

`scan`, `watch`, `paper`, and preopen preparation fetch the genuine completed daily candle for the immediately preceding regular exchange session. Its high/low exclusively supply PDH/PDL. Daily candles are normalized to that session's open and close, including early closes. Incomplete or absent previous-session five-minute history does not disqualify a symbol; those bars provide causal pivot context. Without that context, a setup needs a pivot confirmed from current-session candles. A missing matching daily candle blocks the affected symbol unless the journal already holds it; candles from another session and intraday extrema cannot substitute. This is the default behavior and requires no configuration change; existing journals remain usable with the same configuration.

When a concurrent historical request has a transport or authentication failure, the default client marks the connection unusable. The source waits for the remaining requests in that batch group before closing it, which signals the wrapper's connection task to shut down its transport. Library consumers that inject a plain `RobinhoodMCPClient` use serial historical requests because that client closes its shared connection immediately on those failures.

Robinhood's historical endpoint is **polled**, not a WebSocket feed; this implementation makes no claim of consolidated SIP coverage. Monitoring initializes from persisted bars, consumes only appended bars for unchanged symbols, and reconstructs affected symbols after revisions or backfills. It emits a signal only while its confirmation is fresh (default 90 seconds) and its setup remains pending. Every scan refreshes the previous-session daily candle and five-minute history from the previous regular session through the latest completed current-session candle. The intraday cutoff is refreshed after daily retrieval to include candles completed during that read; signal freshness is checked after fetching and reconstruction. If daily retrieval reaches the session close, including an early close, the scan returns `market_closed` before requesting intraday history. Historical reads use at most four concurrent ten-symbol batches through the default client. Older retained candles still supply pivot context but are outside that refresh window.

Daily corrections change PDH/PDL and rebuild affected symbols just as intraday corrections rebuild structure and setups. Rebuilding retains each confirmation's original age. `revised_symbols` in scan/preparation output includes both daily and intraday revisions. Atomic `data_revision` events record the candle timestamp, changed fields, and old/new values; daily revisions add `timeframe="day"`. `daily_bars_fetched` counts returned daily candles, and `missing_daily_symbols` identifies symbols without a cached or fetched daily candle for the required session. Revisions do not quarantine symbols: the rebuilt setup determines eligibility, subject to current-session continuity and freshness checks. Legacy quarantine rows remain historical; the legacy `quarantined` output field is empty.

`watch` reconnects after failed data reads and stops after three consecutive failures by default. Continuous `paper` retries data reads while broker reconciliation continues; its `--max-errors` budget applies to Alpaca synchronization failures. No signals are emitted from failed reads. See [Paper trading](paper-trading.md) for entry cancellation and recovery behavior. Use `--poll-seconds`, `--max-errors`, and `--max-signal-age` to tune monitoring; refreshing these inputs for hundreds of symbols increases read volume and may exceed the freshness budget.

Preparation and scans attempt to repair uncached five-minute gaps using genuine one-minute history. A repair requires all five completed, consecutive regular-session minutes; interpolated or incomplete data cannot fill a gap. Optional repairs have a five-second budget, prioritize current-session gaps, and back off persistent older gaps for fifteen minutes. Persisted provenance keeps minute repairs eligible for revalidation after corrections and restarts; a native five-minute candle supersedes its repair. `repaired_bars` reports successful repairs. If Robinhood reports that stored credentials require reauthorization, run `uv run --no-sync robinhood-mcp-wrapper auth login --force` before the next session.

Missing current-session opening bars or internal gaps still block setups. Session and signal records include `previous_session_level_source="daily"` plus `previous_session_complete`, `previous_session_bars`, and `previous_session_expected_bars`. These coverage fields describe previous-session five-minute history and are diagnostic when daily levels are available.

```bash
# Export five-minute bars for strategy inspection:
uv run --no-sync sweepflow fetch --symbols SPY --bar-minutes 5 \
  --start 2026-09-21T13:30:00Z --end 2026-09-22T20:00:00Z \
  --output data/spy_5m.csv
uv run --no-sync sweepflow signals data/spy_5m.csv --db data/signals.sqlite

# Fetch actual minute history through robinhood_mcp_wrapper, then replay it:
uv run --no-sync sweepflow fetch --symbols SPY --bar-minutes 1 \
  --start 2026-09-21T13:30:00Z --end 2026-09-22T20:00:00Z \
  --output data/spy_1m.csv
uv run --no-sync sweepflow --config config/strategy.toml replay data/spy_1m.csv \
  --allow-shorts --db data/replay.sqlite

# Export the audit trail:
uv run --no-sync sweepflow audit data/replay.sqlite > data/decisions.jsonl
```

CSV schema (timestamps are **bar starts**, with timezone offsets or `Z`):

```csv
symbol,timestamp,open,high,low,close,volume,duration_seconds
SPY,2026-09-21T13:30:00Z,101,110,100,101,1000,60
```

`volume` is optional. `duration_seconds` must be 60 for replay or 300 for signal analysis; if omitted, explicitly pass `--bar-minutes 1` or `--bar-minutes 5`. Offline CSV commands have no daily feed: `signals` needs complete five-minute history for the immediately preceding regular session to reconstruct PDH/PDL. Replay requires at least two consecutive complete regular sessions, including previous-day warmup, for every supplied symbol. It rejects missing/duplicate minutes before placing simulated orders and does not turn five-minute bars into invented execution data. Out-of-session minutes are excluded. Use consistently adjusted input prices across sessions.

## Persistence and verification

Signal UUIDs and broker client order IDs retain their original identity scheme across the SweepFlow rename so existing journals and paper orders reconcile without creating duplicate trades. The internal `liquidity:` UUID seed and `liq-` broker prefix are persistent compatibility values, not package or command names.

SQLite stores daily candles separately from intraday candles, alongside atomic before/after revision audits, configuration/universe fingerprints, unique signal IDs, and decision events. Restarting shadow mode reuses matching daily levels, reconstructs state machines from all retained intraday candles (including older confirmed pivots), and suppresses duplicate signals. Daily candles refresh for corrections on each scan. Unchanged candle batches do not rewrite SQLite rows; in-process caches refresh after changes committed through another connection. Candles committed by another writer beyond a scan's captured intraday cutoff remain stored for a later scan. Old revision quarantine records remain historical only. Initial reconstruction and corrected-symbol replay costs grow with journal history. Changing shadow configuration or universe requires a new `--db`; Alpaca paper journals retain order ownership across universe changes. Offline replay is a deterministic fresh run; it is not a resumable live brokerage session. `PaperBroker.to_dict/from_dict` exposes snapshots for further integrations.

```bash
uv run --no-sync pytest
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
```

Tests use fake Robinhood clients and synthetic candles; they do not submit trades. Coverage includes causal pivot confirmation, symmetric setups, BOS deadlines, FVG rejection and entry variants, early closes, missing bars, risk caps, conservative bracket fills, daily breakers, replay ordering, data revisions, and restart deduplication.

See [Strategy and replay](strategy.md) for simulated fill ordering and [Paper trading](paper-trading.md) for broker execution.
