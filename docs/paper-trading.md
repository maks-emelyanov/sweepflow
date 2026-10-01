# Paper trading

Use a dedicated Alpaca **paper** account and a POSIX host (Linux/WSL is the supported operating setup). Install the optional Robinhood wrapper and authenticate as described in [the README](../README.md#optional-integrations) before running the continuous `paper` workflow. One-shot `alpaca-sync` does not need the Robinhood wrapper.

## Alpaca paper account

Copy [../.env.example](../.env.example) to `.env` if you do not already have a credentials file, then add the paper credentials (the file is git-ignored):

```dotenv
ALPACA_API_KEY=your-paper-key
ALPACA_API_SECRET=your-paper-secret
```

The aliases `ALPACA_API_KEY_ID` / `ALPACA_API_SECRET_KEY`, `ALPACA_SECRET_KEY`, and `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY` are also supported. Process environment variables take precedence over `.env`. Credentials are sent only to Alpaca's fixed HTTPS paper endpoint and are never written to the journal or printed. `--env-file` selects another credentials file.

```bash
# Synchronize balances, orders, fills, and managed positions without scanning for entries:
uv run --no-sync sweepflow --config config/strategy.toml alpaca-sync

# Continuously scan State Street SPY constituents and trade in the paper account:
uv run --no-sync sweepflow --config config/strategy.toml paper --allow-shorts

# A smaller universe, or an explicitly saved universe:
uv run --no-sync sweepflow --config config/strategy.toml paper --symbols AAPL MSFT
uv run --no-sync sweepflow --config config/strategy.toml paper --symbols-file data/sp500.json

# One scan plus account reconciliation, then exit:
uv run --no-sync sweepflow --config config/strategy.toml paper --once
```

`paper` runs until interrupted; `alpaca-sync` runs once. Both default to `data/alpaca-paper.sqlite`. Keep this journal across restarts: it records ownership, submission intents, broker IDs, fills, and the daily-loss latch. The account and strategy settings are bound to the journal, while universe membership and the short-entry toggle may change without losing broker state. An account lock prevents concurrent runners from using the same account under the same OS user, including with different journal files. Changing risk/strategy settings requires a deliberate migration after managed orders and positions are closed; do not switch journals to bypass unresolved orders.

Each accepted strategy signal becomes a whole-share `limit` order with `order_class="bracket"`, a take-profit limit, a stop trigger, `time_in_force="gtc"`, and no extended-hours execution. Actual Alpaca equity and buying power determine sizing; the replay-only `risk.account_equity` does not fund the paper account. The existing percentage, daily risk, slot, and exposure caps still apply. Bearish signals require `--allow-shorts` (or configuration), an account that supports shorting, and an active, tradable, shortable, easy-to-borrow asset. Prices are checked against Alpaca precision requirements. Reference: [Alpaca bracket orders](https://docs.alpaca.markets/us/docs/orders-at-alpaca).

The runner persists an intent before sending its deterministic client order ID. An ambiguous POST result is reconciled by that ID; it is never blindly resubmitted. Unknown submissions block further entries until resolved. On restart, still-fresh signals journaled just before a crash can proceed, and existing orders reconcile independently of signal freshness. Filled bracket exits stay active after the entry window ends.

Alpaca activates bracket exits only after the entry fills completely. A detected partial entry is canceled and its attributed exposure is flattened after cancellation is confirmed. Expired or strategy-invalidated unfilled entries are canceled. Before any managed position is flattened, the runner cancels its live bracket legs, confirms their terminal states, refetches the position, and verifies that the quantity matches this strategy's recorded fills. Conflicting/unowned exposure is reported and blocks entries; the runner does not liquidate other strategies' holdings. Unowned risk cannot be safely budgeted, so a dedicated paper account is the normal operating setup.

Broker synchronization runs every five seconds by default, independently of asynchronous Robinhood scans. Scan starts are scheduled thirty seconds apart; a scan taking longer starts its successor after completion without adding another thirty-second delay. Failed scans retain a retry cooldown while broker management continues. Use `--poll-seconds` and `--scan-seconds` to adjust these intervals. The candles are five-minute bars, but revisions are applied on every scan, including between candle boundaries. Signals are checked against the 90-second freshness budget again before submission. Corrected candles rebuild the affected symbol immediately: unchanged setups remain eligible, invalidated unfilled entries are canceled, and changed entry/stop/target prices produce a replacement signal. Replacement requires confirmed cancellation of the old entry, zero prior fills, and the original confirmation still being fresh; corrections never reset signal age. Filled positions retain their existing protective exits, and corrected history cannot create another trade from an already-filled setup or reset the session's executed-setup limit. Stale trailing candles and feed failures still prevent new entries and cancel affected pending entries. Feed errors keep broker management running; `--max-errors` (default three) stops the runner after consecutive Alpaca synchronization failures.

Alpaca GET requests retry temporary transport failures, HTTP 429, and server errors up to three attempts within the original request timeout. Short backoffs respect `Retry-After`; longer server delays defer subsequent reads without using the runner's consecutive-error budget. Order submissions and cancellations are sent once and reconciled by durable broker/client IDs. Error events record sanitized HTTP status, operation, endpoint template, reason, and attempt count when available, followed by recovery events when management resumes. Credentials, upstream response bodies, and arbitrary exception text are excluded.

The daily breaker uses actual marked account equity against the persisted prior-close baseline; once the configured loss is reached, managed entries are canceled and managed positions are closed. The breaker remains latched for the trading date. End-of-day closing starts **60 seconds before the exchange close** (`--eod-seconds`) so there is time to confirm cancellations and send market exits while the market is open. This is a best-effort polled process: network outages or asynchronous fills can prevent flatness before the close. GTC protective exits survive a process outage; subsequent synchronization resolves remaining exposure when the market reopens.

`--once` leaves accepted paper brackets at Alpaca; run continuous `paper` for entry expiry, feed invalidation, daily-loss, and end-of-day management. An orderly interruption cancels unfilled strategy entries and preserves filled positions' protective exits. The paper account is a broker simulation, separate from the [offline minute-bar replay](strategy.md#replay-execution-and-model-limits).

## Trade history dashboard

The optional Alpaca Dashboard is an independent library. Obtain its source checkout separately and install it into SweepFlow's environment:

```bash
uv pip install --python .venv/bin/python --editable /absolute/path/to/alpaca-dashboard
uv run --no-sync alpaca-dashboard
# Open http://127.0.0.1:8050
```

It uses the current project's `.env` credentials and `data/dashboard.sqlite` cache, reads the entire Alpaca paper account, and refreshes every five seconds. Its selected `.env` credential pair takes precedence over shell variables; a partial pair is rejected. This precedence differs from SweepFlow's broker client, which prefers shell variables. The dashboard does not read the trading journal or expose strategy attribution. It runs independently of the trading process and makes only GET broker requests.

```bash
uv run --no-sync alpaca-dashboard \
  --env-file .env \
  --cache-db data/dashboard.sqlite \
  --port 8050 \
  --refresh-seconds 5
```

All dashboard implementation, assets, tests, and detailed documentation live in its separate checkout. Run dashboard development and browser checks there. Use `uv sync --locked --inexact --group dev` when updating SweepFlow's environment so separately installed integrations remain available.

See [Scheduled operations](operations.md) to run the continuous paper workflow with systemd.
