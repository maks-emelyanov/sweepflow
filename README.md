# SweepFlow

[![CI](https://github.com/maks-emelyanov/sweepflow/actions/workflows/ci.yml/badge.svg)](https://github.com/maks-emelyanov/sweepflow/actions/workflows/ci.yml)
[![Python 3.14+](https://img.shields.io/badge/python-3.14%2B-blue)](https://github.com/maks-emelyanov/sweepflow/blob/main/pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

SweepFlow is a Python workflow for researching and paper trading a **prior-day liquidity sweep → close-confirmed break of structure → fair value gap → resting limit entry** strategy. It uses the NYSE calendar, strict candle validation, decimal price arithmetic, and SQLite audit journals.

| Mode | Input | Behavior |
| --- | --- | --- |
| Offline replay | One-minute CSV candles | Simulates entries, bracket exits, sizing, and portfolio limits |
| Historical signals | Five-minute CSV candles | Reports strategy signals without simulated fills |
| Shadow monitoring | Robinhood market data | Journals fresh signals without sending orders |
| Alpaca paper trading | Robinhood signals and an Alpaca paper account | Submits paper limit brackets and reconciles orders and positions |

Monitoring and paper trading take PDH/PDL from Robinhood's completed daily candle for the immediately preceding regular exchange session. Missing previous-session five-minute candles do not disqualify a symbol; those candles supply pivot context. Offline CSV commands require complete previous-session intraday history to reconstruct the levels. See [Usage and data](docs/usage.md#data-and-execution).

**Real-money order submission is unavailable.** Alpaca execution accepts only its fixed HTTPS paper endpoint. SweepFlow uses Robinhood for read-only data; its `live` command explains the execution limitation before making any network call. Synthetic examples and replay results are research outputs, not evidence of profitability.

## Quick start

Use Python **3.14 or newer** and [uv](https://docs.astral.sh/uv/getting-started/installation/). Clone the repository and install the core and development dependencies:

```bash
git clone https://github.com/maks-emelyanov/sweepflow.git
cd sweepflow
uv sync --locked --group dev
uv run --no-sync sweepflow --help

# Included synthetic data: no broker account or credentials needed.
uv run --no-sync sweepflow replay examples/demo_1m.csv --db data/demo.sqlite
uv run --no-sync sweepflow audit data/demo.sqlite > data/decisions.jsonl
```

The demo produces one long trade: entry **102**, stop **99.49**, target **110**, quantity **99**, and simulated P&L **792**. To regenerate its 780 synthetic minute bars, run `uv run --no-sync python examples/make_demo.py`.

The core installation needs no sibling repositories. Network monitoring and paper trading additionally need the optional Robinhood wrapper described below. Paper account locking and the session supervisor require a POSIX system; Linux/WSL is the supported operating setup, and automatic scheduling requires systemd.

## Configuration and credentials

The validated settings are in [config/strategy.toml](config/strategy.toml). Defaults use 0.25% risk per trade, a 1% daily loss limit, four concurrent positions, and a minimum 2.5 reward/risk ratio. Replay starts with hypothetical equity of 100,000; Alpaca paper execution sizes against the actual paper account.

Global `--config` goes **before** the subcommand:

```bash
uv run --no-sync sweepflow --config config/strategy.toml replay examples/demo_1m.csv
```

Offline commands do not need `.env`. For Alpaca paper trading, copy the template only if you do not already have a credentials file:

```bash
cp -n .env.example .env
```

Fill in `ALPACA_API_KEY` and `ALPACA_API_SECRET` with a matching **paper-account** pair. Shell variables override `.env` for SweepFlow; `--env-file` selects a different file. Robinhood authenticates through OAuth and stores its credentials separately. Environment files, credential stores, journals, and runtime data are git-ignored.

## Optional integrations

The Robinhood wrapper and Alpaca dashboard are independent projects. Their source checkouts must be obtained separately; this repository does not assume a checkout location or a public package release. Install integrations into SweepFlow's environment using their actual paths:

SweepFlow expects the wrapper's `robinhood_mcp_wrapper` Python package and `robinhood-mcp-wrapper` command. When upgrading an existing wrapper checkout, rerun the editable install below to refresh its package metadata and console command.

```bash
# Needed for discover/fetch/scan/watch and paper market data.
uv pip install --python .venv/bin/python --editable /absolute/path/to/robinhood
uv run --no-sync robinhood-mcp-wrapper auth login
uv run --no-sync sweepflow discover

# One read-only scan of a small universe.
uv run --no-sync sweepflow scan --symbols AAPL MSFT --db data/shadow.sqlite

# Submit and continuously manage Alpaca PAPER orders (requires credentials).
uv run --no-sync sweepflow --config config/strategy.toml paper --symbols AAPL MSFT

# Optional, independent account dashboard.
uv pip install --python .venv/bin/python --editable /absolute/path/to/alpaca-dashboard
uv run --no-sync alpaca-dashboard
```

Use `uv run --no-sync` after installing these integrations. A plain `uv sync` removes packages outside the core lockfile; to retain them while syncing development dependencies, use `uv sync --locked --inexact --group dev`. Each integration manages its own dependencies and license.

`paper` continues until interrupted. **`paper --once` leaves accepted brackets at Alpaca** and does not provide ongoing expiry or end-of-day management. Keep `data/alpaca-paper.sqlite` across restarts so orders and fills remain attributable to this strategy. Read the [paper trading guide](docs/paper-trading.md) before operating a paper account.

## Documentation

- [Usage and data](docs/usage.md): CLI commands, universe snapshots, CSV schema, feed revisions, and persistence.
- [Strategy and replay](docs/strategy.md): causal setup rules, sizing, fill ordering, and model limits.
- [Paper trading](docs/paper-trading.md): account setup, broker reconciliation, shutdown behavior, and the optional dashboard.
- [Scheduled operations](docs/operations.md): portable systemd templates, exchange hours, installation, and logs.
- [Contributing](CONTRIBUTING.md): development, checks, and pull requests.
- [Security](SECURITY.md): private vulnerability reporting and credential handling.

## Development

```bash
uv sync --locked --inexact --group dev
uv run --no-sync pytest
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv build
```

GitHub Actions checks a clean core installation, tests, lint, formatting, distribution builds, and an installed-wheel replay. Tests use fake clients and synthetic data; they do not submit trades. The Robinhood adapter tests run when its optional wrapper is installed and are explicitly skipped otherwise.

## License

[MIT](LICENSE), copyright 2026 Maks Emelyanov.
