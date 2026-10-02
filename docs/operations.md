# Automatic paper schedule

The optional user systemd schedule supports Linux and WSL distributions with
systemd enabled. Complete the [installation and authentication steps](../README.md)
first, including the Robinhood wrapper, the checkout's `.venv`, and Alpaca paper
credentials in `.env`.

The templates in `ops/systemd/` run the State Street → Robinhood → Alpaca paper
workflow **30 minutes before the NYSE open** until **10 minutes after the close**.
Normal hours are **09:00–16:10 America/New_York**. The exchange calendar skips
weekends and holidays and ends at **13:10** on a 13:00 early close. New York time
handles daylight saving changes.

## Render and install

From the checkout root, generate units that point to this checkout and review them:

```bash
uv run --no-sync python ops/render_systemd.py data/systemd
cat data/systemd/sweepflow-paper.service
systemd-analyze --user verify data/systemd/sweepflow-paper.service data/systemd/sweepflow-paper.timer
```

The renderer only writes the two units to the supplied output directory. It does
not install, enable, or start them. `--root /absolute/path/to/sweepflow` selects a
different checkout. Paths may contain spaces, `%`, and `$`; paths with quotes,
backslashes, control characters, or leading/trailing whitespace are rejected.
Generated units remain valid after changing the shell's working directory.

The service uses `config/strategy.toml`, `.env`, and `data/alpaca-paper.sqlite`
relative to its checkout, and enables eligible paper shorts with `--allow-shorts`.
Remove that flag from the rendered service if shorts are unwanted. Review the
configuration and credentials before enabling the schedule.

Install the reviewed units and enable the timer:

```bash
install -d -m 755 ~/.config/systemd/user
install -m 644 data/systemd/sweepflow-paper.service data/systemd/sweepflow-paper.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now sweepflow-paper.timer
```

These commands change the local scheduler. Enabling the timer can start a paper
session immediately when its calendar guard permits. Keep the existing paper
journal across restarts; it records order ownership and unresolved submissions.
After moving the checkout or editing the templates, render and install the units
again, then reload the user manager. Stop an existing service before moving its
checkout.

To keep the user manager available after logout and start it at Linux boot,
optionally enable lingering:

```bash
loginctl enable-linger "$USER"
```

**The host and Linux/WSL instance must remain running with network access throughout
the trading session.** This schedule does not boot Windows, start a stopped WSL
distribution, or wake a sleeping host. Credentials remain in `.env` and Robinhood's
credential store; they are not embedded in the units. Logs use the system journal.

## Session behavior

At **09:00 ET**, startup loads configuration, synchronizes the paper account, and
refreshes State Street's S&P 500 universe. Before the open, it connects to Robinhood
and downloads the preceding regular session's genuine completed daily candle plus
its five-minute history, repairing gaps when possible. Preparation stores these
separately in the existing journal and creates no signals or entries. It reports
`ready` only when both daily candles and complete five-minute history are available
for every symbol. Incomplete history, missing daily candles, and connection failures
retry while account reconciliation continues; successful preparation waits for the
open. A restart retains cached history and refreshes it for corrections.
`paper_phase`, `paper_universe_ready`, and `paper_preparation` events show progress.

At the **exchange open, normally 09:30 ET**, Alpaca's market-open flag enables scans;
the scanner independently checks the exchange calendar and consumes only completed
regular-session candles. New entries recheck the broker clock. A late startup or
unfinished preparation catches up through the normal scanner without waiting for
preparation to become ready. PDH/PDL come exclusively from the matching previous
regular session's daily candle. Missing previous-session five-minute history is
diagnostic and may delay causal pivot confirmation, but does not block daily levels.
`daily_bars_fetched` and `missing_daily_symbols` show daily availability;
`previous_session_complete`, `previous_session_bars`, and
`previous_session_expected_bars` report five-minute coverage in session and signal
records. Current-session missing opening bars or internal gaps, unavailable daily
levels, and freshness checks still block setups.
Broker reconciliation runs every five seconds and scans follow their configured
cadence. Each scan refreshes its intraday cutoff after downloading daily candles,
so a five-minute candle completed during that download can be processed in the same
scan. Freshness is checked after data retrieval and reconstruction and again before
paper submission; the default maximum confirmation age is 90 seconds.

End-of-day closing begins one minute before the exchange close, normally **15:59
ET**, or **12:59 ET** on a 13:00 early close. Scans stop at the close; reconciliation
continues for ten more minutes. Closing remains a best-effort process when network
failures or asynchronous fills intervene. A closed-market `paper --once` run only
reconciles the account.

The timer checks for a missing service every minute during normal trading-day
hours and on user-manager startup. A running service is not duplicated. Failed
sessions restart after 30 seconds. `Persistent=true` catches missed activations;
the Python calendar guard prevents after-hours catchups from opening a session.
The account lock also protects against concurrent manual runs. The supervisor
forwards a graceful interrupt to the paper runner when its session ends or systemd
stops it, then uses bounded termination if the process cannot stop. An orderly
interruption cancels unfilled strategy entries and preserves filled positions'
protective exits.

## Inspect and stop

```bash
# Inspect the installed schedule and logs:
systemctl --user status sweepflow-paper.timer sweepflow-paper.service
systemctl --user list-timers sweepflow-paper.timer
journalctl --user -u sweepflow-paper.service -f

# Disable future starts and stop the current paper session:
systemctl --user disable --now sweepflow-paper.timer
systemctl --user stop sweepflow-paper.service

# Re-enable the schedule:
systemctl --user enable --now sweepflow-paper.timer
```

For manual commands after installing the optional local wrapper, use
`uv run --no-sync sweepflow ...` so uv preserves that installation. The scheduled
service invokes `.venv/bin/python` directly and does not run uv synchronization.
