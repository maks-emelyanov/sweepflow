"""Commands for universe snapshots, discovery, signals, and one-minute replay."""

from __future__ import annotations

import argparse
import asyncio
import math
import sys
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from sweepflow.config import AppConfig, load_config
from sweepflow.data import FIVE_MINUTES, read_csv, write_csv
from sweepflow.integrations import load_robinhood_data_source, require_live_execution
from sweepflow.monitor import scan
from sweepflow.replay import replay
from sweepflow.storage import Journal, dumps
from sweepflow.strategy import StrategyEngine
from sweepflow.universe import (
    SP500_ETFS,
    fetch_sp500_universe,
    load_symbols,
    normalize_symbol,
    save_universe,
)


def timestamp(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError("include a timezone offset or Z")
        return result
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def parser() -> argparse.ArgumentParser:
    app = argparse.ArgumentParser(description=__doc__)
    app.add_argument(
        "--config", type=Path, help="TOML configuration (defaults to built-in settings)"
    )
    commands = app.add_subparsers(dest="command", required=True)
    commands.add_parser("discover", help="Read current Robinhood MCP capabilities; does not trade")
    universe = commands.add_parser("universe", help="Download dated S&P 500 constituents")
    universe.add_argument("--output", type=Path, default=Path("data/sp500.json"))
    for name, help_text in (
        ("fetch", "Export Robinhood one-minute or five-minute bars"),
        ("scan", "One read-only scan of current completed Robinhood candles"),
        ("watch", "Poll completed candles and journal fresh signals; does not trade"),
    ):
        command = commands.add_parser(name, help=help_text)
        select = command.add_mutually_exclusive_group()
        select.add_argument("--symbols", nargs="+", help="Explicit symbols (overrides universe)")
        select.add_argument("--symbols-file", type=Path, help="Dated JSON, CSV, or text universe")
        select.add_argument("--etfs", action="store_true", help="Use SPY, VOO, IVV")
        if name == "fetch":
            command.add_argument("--start", type=timestamp, required=True)
            command.add_argument("--end", type=timestamp, required=True)
            command.add_argument("--output", type=Path, required=True)
            command.add_argument(
                "--bar-minutes",
                type=int,
                choices=(1, 5),
                default=1,
                help="1 for execution replay (default); 5 for signal analysis",
            )
        else:
            command.add_argument("--db", type=Path, default=Path("data/shadow.sqlite"))
            command.add_argument("--max-signal-age", type=float, default=90, metavar="SECONDS")
        if name == "watch":
            command.add_argument("--poll-seconds", type=float, default=30)
            command.add_argument("--max-errors", type=int, default=3)
    for name in ("paper", "alpaca-sync"):
        command = commands.add_parser(
            name,
            help=(
                "Trade the strategy in Alpaca paper"
                if name == "paper"
                else "Reconcile Alpaca paper account, positions, and strategy orders"
            ),
        )
        command.add_argument("--db", type=Path, default=Path("data/alpaca-paper.sqlite"))
        command.add_argument("--env-file", type=Path, default=Path(".env"))
        command.add_argument(
            "--allow-shorts",
            action="store_true",
            help="Enable paper shorts when Alpaca confirms borrow availability",
        )
        command.add_argument(
            "--eod-seconds",
            type=int,
            default=60,
            help="Begin closing managed positions this many seconds before close",
        )
        if name == "paper":
            select = command.add_mutually_exclusive_group()
            select.add_argument("--symbols", nargs="+")
            select.add_argument("--symbols-file", type=Path)
            select.add_argument("--etfs", action="store_true")
            command.add_argument("--once", action="store_true", help="Run one scan and synchronize")
            command.add_argument(
                "--poll-seconds", type=float, default=5, help="Alpaca reconciliation interval"
            )
            command.add_argument(
                "--scan-seconds", type=float, default=30, help="Delay between strategy scans"
            )
            command.add_argument(
                "--max-errors",
                type=int,
                default=3,
                help="Stop after this many consecutive Alpaca sync failures",
            )
            command.add_argument("--max-signal-age", type=float, default=90)
    for name, help_text in (
        ("replay", "Simulate brackets using complete one-minute CSV sessions"),
        ("signals", "Analyze five-minute CSV candles without simulated fills"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("csv", type=Path)
        command.add_argument("--db", type=Path, help="Optional SQLite audit journal")
        command.add_argument(
            "--bar-minutes",
            type=int,
            choices=(1, 5),
            help="Required when CSV omits duration_seconds",
        )
        if name == "replay":
            command.add_argument(
                "--allow-shorts",
                action="store_true",
                help="Enable hypothetical short execution in paper replay",
            )
    audit = commands.add_parser("audit", help="Export recorded decisions as JSON lines")
    audit.add_argument("db", type=Path)
    commands.add_parser("live", help="Explain unavailable Robinhood execution capabilities")
    return app


def resolve_symbols(args: argparse.Namespace, config: AppConfig) -> tuple[str, ...]:
    if args.symbols:
        return tuple(dict.fromkeys(normalize_symbol(symbol) for symbol in args.symbols))
    if args.symbols_file:
        return load_symbols(args.symbols_file)
    if args.etfs:
        return SP500_ETFS
    if config.symbols:
        return tuple(normalize_symbol(symbol) for symbol in config.symbols)
    if config.universe == "etfs":
        return SP500_ETFS
    if config.universe == "custom":
        raise ValueError("Custom universe requires symbols in config or --symbols-file")
    snapshot = fetch_sp500_universe()
    save_universe(snapshot, Path("data/sp500.json"))
    print(
        f"Using {len(snapshot.symbols)} State Street SPY holdings as of {snapshot.as_of}",
        file=sys.stderr,
    )
    return snapshot.symbols


async def network_command(args: argparse.Namespace, config: AppConfig) -> None:
    source_factory = load_robinhood_data_source()
    if args.command == "discover":
        async with source_factory() as source:
            print(dumps(await source.discover()))
        return
    symbols = resolve_symbols(args, config)
    if args.command == "fetch":
        async with source_factory() as source:
            result = await source.get_bars(
                symbols, args.start, args.end, bar_minutes=args.bar_minutes
            )
        bars = sorted(
            (bar for values in result.values() for bar in values),
            key=lambda bar: (bar.start, bar.symbol),
        )
        if not bars:
            raise ValueError("No completed regular-session bars returned")
        write_csv(args.output, bars)
        print(dumps({"bars": len(bars), "symbols": len(symbols), "output": str(args.output)}))
        return
    if not 0 < args.max_signal_age <= 300:
        raise ValueError("--max-signal-age must be between 0 and 300 seconds")
    if args.command == "watch" and (args.poll_seconds < 1 or args.max_errors < 1):
        raise ValueError("--poll-seconds and --max-errors must be positive")
    with Journal(args.db) as journal:
        failures = 0
        while True:
            try:
                # A fresh connection per poll permits recovery after read-only transport errors.
                async with source_factory() as source:
                    result = await scan(
                        source,
                        symbols,
                        config,
                        journal,
                        max_signal_age=timedelta(seconds=args.max_signal_age),
                    )
                failures = 0
                print(dumps(result), flush=True)
            except Exception as exc:
                failures += 1
                journal.record(
                    {"event": "feed_error", "count": failures, "error_type": type(exc).__name__}
                )
                if args.command != "watch" or failures >= args.max_errors:
                    raise
                print(
                    f"Market-data read failed ({failures}/{args.max_errors}); reconnecting",
                    file=sys.stderr,
                    flush=True,
                )
            if args.command != "watch":
                break
            await asyncio.sleep(args.poll_seconds)


def offline_command(args: argparse.Namespace, config: AppConfig) -> None:
    bars = read_csv(args.csv, default_minutes=args.bar_minutes)
    if config.symbols:
        bars = [bar for bar in bars if bar.symbol in config.symbols]
        if not bars:
            raise ValueError("No input bars match configured symbols")
    if args.command == "replay" and args.allow_shorts:
        config = replace(config, execution=replace(config.execution, allow_shorts=True))
    with Journal(args.db or ":memory:") as journal:
        journal.record({"event": "run_started", "mode": args.command, "config": config})
        if args.command == "replay":
            print(dumps(replay(bars, config, journal)))
            return
        if any(bar.duration != FIVE_MINUTES for bar in bars):
            raise ValueError("signals requires five-minute bars; use replay for minute data")
        engine = StrategyEngine(config.strategy, on_event=journal.record)
        for bar in bars:
            if signal := engine.on_bar(bar, now=bar.end):
                journal.save_signal(signal)
                print(dumps(signal))


def alpaca_command(args: argparse.Namespace, config: AppConfig) -> None:
    from sweepflow.alpaca import AlpacaPaperClient
    from sweepflow.alpaca_execution import AlpacaPaperBroker
    from sweepflow.paper import paper_account_lock, run_paper

    if not 10 <= args.eod_seconds <= 3600:
        raise ValueError("--eod-seconds must be between 10 and 3600")
    if args.allow_shorts:
        config = replace(config, execution=replace(config.execution, allow_shorts=True))
    if args.command == "paper":
        for name in ("poll_seconds", "scan_seconds", "max_signal_age"):
            if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
                raise ValueError(f"--{name.replace('_', '-')} must be finite and positive")
        if args.max_signal_age > 300 or args.max_errors < 1:
            raise ValueError("Signal age cannot exceed 300 seconds; max errors must be positive")
        if args.poll_seconds > args.eod_seconds / 2:
            raise ValueError("Reconciliation interval must be at most half the closing lead time")
    with AlpacaPaperClient.from_env(env_file=args.env_file) as client:
        account_id = str(client.get_account()["id"])
        with paper_account_lock(account_id), Journal(args.db) as journal:
            journal.bind_alpaca_account(account_id)
            broker = AlpacaPaperBroker(
                client,
                config,
                journal,
                eod_seconds=args.eod_seconds,
                max_signal_age_seconds=getattr(args, "max_signal_age", 90),
            )
            if args.command == "alpaca-sync":
                print(dumps(broker.sync()), flush=True)
                return
            # Synchronize first, even if downloading the universe later fails.
            print(dumps({"event": "alpaca_sync", **broker.sync()}), flush=True)
            print(dumps({"event": "paper_universe_started"}), flush=True)
            symbols = resolve_symbols(args, config)
            event = {"event": "paper_universe_ready", "symbols": len(symbols)}
            journal.record(event)
            print(dumps(event), flush=True)
            asyncio.run(
                run_paper(
                    broker,
                    symbols,
                    config,
                    journal,
                    once=args.once,
                    poll_seconds=args.poll_seconds,
                    scan_seconds=args.scan_seconds,
                    max_errors=args.max_errors,
                    max_signal_age=args.max_signal_age,
                )
            )


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "live":
            require_live_execution()
        elif args.command == "universe":
            snapshot = fetch_sp500_universe()
            save_universe(snapshot, args.output)
            print(dumps(snapshot.to_dict()))
        elif args.command == "audit":
            if not args.db.is_file():
                raise ValueError(f"Journal does not exist: {args.db}")
            with Journal(args.db) as journal:
                for event in journal.events():
                    print(dumps(event))
        elif args.command in {"paper", "alpaca-sync"}:
            alpaca_command(args, config)
        elif args.command in {"replay", "signals"}:
            offline_command(args, config)
        else:
            asyncio.run(network_command(args, config))
        return 0
    except KeyboardInterrupt:
        print("Stopped.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
