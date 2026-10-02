"""Restartable, read-only monitoring of completed Robinhood five-minute candles."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sweepflow.config import AppConfig
from sweepflow.models import Bar, Signal
from sweepflow.sessions import NEW_YORK, Session, SessionCalendar
from sweepflow.storage import Journal
from sweepflow.strategy import StrategyEngine

if TYPE_CHECKING:
    from sweepflow.robinhood import RobinhoodDataSource


@dataclass(frozen=True)
class ScanResult:
    status: str
    symbols: int
    bars_fetched: int
    new_signals: tuple[Signal, ...]
    quarantined: tuple[str, ...] = ()  # Legacy output field; revisions no longer quarantine.
    # Fresh candidates remain available after a crash between journaling and submission.
    eligible_signals: tuple[Signal, ...] = ()
    # Existing entry orders are allowed to wait beyond the freshness budget.
    active_signal_ids: tuple[str, ...] = ()
    stale_symbols: tuple[str, ...] = ()
    revised_symbols: tuple[str, ...] = ()
    repaired_bars: int = 0
    daily_bars_fetched: int = 0
    missing_daily_symbols: tuple[str, ...] = ()


@dataclass(frozen=True)
class PreparationResult:
    status: str
    symbols: int
    bars_fetched: int
    missing_symbols: tuple[str, ...] = ()
    quarantined: tuple[str, ...] = ()
    revised_symbols: tuple[str, ...] = ()
    repaired_bars: int = 0
    daily_bars_fetched: int = 0
    missing_daily_symbols: tuple[str, ...] = ()


@dataclass
class _ScanCache:
    signature: tuple
    generation: int
    requested_at: datetime
    engine: StrategyEngine
    bars: dict[tuple[str, datetime], Bar]
    histories: dict[str, dict[datetime, Bar]]
    daily_bars: dict[tuple[str, datetime], Bar]
    candidates: dict[str, dict[str, Signal]] = field(default_factory=dict)


async def _read_bars(source, symbols, start, end, *, now, known, repaired):
    repair = getattr(source, "get_bars_with_repair", None)
    if repair is not None:
        return await repair(symbols, start, end, now=now, known=known, repaired=repaired)
    return await source.get_bars(symbols, start, end, now=now)


async def _read_daily_bars(source, symbols, session: Session, *, now) -> dict[str, Bar]:
    """Require exact completed-session daily inputs, with no intraday fallback."""
    daily = await source.get_daily_bars(symbols, session, now=now)
    if not isinstance(daily, dict):
        raise ValueError("Daily history must return a symbol-to-bar mapping")
    for symbol, bar in daily.items():
        if (
            symbol not in symbols
            or not isinstance(bar, Bar)
            or bar.symbol != symbol
            or bar.start != session.open
            or bar.end != session.close
            or bar.end > now
        ):
            raise ValueError("Daily bar must match the requested completed regular session")
    return daily


async def prepare_session(
    source: RobinhoodDataSource,
    symbols: Sequence[str],
    config: AppConfig,
    journal: Journal,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PreparationResult:
    """Cache completed previous-session history before open, without signals.

    Reading history also checks the data connection and required capabilities.
    Keep older pivot history and correct cached candles before the coming session.
    Live scans refresh both sessions to detect revisions and repair remaining gaps.
    """
    symbols = tuple(sorted(set(symbols)))
    if not symbols:
        raise ValueError("At least one symbol is required")
    bind_monitor(journal, config, symbols, "alpaca-paper")
    calendar = SessionCalendar()
    now = clock()
    session = calendar.session(now.astimezone(NEW_YORK).date())
    if session is None or now >= session.open:
        return PreparationResult("not_preopen", len(symbols), 0)
    previous = calendar.previous_session(session.label)
    existing = journal.bars_since(previous.open)
    daily = await _read_daily_bars(source, symbols, previous, now=now)
    cached = {
        (bar.symbol, bar.start)
        for bar in existing
        if bar.symbol in symbols and bar.end <= previous.close
    }
    expected = []
    cursor = previous.open
    while cursor + timedelta(minutes=5) <= previous.close:
        expected.append(cursor)
        cursor += timedelta(minutes=5)
    # Refresh intraday history for causal pivot context independently of levels.
    result = await _read_bars(
        source,
        symbols,
        previous.open,
        previous.close,
        now=now,
        known=existing,
        repaired=journal.repaired_bars_since(previous.open),
    )
    bars = sorted(
        (
            bar
            for values in result.values()
            for bar in values
            if bar.symbol in symbols
            and previous.open <= bar.start
            and bar.end <= previous.close
            and bar.duration == timedelta(minutes=5)
            and calendar.is_regular_bar(bar)
        ),
        key=lambda bar: (bar.start, bar.symbol),
    )
    revised = journal.store_bars(
        bars, session.label, repaired_keys=getattr(source, "last_repaired_keys", set())
    )
    revised.update(journal.store_daily_bars(list(daily.values()), session.label))
    cached.update((bar.symbol, bar.start) for bar in bars)
    missing = tuple(
        symbol for symbol in symbols if any((symbol, at) not in cached for at in expected)
    )
    daily_symbols = {
        bar.symbol
        for bar in journal.daily_bars_since(previous.open)
        if bar.start == previous.open and bar.end == previous.close
    }
    missing_daily = tuple(symbol for symbol in symbols if symbol not in daily_symbols)
    return PreparationResult(
        "incomplete" if missing or missing_daily else "ready",
        len(symbols),
        len(bars),
        missing,
        revised_symbols=tuple(sorted(revised)),
        repaired_bars=getattr(source, "last_repaired_bars", 0),
        daily_bars_fetched=len(daily),
        missing_daily_symbols=missing_daily,
    )


def bind_monitor(journal: Journal, config: AppConfig, symbols: Sequence[str], mode: str) -> None:
    if mode == "alpaca-paper":
        # Constituents and the short-entry toggle can change without discarding
        # durable broker ownership. Existing exits are always reconciled.
        journal.bind(
            {
                "mode": mode,
                "config": replace(
                    config,
                    symbols=(),
                    universe="sp500",
                    execution=replace(config.execution, allow_shorts=False),
                ),
            }
        )
    else:
        journal.bind({"mode": mode, "config": config, "symbols": tuple(sorted(set(symbols)))})


async def scan(
    source: RobinhoodDataSource,
    symbols: Sequence[str],
    config: AppConfig,
    journal: Journal,
    *,
    max_signal_age: timedelta = timedelta(seconds=90),
    mode: str = "shadow",
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ScanResult:
    """Apply durable inputs; emit each fresh signal ID at most once.

    Catch-up bars reconstruct state but old confirmations never become fresh
    entries. Revisions/backfills rebuild affected symbols from their complete
    history; unchanged symbols consume only appended bars between scans.
    No trade/preview/cancel MCP calls are made by this service.
    """
    if max_signal_age <= timedelta(0):
        raise ValueError("max_signal_age must be positive")
    symbols = tuple(sorted(set(symbols)))
    if not symbols:
        raise ValueError("At least one symbol is required")
    if mode not in {"shadow", "alpaca-paper"}:
        raise ValueError("Unknown monitoring mode")
    bind_monitor(journal, config, symbols, mode)
    calendar = SessionCalendar()
    requested_at = clock()
    session = calendar.session(requested_at.astimezone(NEW_YORK).date())
    if session is None or not session.open <= requested_at < session.close:
        return ScanResult("market_closed", len(symbols), 0, ())
    previous = calendar.previous_session(session.label)
    persisted = [bar for bar in journal.bars_since() if bar.symbol in symbols]
    persisted_daily = [bar for bar in journal.daily_bars_since() if bar.symbol in symbols]
    signature = (mode, config.strategy, symbols, "daily")
    cached, journal._scan_cache = journal._scan_cache, None
    if cached is not None and (
        cached.signature != signature
        or cached.generation != journal.bar_generation
        or requested_at < cached.requested_at
    ):
        cached = None
    prior_bars = (
        cached.bars if cached is not None else {(bar.symbol, bar.start): bar for bar in persisted}
    )
    known = set(prior_bars)
    prior_daily = {(bar.symbol, bar.start): bar for bar in persisted_daily}
    daily = await _read_daily_bars(source, symbols, previous, now=requested_at)
    # Daily reads for a full universe can cross a candle boundary. Include bars
    # completed during that fetch rather than delaying them until the next scan.
    requested_at = clock()
    if not session.open <= requested_at < session.close:
        return ScanResult("market_closed", len(symbols), 0, ())
    # A daily correction changes levels; intraday corrections change pivots and
    # setups. Refresh both inputs each poll instead of only overlapping the tail.
    result = await _read_bars(
        source,
        symbols,
        previous.open,
        requested_at,
        now=requested_at,
        known=persisted,
        repaired=journal.repaired_bars_since(previous.open),
    )
    fresh_bars = sorted(
        (bar for values in result.values() for bar in values),
        key=lambda bar: (bar.start, bar.symbol),
    )
    revised = journal.store_bars(
        fresh_bars, session.label, repaired_keys=getattr(source, "last_repaired_keys", set())
    )
    revised.update(journal.store_daily_bars(list(daily.values()), session.label))
    # Reads yield to other journal writers. Use committed inputs after fetching
    # and storing so their corrections affect this poll, including cached data
    # omitted by a successful upstream response. Concurrent appends beyond this
    # poll's cutoff remain in the journal for a later scan to consume.
    inputs = {
        (bar.symbol, bar.start): bar
        for bar in journal.bars_since()
        if bar.symbol in symbols and bar.end <= requested_at
    }
    daily_inputs = {
        (bar.symbol, bar.start): bar for bar in journal.daily_bars_since() if bar.symbol in symbols
    }
    changed = {key for key, bar in inputs.items() if key in prior_bars and prior_bars[key] != bar}
    revised.update(symbol for symbol, _ in changed)
    revised.update(
        symbol
        for (symbol, at), bar in daily_inputs.items()
        if (symbol, at) in prior_daily and prior_daily[(symbol, at)] != bar
    )
    daily_changed = {
        symbol
        for (symbol, at), bar in daily_inputs.items()
        if cached is not None and cached.daily_bars.get((symbol, at)) != bar
    }
    # Older confirmed pivots remain valid until superseded, so preserve all known
    # structure context across restarts, not just the daily-level warmup session.
    record_enabled = False

    def audit(event: dict) -> None:
        if record_enabled:
            journal.record(event)

    if cached is None:
        engine = StrategyEngine(
            config.strategy,
            calendar=calendar,
            on_event=audit,
            daily_bars=list(daily_inputs.values()),
        )
        histories: dict[str, dict[datetime, Bar]] = {}
        for bar in inputs.values():
            histories.setdefault(bar.symbol, {})[bar.start] = bar
        candidates_by_symbol: dict[str, dict[str, Signal]] = {}
        replay = list(inputs.values())
    else:
        engine = cached.engine
        engine.on_event = audit
        engine.set_daily_bars(list(daily_inputs.values()))
        histories = cached.histories
        candidates_by_symbol = cached.candidates
        rebuild = set(revised) | daily_changed
        appended = []
        for bar in inputs.values():
            key = (bar.symbol, bar.start)
            if prior_bars.get(key) == bar:
                continue
            histories.setdefault(bar.symbol, {})[bar.start] = bar
            state = engine.states.get(bar.symbol)
            if (
                state is not None
                and state.last_bar is not None
                and bar.start <= state.last_bar.start
            ):
                rebuild.add(bar.symbol)
            else:
                appended.append(bar)
        replay = [bar for bar in appended if bar.symbol not in rebuild]
        for symbol in rebuild:
            engine.states.pop(symbol, None)
            candidates_by_symbol.pop(symbol, None)
            replay.extend(histories.get(symbol, {}).values())
    for index, bar in enumerate(sorted(replay, key=lambda bar: (bar.start, bar.symbol))):
        if index % 200 == 0:
            await asyncio.sleep(0)  # Let paper reconciliation run during long reconstructions.
        record_enabled = (
            (bar.symbol, bar.start) not in known
            or (bar.symbol, bar.start) in changed
            or (bar.symbol in daily_changed and bar.start >= session.open)
        )
        signal = engine.on_bar(bar, now=requested_at)
        if signal is not None and signal.created_at >= session.open:
            candidates_by_symbol.setdefault(signal.symbol, {})[signal.id] = signal
    fetched_at = clock()  # Include reconstruction latency in the freshness budget.
    # A trailing missing candle has no later candle to reveal a gap in the engine.
    # Allow the same bounded publication grace used for signal confirmations.
    stale = {
        symbol
        for symbol in symbols
        if mode == "alpaca-paper"
        and (
            engine.state_for(symbol).last_bar is None
            or fetched_at - engine.state_for(symbol).last_bar.end
            > timedelta(minutes=5) + max_signal_age
        )
    }
    emitted = []
    eligible = []
    active = tuple(
        sorted(
            state.signal.id
            for symbol, state in engine.states.items()
            if state.signal is not None
            and state.phase.value == "ENTRY_PENDING"
            and symbol not in stale
            and fetched_at < state.signal.expires_at
        )
    )
    candidates = [
        signal
        for items in candidates_by_symbol.values()
        for signal in items.values()
        if signal.created_at >= session.open
    ]
    for signal in sorted(candidates, key=lambda signal: (signal.created_at, signal.symbol)):
        confirmation_key = (signal.symbol, signal.created_at - timedelta(minutes=5))
        reason = None
        if signal.symbol in stale:
            reason = "stale_market_data"
        elif fetched_at >= signal.expires_at:
            reason = "setup_expired"
        elif fetched_at - signal.created_at > max_signal_age:
            reason = "stale_confirmation"
        elif fetched_at < signal.created_at:
            reason = "future_confirmation"
        if reason:
            if (
                confirmation_key not in known
                or signal.symbol in revised
                or signal.symbol in daily_changed
            ):
                journal.record(
                    {"event": "shadow_rejected", "signal_id": signal.id, "reason": reason}
                )
            continue
        state = engine.state_for(signal.symbol)
        if (
            state.signal is None
            or state.signal.id != signal.id
            or state.phase.value != "ENTRY_PENDING"
        ):
            continue
        eligible.append(signal)
        if journal.save_signal(signal):
            journal.record({"event": f"{mode}_signal", "signal": signal})
            emitted.append(signal)
    # Failed/canceled scans never publish a partially advanced engine cache.
    journal._scan_cache = _ScanCache(
        signature,
        journal.bar_generation,
        requested_at,
        engine,
        inputs,
        histories,
        daily_inputs,
        {
            symbol: {key: item for key, item in items.items() if item.created_at >= session.open}
            for symbol, items in candidates_by_symbol.items()
        },
    )
    return ScanResult(
        "ok",
        len(symbols),
        len(fresh_bars),
        tuple(emitted),
        (),
        tuple(eligible),
        active,
        tuple(sorted(stale)),
        tuple(sorted(revised)),
        getattr(source, "last_repaired_bars", 0),
        len(daily),
        tuple(symbol for symbol in symbols if (symbol, previous.open) not in daily_inputs),
    )
