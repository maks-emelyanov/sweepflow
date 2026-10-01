"""One-minute execution replay through the same five-minute strategy engine."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from itertools import groupby

from sweepflow.config import AppConfig
from sweepflow.data import MINUTE, FiveMinuteAggregator
from sweepflow.execution import BrokerEvent, PaperBroker
from sweepflow.models import Bar, SetupState
from sweepflow.sessions import SessionCalendar
from sweepflow.storage import Journal
from sweepflow.strategy import StrategyEngine


@dataclass(frozen=True)
class ReplayResult:
    bars: int
    signals: int
    trades: int
    wins: int
    starting_equity: Decimal
    ending_equity: Decimal
    pnl: Decimal
    max_drawdown: Decimal


def validate_replay(bars: list[Bar], calendar: SessionCalendar) -> list[Bar]:
    """Never invent execution prices across missing minutes or missing sessions."""
    if not bars or any(bar.duration != MINUTE for bar in bars):
        raise ValueError(
            "Execution replay requires one-minute bars; five-minute bars are signal-only"
        )
    grouped: dict[tuple[str, date], list[Bar]] = defaultdict(list)
    regular = []
    for bar in bars:
        session = calendar.session_for(bar.start)
        if session is None:
            continue
        if not calendar.is_regular_bar(bar):
            raise ValueError(f"Misaligned minute bar: {bar.symbol} {bar.start}")
        grouped[bar.symbol, session.label].append(bar)
        regular.append(bar)
    if not regular:
        raise ValueError("No regular-session input bars")
    regular.sort(key=lambda bar: (bar.start, bar.symbol))
    days = sorted({day for _, day in grouped})
    symbols = sorted({symbol for symbol, _ in grouped})
    if len(days) < 2:
        raise ValueError(
            "Replay needs at least two complete sessions, including previous-day warmup"
        )
    for before, after in zip(days, days[1:], strict=False):
        if calendar.previous_session(after).label != before:
            raise ValueError(f"Missing exchange session between {before} and {after}")
    for symbol in symbols:
        for day in days:
            session = calendar.session(day)
            assert session is not None
            items = sorted(grouped.get((symbol, day), []), key=lambda bar: bar.start)
            count = int((session.close - session.open) / MINUTE)
            if len(items) != count or any(
                bar.start != session.open + index * MINUTE for index, bar in enumerate(items)
            ):
                raise ValueError(
                    f"{symbol} {day}: replay requires all {count} regular-session minutes "
                    "(missing, duplicate, or misaligned execution data)"
                )
    return regular


def replay(bars: list[Bar], config: AppConfig, journal: Journal | None = None) -> ReplayResult:
    calendar = SessionCalendar()
    bars = validate_replay(bars, calendar)
    record = journal.record if journal else lambda _: None
    engine = StrategyEngine(config.strategy, calendar=calendar, on_event=record)
    broker = PaperBroker(config.risk, config.execution, audit=record)
    aggregator = FiveMinuteAggregator(calendar)
    signal_count = 0
    trade_count = 0
    wins = 0
    current_day = None
    peak = broker.equity
    max_drawdown = Decimal(0)

    def order_events(events: list[BrokerEvent]) -> None:
        nonlocal trade_count, wins
        for event in events:
            if event.status in {"filled", "closed", "cancelled", "rejected"}:
                engine.on_order_event(event.symbol, event.status, at=event.at)
            if event.status == "closed":
                trade_count += 1
                wins += int(event.pnl > 0)

    for _, minute_group in groupby(bars, key=lambda bar: bar.start):
        batch = list(minute_group)
        session = calendar.session_for(batch[0].start)
        assert session is not None
        if session.label != current_day:
            broker.start_session(session.label)
            current_day = session.label
        # All existing orders execute before any signals from this minute's close.
        order_events(broker.on_bars(batch))
        completed = [result for bar in batch if (result := aggregator.on_bar(bar)) is not None]
        for bar in completed:
            signal = engine.on_bar(bar, now=bar.end)
            if signal is not None:
                signal_count += 1
                if journal:
                    journal.save_signal(signal)
                # Signal and sizing decision are journaled before the simulated order is accepted.
                record({"event": "order_intent", "signal": signal})
                event = broker.submit(signal, at=bar.end)
                order_events([event])
            state = engine.state_for(bar.symbol)
            if state.phase is SetupState.INVALIDATED and bar.symbol in broker.pending:
                order_events(broker.cancel(bar.symbol, at=bar.end, reason="strategy_invalidated"))
        if batch[0].end == session.close:
            order_events(
                broker.flatten_session(session.close, {bar.symbol: bar.close for bar in batch})
            )
        peak = max(peak, broker.equity)
        max_drawdown = max(max_drawdown, (peak - broker.equity) / peak)
    result = ReplayResult(
        bars=len(bars),
        signals=signal_count,
        trades=trade_count,
        wins=wins,
        starting_equity=config.risk.account_equity,
        ending_equity=broker.equity,
        pnl=broker.equity - config.risk.account_equity,
        max_drawdown=max_drawdown,
    )
    record({"event": "replay_complete", "result": result})
    return result
