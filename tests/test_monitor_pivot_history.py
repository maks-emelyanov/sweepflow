"""Regression for retaining known structure across daily shadow rebuilds."""

import asyncio
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

from test_monitor import FakeSource, daily_history

from sweepflow.config import AppConfig
from sweepflow.models import Bar
from sweepflow.monitor import scan
from sweepflow.sessions import SessionCalendar
from sweepflow.storage import Journal
from sweepflow.strategy import StrategyEngine


def test_shadow_preserves_confirmed_pivot_older_than_previous_session():
    calendar = SessionCalendar()
    first, previous, current = [
        calendar.session(day) for day in (date(2025, 6, 2), date(2025, 6, 3), date(2025, 6, 4))
    ]

    def candle(start, *, high="99.8", low="99.2", open_="99.5", close="99.5"):
        return Bar("AAPL", start, Decimal(open_), Decimal(high), Decimal(low), Decimal(close))

    history = [candle(first.open + timedelta(minutes=5 * index)) for index in range(78)]
    history[3] = replace(history[3], high=Decimal("110"))
    history[5] = replace(history[5], low=Decimal("99"))
    history[73] = replace(history[73], high=Decimal("100"))
    # Increasing highs create no confirmed swing high during the previous day.
    history.extend(
        candle(
            previous.open + timedelta(minutes=5 * index),
            high=str(Decimal("102.30") + Decimal(index) / 10),
        )
        for index in range(78)
    )
    history.extend(
        [
            candle(current.open, high="99.7", low="98.8", close="99.4"),
            candle(
                current.open + timedelta(minutes=5),
                open_="99.4",
                high="100.8",
                low="99.3",
                close="100.6",
            ),
            candle(
                current.open + timedelta(minutes=10),
                open_="100.6",
                high="101",
                low="100.2",
                close="100.7",
            ),
        ]
    )
    engine = StrategyEngine(calendar=calendar, daily_bars=daily_history(history))
    expected = [signal for item in history if (signal := engine.on_bar(item)) is not None]
    assert len(expected) == 1

    with Journal(":memory:") as journal:
        journal.store_bars(history, current.label)
        journal.store_daily_bars(daily_history(history[:-3]), current.label)
        result = asyncio.run(
            scan(
                FakeSource(history),
                ["AAPL"],
                AppConfig(),
                journal,
                clock=lambda: current.open + timedelta(minutes=15, seconds=30),
            )
        )
    assert [signal.id for signal in result.new_signals] == [expected[0].id]
