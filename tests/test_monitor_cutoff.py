"""Daily history latency must not leave an outdated intraday cutoff."""

import unittest
from datetime import date, timedelta
from unittest.mock import patch

from test_monitor import FakeSource, scenario

from sweepflow.monitor import scan
from sweepflow.sessions import SessionCalendar
from sweepflow.storage import Journal
from sweepflow.strategy import StrategyEngine


class AdvancingSource(FakeSource):
    def __init__(self, bars, *, started_at, daily_finished_at, intraday_finished_at=None):
        super().__init__(bars)
        self.now = started_at
        self.daily_finished_at = daily_finished_at
        self.intraday_finished_at = intraday_finished_at

    async def get_daily_bars(self, symbols, previous, *, now):
        result = await super().get_daily_bars(symbols, previous, now=now)
        self.now = self.daily_finished_at
        return result

    async def get_bars(self, symbols, start, end, *, now):
        result = await super().get_bars(symbols, start, end, now=now)
        if self.intraday_finished_at is not None:
            self.now = self.intraday_finished_at
        return result


class MonitorCutoffTests(unittest.IsolatedAsyncioTestCase):
    async def test_daily_fetch_crossing_candle_boundary_refreshes_cutoff_and_emits_signal(self):
        bars, config, session = scenario()
        started_at = session.open + timedelta(minutes=29, seconds=59)
        daily_finished_at = session.open + timedelta(minutes=30, seconds=30)
        source = AdvancingSource(bars, started_at=started_at, daily_finished_at=daily_finished_at)
        consumed_at = []
        on_bar = StrategyEngine.on_bar

        def record_cutoff(engine, bar, *, now):
            consumed_at.append(now)
            return on_bar(engine, bar, now=now)

        with Journal(":memory:") as journal, patch.object(StrategyEngine, "on_bar", record_cutoff):
            result = await scan(source, ["AAPL"], config, journal, clock=lambda: source.now)
            self.assertEqual(result.status, "ok")
            self.assertEqual(len(result.new_signals), 1)
            self.assertEqual(result.new_signals[0].created_at, bars[-1].end)
            self.assertEqual(result.eligible_signals, result.new_signals)
        previous = SessionCalendar().previous_session(session.label)
        self.assertEqual(source.daily_calls, [(("AAPL",), previous, started_at)])
        self.assertEqual(
            source.calls, [(("AAPL",), previous.open, daily_finished_at, daily_finished_at)]
        )
        self.assertEqual(consumed_at, [daily_finished_at] * len(bars))

    async def test_freshness_still_counts_intraday_latency_after_daily_cutoff_refresh(self):
        for confirmation_age, expected_signals in ((90, 1), (91, 0)):
            with self.subTest(confirmation_age=confirmation_age):
                bars, config, session = scenario()
                daily_finished_at = session.open + timedelta(minutes=30, seconds=30)
                source = AdvancingSource(
                    bars,
                    started_at=session.open + timedelta(minutes=29, seconds=59),
                    daily_finished_at=daily_finished_at,
                    intraday_finished_at=bars[-1].end + timedelta(seconds=confirmation_age),
                )
                with Journal(":memory:") as journal:
                    result = await scan(
                        source, ["AAPL"], config, journal, clock=lambda source=source: source.now
                    )
                    self.assertEqual(len(result.new_signals), expected_signals)
                    self.assertEqual(len(result.eligible_signals), expected_signals)
                    if not expected_signals:
                        self.assertTrue(
                            any(
                                event.get("reason") == "stale_confirmation"
                                for event in journal.events()
                            )
                        )
                self.assertEqual(source.calls[0][2:], (daily_finished_at, daily_finished_at))

    async def test_daily_fetch_crossing_regular_or_early_close_stops_before_intraday(self):
        for label in (date(2026, 9, 22), date(2026, 11, 27)):
            session = SessionCalendar().session(label)
            self.assertIsNotNone(session)
            for after_close in (timedelta(0), timedelta(seconds=1)):
                with self.subTest(label=label, after_close=after_close):
                    _, config, _ = scenario()
                    source = AdvancingSource(
                        [],
                        started_at=session.close - timedelta(seconds=15),
                        daily_finished_at=session.close + after_close,
                    )
                    with (
                        Journal(":memory:") as journal,
                        patch.object(StrategyEngine, "on_bar") as on_bar,
                    ):
                        result = await scan(
                            source,
                            ["AAPL"],
                            config,
                            journal,
                            mode="alpaca-paper",
                            clock=lambda source=source: source.now,
                        )
                        self.assertEqual(result.status, "market_closed")
                        self.assertEqual(result.new_signals, ())
                        self.assertEqual(result.eligible_signals, ())
                        self.assertEqual(result.active_signal_ids, ())
                        self.assertEqual(result.bars_fetched, 0)
                        self.assertEqual(source.calls, [])
                        self.assertEqual(len(source.daily_calls), 1)
                        on_bar.assert_not_called()


if __name__ == "__main__":
    unittest.main()
