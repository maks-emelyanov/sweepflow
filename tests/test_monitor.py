import unittest
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal as D
from pathlib import Path
from tempfile import TemporaryDirectory

from sweepflow.config import AppConfig, StrategyConfig
from sweepflow.models import Bar
from sweepflow.monitor import scan
from sweepflow.sessions import SessionCalendar
from sweepflow.storage import Journal


def daily_history(bars):
    """Create independent daily fixtures once, before intraday history changes."""
    calendar = SessionCalendar()
    grouped = {}
    for bar in bars:
        session = calendar.session_for(bar.start)
        if session is not None:
            grouped.setdefault((bar.symbol, session.label), []).append(bar)
    result = []
    for (symbol, label), history in sorted(grouped.items()):
        session = calendar.session(label)
        history.sort(key=lambda bar: bar.start)
        result.append(
            Bar(
                symbol,
                session.open,
                history[0].open,
                max(bar.high for bar in history),
                min(bar.low for bar in history),
                history[-1].close,
                sum(bar.volume for bar in history),
                duration=session.close - session.open,
            )
        )
    return result


class FakeSource:
    def __init__(self, bars, *, daily_bars=None):
        self.bars = bars
        self.calls = []
        self.daily_bars = daily_history(bars) if daily_bars is None else list(daily_bars)
        self.daily_calls = []

    async def get_daily_bars(self, symbols, previous, *, now):
        self.daily_calls.append((tuple(symbols), previous, now))
        return {
            symbol: bar
            for bar in self.daily_bars
            for symbol in symbols
            if bar.symbol == symbol and bar.start.date() == previous.label
        }

    async def get_bars(self, symbols, start, end, *, now):
        self.calls.append((tuple(symbols), start, end, now))
        return {
            symbol: [
                bar
                for bar in self.bars
                if bar.symbol == symbol and start <= bar.start and bar.end <= min(end, now)
            ]
            for symbol in symbols
        }


def scenario():
    calendar = SessionCalendar()
    prior = calendar.session(date(2026, 9, 21))
    current = calendar.session(date(2026, 9, 22))
    history = [
        Bar(
            "AAPL",
            prior.open + timedelta(minutes=5 * index),
            D("100"),
            D("110") if index == 0 else D("101"),
            D("98") if index == 0 else D("99"),
            D("100"),
            100,
        )
        for index in range(78)
    ]
    prices = [
        ("100", "101", "99", "100"),
        ("100", "101.5", "99.5", "100"),
        ("100", "101", "99.5", "100"),
        ("99.5", "100", "97.9", "99.5"),
        ("99.5", "102.5", "99.5", "102"),
        ("102", "103", "100.5", "102.5"),
    ]
    history.extend(
        Bar(
            "AAPL",
            current.open + timedelta(minutes=index * 5),
            *(D(value) for value in values),
            volume=100,
        )
        for index, values in enumerate(prices)
    )
    config = AppConfig(strategy=StrategyConfig(pivot_left=1, pivot_right=1))
    return history, config, current


class MonitorTests(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_signal_full_warmup_and_restart_deduplication(self):
        bars, config, session = scenario()
        source = FakeSource(bars)
        now = session.open + timedelta(minutes=30, seconds=30)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "shadow.sqlite"
            with Journal(path) as journal:
                result = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
                self.assertEqual(result.status, "ok")
                self.assertEqual(len(result.new_signals), 1)
                self.assertEqual(result.new_signals[0].entry, D("100.5"))
                self.assertEqual(result.new_signals[0].target, D("110"))
                count = len(journal.events())
            with Journal(path) as journal:
                repeated = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
                self.assertEqual(repeated.new_signals, ())
                self.assertEqual(len(journal.events()), count)
        self.assertEqual(source.calls[1][1], bars[0].start)

    async def test_stale_catchup_confirmation_is_not_emitted(self):
        bars, config, session = scenario()
        now = session.open + timedelta(minutes=32)
        with Journal(":memory:") as journal:
            result = await scan(FakeSource(bars), ["AAPL"], config, journal, clock=lambda: now)
            self.assertEqual(result.new_signals, ())
            rejected = [
                event for event in journal.events() if event.get("event") == "shadow_rejected"
            ]
            self.assertEqual(rejected[-1]["reason"], "stale_confirmation")

    async def test_fetch_latency_counts_toward_signal_age(self):
        bars, config, session = scenario()
        ticks = iter(
            [
                session.open + timedelta(minutes=30, seconds=30),
                session.open + timedelta(minutes=30, seconds=30),
                session.open + timedelta(minutes=32),
            ]
        )
        with Journal(":memory:") as journal:
            result = await scan(
                FakeSource(bars), ["AAPL"], config, journal, clock=lambda: next(ticks)
            )
            self.assertEqual(result.new_signals, ())
            self.assertTrue(
                any(event.get("reason") == "stale_confirmation" for event in journal.events())
            )

    async def test_market_closed_does_not_fetch(self):
        bars, config, session = scenario()
        source = FakeSource(bars)
        with Journal(":memory:") as journal:
            result = await scan(source, ["AAPL"], config, journal, clock=lambda: session.close)
            self.assertEqual(result.status, "market_closed")
            self.assertEqual(source.calls, [])
            self.assertEqual(source.daily_calls, [])

    async def test_missing_current_candle_prevents_signal(self):
        bars, config, session = scenario()
        del bars[80]  # Missing 09:40 candle before sweep.
        now = session.open + timedelta(minutes=30, seconds=30)
        with Journal(":memory:") as journal:
            result = await scan(FakeSource(bars), ["AAPL"], config, journal, clock=lambda: now)
            self.assertEqual(result.new_signals, ())
            self.assertTrue(
                any(event.get("reason") == "missing_candle" for event in journal.events())
            )

    async def test_later_poll_repairs_internal_gap_before_fresh_confirmation(self):
        bars, config, session = scenario()
        incomplete = [bar for index, bar in enumerate(bars) if index != 80]
        source = FakeSource(incomplete)
        with Journal(":memory:") as journal:
            first = await scan(
                source,
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open + timedelta(minutes=25, seconds=30),
            )
            self.assertEqual(first.new_signals, ())
            source.bars = bars
            repaired = await scan(
                source,
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open + timedelta(minutes=30, seconds=30),
            )
            self.assertEqual(len(repaired.new_signals), 1)
            self.assertEqual(repaired.quarantined, ())
            self.assertEqual(source.calls[-1][1], bars[0].start)

    async def test_incomplete_previous_session_allows_fresh_signal_across_restart(self):
        bars, config, session = scenario()
        del bars[30]
        source = FakeSource(bars)
        now = session.open + timedelta(minutes=30, seconds=30)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "shadow.sqlite"
            with Journal(path) as journal:
                result = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
                self.assertEqual(len(result.new_signals), 1)
                signal = result.new_signals[0]
                self.assertEqual(signal.target, D("110"))
                self.assertEqual(signal.metadata["previous_session_level_source"], "daily")
                self.assertIs(signal.metadata["previous_session_complete"], False)
                self.assertEqual(signal.metadata["previous_session_bars"], 77)
                self.assertEqual(signal.metadata["previous_session_expected_bars"], 78)
                self.assertEqual(result.quarantined, ())
                count = len(journal.events())
                cached = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
                self.assertEqual(cached.new_signals, ())
                self.assertEqual(cached.eligible_signals, (signal,))
                self.assertEqual(cached.active_signal_ids, (signal.id,))
                self.assertEqual(len(journal.events()), count)
            with Journal(path) as journal:
                restarted = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
                self.assertEqual(restarted.new_signals, ())
                self.assertEqual(restarted.eligible_signals, (signal,))
                self.assertEqual(restarted.active_signal_ids, (signal.id,))
                self.assertEqual(len(journal.events()), count)

    async def test_missing_daily_bar_cannot_use_complete_intraday_levels(self):
        bars, config, session = scenario()
        older = SessionCalendar().previous_session(bars[0].start.date())
        older_history = [
            replace(bar, start=older.open + timedelta(minutes=5 * index))
            for index, bar in enumerate(bars[:78])
        ]
        with Journal(":memory:") as journal:
            journal.store_bars(older_history, older.label)
            result = await scan(
                FakeSource(bars, daily_bars=[]),
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open + timedelta(minutes=30, seconds=30),
            )
            self.assertEqual(result.new_signals, ())
            self.assertEqual(result.eligible_signals, ())
            self.assertEqual(result.missing_daily_symbols, ("AAPL",))
            self.assertEqual(result.daily_bars_fetched, 0)

    async def test_previous_intraday_backfill_keeps_authoritative_daily_target(self):
        for backfill_delay, expected_signals in ((45, 1), (120, 0)):
            with self.subTest(backfill_delay=backfill_delay):
                bars, config, session = scenario()
                daily_bars = daily_history(bars)
                bars[30] = replace(bars[30], high=D("112"))
                source = FakeSource(
                    [bar for index, bar in enumerate(bars) if index != 30], daily_bars=daily_bars
                )
                first_at = session.open + timedelta(minutes=30, seconds=30)
                backfilled_at = session.open + timedelta(minutes=30, seconds=backfill_delay)
                with Journal(":memory:") as journal:
                    first = await scan(
                        source,
                        ["AAPL"],
                        config,
                        journal,
                        clock=lambda now=first_at: now,
                    )
                    original = first.new_signals[0]
                    self.assertEqual(original.target, D("110"))
                    self.assertIs(original.metadata["previous_session_complete"], False)
                    source.bars = bars
                    backfilled = await scan(
                        source,
                        ["AAPL"],
                        config,
                        journal,
                        clock=lambda now=backfilled_at: now,
                    )
                    self.assertEqual(backfilled.new_signals, ())
                    self.assertEqual(len(backfilled.eligible_signals), expected_signals)
                    self.assertEqual(backfilled.quarantined, ())
                    self.assertEqual(backfilled.active_signal_ids, (original.id,))
                    if expected_signals:
                        corrected = backfilled.eligible_signals[0]
                        self.assertEqual(corrected.id, original.id)
                        self.assertEqual(corrected.target, D("110"))
                        self.assertEqual(corrected.created_at, original.created_at)
                        self.assertIs(corrected.metadata["previous_session_complete"], True)
                        self.assertEqual(corrected.metadata["previous_session_bars"], 78)
                        repeated = await scan(
                            source,
                            ["AAPL"],
                            config,
                            journal,
                            clock=lambda now=backfilled_at: now,
                        )
                        self.assertEqual(repeated.new_signals, ())
                        self.assertEqual(repeated.eligible_signals, (corrected,))
                    self.assertEqual(
                        journal.connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0],
                        1,
                    )

    async def test_daily_bar_extrema_are_authoritative_with_complete_or_incomplete_intraday(self):
        for incomplete in (False, True):
            with self.subTest(incomplete=incomplete):
                bars, config, session = scenario()
                previous = SessionCalendar().previous_session(session.label)
                daily_bars = [
                    replace(bar, high=D("112"), low=D("98.1")) for bar in daily_history(bars[:78])
                ]
                history = bars[1:] if incomplete else bars
                source = FakeSource(history, daily_bars=daily_bars)
                now = session.open + timedelta(minutes=30, seconds=30)
                with Journal(":memory:") as journal:
                    result = await scan(
                        source, ["AAPL"], config, journal, clock=lambda now=now: now
                    )
                    signal = result.new_signals[0]
                    self.assertEqual(signal.target, D("112"))
                    self.assertEqual(signal.metadata["pdl"], "98.1")
                    self.assertEqual(signal.metadata["previous_session_level_source"], "daily")
                    self.assertIs(signal.metadata["previous_session_complete"], not incomplete)
                    self.assertEqual(result.daily_bars_fetched, 1)
                    self.assertEqual(result.missing_daily_symbols, ())
                    self.assertEqual(source.daily_calls, [(("AAPL",), previous, now)])

    async def test_daily_bar_allows_current_pivots_without_previous_intraday_history(self):
        bars, config, session = scenario()
        source = FakeSource(bars[78:], daily_bars=daily_history(bars[:78]))
        now = session.open + timedelta(minutes=30, seconds=30)
        with Journal(":memory:") as journal:
            result = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            signal = result.new_signals[0]
            self.assertEqual(signal.target, D("110"))
            self.assertEqual(signal.metadata["structure_level"], "101.5")
            self.assertIs(signal.metadata["previous_session_complete"], False)
            self.assertEqual(signal.metadata["previous_session_bars"], 0)
            self.assertEqual(signal.metadata["previous_session_level_source"], "daily")

    async def test_stale_or_forming_daily_bar_fails_closed(self):
        bars, config, session = scenario()
        calendar = SessionCalendar()
        previous = calendar.previous_session(session.label)
        older = calendar.previous_session(previous.label)
        daily = daily_history(bars[:78])[0]
        for invalid_daily in (
            replace(daily, start=older.open),
            replace(daily, start=session.open),
            replace(daily, duration=timedelta(minutes=5)),
        ):
            with self.subTest(daily_start=invalid_daily.start, duration=invalid_daily.duration):

                class InvalidSource(FakeSource):
                    async def get_daily_bars(self, symbols, previous, *, now):
                        return {"AAPL": self.daily_bars[0]}

                with Journal(":memory:") as journal:
                    with self.assertRaises(ValueError):
                        await scan(
                            InvalidSource(bars, daily_bars=[invalid_daily]),
                            ["AAPL"],
                            config,
                            journal,
                            clock=lambda: session.open + timedelta(minutes=30, seconds=30),
                        )
                    self.assertEqual(
                        journal.connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0],
                        0,
                    )

    async def test_daily_only_revision_rebuilds_target_with_original_freshness_across_restart(self):
        for restart in (False, True):
            for revision_delay, expected_signals in ((45, 1), (120, 0)):
                with self.subTest(restart=restart, revision_delay=revision_delay):
                    bars, config, session = scenario()
                    source = FakeSource(bars)
                    first_at = session.open + timedelta(minutes=30, seconds=30)
                    revised_at = session.open + timedelta(minutes=30, seconds=revision_delay)
                    with TemporaryDirectory() as directory:
                        path = Path(directory) / "shadow.sqlite"
                        journal = Journal(path)
                        try:
                            first = await scan(
                                source,
                                ["AAPL"],
                                config,
                                journal,
                                clock=lambda now=first_at: now,
                            )
                            original = first.new_signals[0]
                            source.daily_bars = [
                                replace(bar, high=D("112")) if bar.start < session.open else bar
                                for bar in source.daily_bars
                            ]
                            if restart:
                                journal.close()
                                journal = Journal(path)
                            revised = await scan(
                                source,
                                ["AAPL"],
                                config,
                                journal,
                                clock=lambda now=revised_at: now,
                            )
                            self.assertEqual(source.bars, bars)
                            self.assertEqual(revised.revised_symbols, ("AAPL",))
                            self.assertEqual(len(revised.new_signals), expected_signals)
                            self.assertEqual(len(revised.eligible_signals), expected_signals)
                            self.assertNotIn(original.id, revised.active_signal_ids)
                            if expected_signals:
                                corrected = revised.new_signals[0]
                                self.assertEqual(corrected.target, D("112"))
                                self.assertEqual(corrected.created_at, original.created_at)
                                self.assertNotEqual(corrected.id, original.id)
                            repeated = await scan(
                                source,
                                ["AAPL"],
                                config,
                                journal,
                                clock=lambda now=revised_at: now,
                            )
                            self.assertEqual(repeated.new_signals, ())
                            self.assertEqual(repeated.eligible_signals, revised.new_signals)
                        finally:
                            journal.close()

    async def test_backfilled_daily_levels_keep_original_confirmation_freshness(self):
        for backfill_delay, expected_signals in ((45, 1), (120, 0)):
            with self.subTest(backfill_delay=backfill_delay):
                bars, config, session = scenario()
                source = FakeSource(bars, daily_bars=[])
                first_at = session.open + timedelta(minutes=30, seconds=30)
                backfilled_at = session.open + timedelta(minutes=30, seconds=backfill_delay)
                with Journal(":memory:") as journal:
                    first = await scan(
                        source,
                        ["AAPL"],
                        config,
                        journal,
                        clock=lambda now=first_at: now,
                    )
                    self.assertEqual(first.new_signals, ())
                    self.assertEqual(first.missing_daily_symbols, ("AAPL",))
                    source.daily_bars = daily_history(bars[:78])
                    backfilled = await scan(
                        source,
                        ["AAPL"],
                        config,
                        journal,
                        clock=lambda now=backfilled_at: now,
                    )
                    self.assertEqual(backfilled.missing_daily_symbols, ())
                    self.assertEqual(len(backfilled.new_signals), expected_signals)
                    self.assertEqual(len(backfilled.eligible_signals), expected_signals)
                    if expected_signals:
                        self.assertEqual(backfilled.new_signals[0].created_at, bars[-1].end)
                    else:
                        self.assertTrue(
                            any(
                                event.get("reason") == "stale_confirmation"
                                for event in journal.events()
                            )
                        )
                    self.assertEqual(
                        journal.connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0],
                        expected_signals,
                    )

    async def test_daily_backfill_with_no_intraday_history_keeps_scan_available(self):
        bars, config, session = scenario()
        source = FakeSource([], daily_bars=[])
        now = session.open + timedelta(minutes=30, seconds=30)
        with Journal(":memory:") as journal:
            first = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            self.assertEqual(first.missing_daily_symbols, ("AAPL",))
            source.daily_bars = daily_history(bars[:78])
            backfilled = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            self.assertEqual(backfilled.status, "ok")
            self.assertEqual(backfilled.bars_fetched, 0)
            self.assertEqual(backfilled.daily_bars_fetched, 1)
            self.assertEqual(backfilled.missing_daily_symbols, ())
            self.assertEqual(backfilled.new_signals, ())
            self.assertEqual(backfilled.eligible_signals, ())
            self.assertEqual(journal.daily_bars_since(), source.daily_bars)

    async def test_daily_only_revision_invalidates_and_restores_pending_signal(self):
        bars, config, session = scenario()
        source = FakeSource(bars)
        original_daily = source.daily_bars
        now = session.open + timedelta(minutes=30, seconds=30)
        with Journal(":memory:") as journal:
            first = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            source.daily_bars = [replace(bar, high=D("101")) for bar in original_daily[:1]]
            invalidated = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            self.assertEqual(source.bars, bars)
            self.assertEqual(invalidated.revised_symbols, ("AAPL",))
            self.assertEqual(invalidated.new_signals, ())
            self.assertEqual(invalidated.eligible_signals, ())
            self.assertEqual(invalidated.active_signal_ids, ())
            source.daily_bars = original_daily
            restored = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            self.assertEqual(restored.revised_symbols, ("AAPL",))
            self.assertEqual(restored.new_signals, ())
            self.assertEqual(restored.eligible_signals, first.new_signals)
            self.assertEqual(restored.active_signal_ids, first.active_signal_ids)

    async def test_revised_observed_bar_allows_new_signal_across_restart(self):
        bars, config, session = scenario()
        before_confirmation = session.open + timedelta(minutes=25, seconds=30)
        after_confirmation = session.open + timedelta(minutes=30, seconds=30)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "shadow.sqlite"
            with Journal(path) as journal:
                first = await scan(
                    FakeSource(bars), ["AAPL"], config, journal, clock=lambda: before_confirmation
                )
                self.assertEqual(first.new_signals, ())
            bars[-2] = replace(bars[-2], high=D("102.6"))
            with Journal(path) as journal:
                result = await scan(
                    FakeSource(bars), ["AAPL"], config, journal, clock=lambda: after_confirmation
                )
                self.assertEqual(len(result.new_signals), 1)
                self.assertEqual(result.quarantined, ())
                self.assertEqual(result.revised_symbols, ("AAPL",))
                self.assertEqual(journal.quarantined(session.label), set())
                self.assertFalse(
                    any(event.get("reason") == "revised_market_data" for event in journal.events())
                )
            with Journal(path) as journal:
                repeated = await scan(
                    FakeSource(bars), ["AAPL"], config, journal, clock=lambda: after_confirmation
                )
                self.assertEqual(repeated.quarantined, ())
                self.assertEqual(repeated.revised_symbols, ())
                self.assertEqual(repeated.new_signals, ())
                self.assertEqual(repeated.eligible_signals, result.new_signals)

    async def test_legacy_quarantine_does_not_block_reconstructed_signal_after_restart(self):
        bars, config, session = scenario()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "shadow.sqlite"
            with Journal(path) as journal:
                journal.store_bars(bars[:-1], session.label)
                with journal.connection:
                    journal.connection.execute(
                        "INSERT INTO quarantined VALUES (?,?)",
                        ("AAPL", session.label.isoformat()),
                    )
            with Journal(path) as journal:
                result = await scan(
                    FakeSource(bars),
                    ["AAPL"],
                    config,
                    journal,
                    clock=lambda: session.open + timedelta(minutes=30, seconds=30),
                )
                self.assertEqual(len(result.new_signals), 1)
                self.assertEqual(result.quarantined, ())
                self.assertEqual(result.revised_symbols, ())
                self.assertEqual(result.active_signal_ids, (result.new_signals[0].id,))
                # Historical evidence is preserved even though it no longer gates entries.
                self.assertEqual(journal.quarantined(session.label), {"AAPL"})

    async def test_earlier_sweep_revision_removes_and_restores_pending_signal(self):
        bars, config, session = scenario()
        source = FakeSource(bars)
        now = session.open + timedelta(minutes=30, seconds=30)
        with Journal(":memory:") as journal:
            first = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            original = first.new_signals[0]
            source.bars = list(bars)
            source.bars[81] = replace(bars[81], low=D("98"))
            removed = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            self.assertEqual(removed.revised_symbols, ("AAPL",))
            self.assertEqual(removed.new_signals, ())
            self.assertEqual(removed.eligible_signals, ())
            self.assertEqual(removed.active_signal_ids, ())
            self.assertEqual(removed.quarantined, ())
            self.assertEqual(source.calls[-1][1], bars[0].start)
            source.bars = bars
            restored = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            self.assertEqual(restored.revised_symbols, ("AAPL",))
            self.assertEqual(restored.new_signals, ())
            self.assertEqual(restored.eligible_signals, (original,))
            self.assertEqual(restored.active_signal_ids, (original.id,))

    async def test_price_revisions_recompute_daily_target_stop_and_entry(self):
        for index, changes, expected in (
            (0, {"high": D("112")}, {"target": D("112")}),
            (81, {"low": D("97.8")}, {"stop": D("97.79")}),
            (83, {"low": D("100.6")}, {"entry": D("100.6")}),
        ):
            with self.subTest(changes=changes):
                bars, config, session = scenario()
                source = FakeSource(bars)
                now = session.open + timedelta(minutes=30, seconds=30)
                with Journal(":memory:") as journal:
                    first = await scan(source, ["AAPL"], config, journal, clock=lambda now=now: now)
                    original = first.new_signals[0]
                    source.bars = list(bars)
                    source.bars[index] = replace(bars[index], **changes)
                    if index == 0:
                        source.daily_bars = [
                            replace(bar, **changes) if bar.start < session.open else bar
                            for bar in source.daily_bars
                        ]
                    result = await scan(
                        source, ["AAPL"], config, journal, clock=lambda now=now: now
                    )
                    self.assertEqual(result.revised_symbols, ("AAPL",))
                    self.assertEqual(result.quarantined, ())
                    self.assertEqual(len(result.new_signals), 1)
                    corrected = result.new_signals[0]
                    self.assertNotEqual(corrected.id, original.id)
                    self.assertEqual(corrected.created_at, original.created_at)
                    for field, value in expected.items():
                        self.assertEqual(getattr(corrected, field), value)
                    self.assertEqual(result.eligible_signals, (corrected,))
                    self.assertEqual(result.active_signal_ids, (corrected.id,))
                    self.assertNotIn(original.id, result.active_signal_ids)

    async def test_volume_revision_keeps_same_pending_signal_without_duplicate_emission(self):
        bars, config, session = scenario()
        source = FakeSource(bars)
        now = session.open + timedelta(minutes=30, seconds=30)
        with Journal(":memory:") as journal:
            first = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            source.bars = list(bars)
            source.bars[0] = replace(bars[0], volume=101)
            result = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            self.assertEqual(result.revised_symbols, ("AAPL",))
            self.assertEqual(result.new_signals, ())
            self.assertEqual(result.eligible_signals, first.new_signals)
            self.assertEqual(result.active_signal_ids, (first.new_signals[0].id,))
            self.assertEqual(result.quarantined, ())
            self.assertEqual(
                len([event for event in journal.events() if event.get("event") == "shadow_signal"]),
                1,
            )

    async def test_correction_can_create_signal_only_with_original_confirmation_freshness(self):
        for correction_delay, expected_signals in ((45, 1), (120, 0)):
            with self.subTest(correction_delay=correction_delay):
                bars, config, session = scenario()
                invalid_fvg = list(bars)
                invalid_fvg[-1] = replace(bars[-1], low=D("100"))
                source = FakeSource(invalid_fvg)
                first_at = session.open + timedelta(minutes=30, seconds=30)
                corrected_at = session.open + timedelta(minutes=30, seconds=correction_delay)
                with Journal(":memory:") as journal:
                    first = await scan(
                        source,
                        ["AAPL"],
                        config,
                        journal,
                        clock=lambda now=first_at: now,
                    )
                    self.assertEqual(first.new_signals, ())
                    source.bars = bars
                    corrected = await scan(
                        source,
                        ["AAPL"],
                        config,
                        journal,
                        clock=lambda now=corrected_at: now,
                    )
                    self.assertEqual(corrected.revised_symbols, ("AAPL",))
                    self.assertEqual(len(corrected.new_signals), expected_signals)
                    self.assertEqual(len(corrected.eligible_signals), expected_signals)
                    self.assertEqual(corrected.quarantined, ())
                    if expected_signals:
                        self.assertEqual(corrected.new_signals[0].created_at, bars[-1].end)
                    self.assertEqual(
                        journal.connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0],
                        expected_signals,
                    )

    async def test_feed_error_does_not_journal_unvalidated_bars(self):
        class FailingSource(FakeSource):
            async def get_bars(self, *args, **kwargs):
                raise RuntimeError("offline")

        bars, config, session = scenario()
        with Journal(":memory:") as journal:
            with self.assertRaisesRegex(RuntimeError, "offline"):
                await scan(
                    FailingSource(bars),
                    ["AAPL"],
                    config,
                    journal,
                    clock=lambda: session.open + timedelta(minutes=30),
                )
            self.assertEqual(journal.bars_since(session.open - timedelta(days=1)), [])


if __name__ == "__main__":
    unittest.main()
