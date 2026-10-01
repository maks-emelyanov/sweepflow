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


class FakeSource:
    def __init__(self, bars):
        self.bars = bars
        self.calls = []

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
            [session.open + timedelta(minutes=30, seconds=30), session.open + timedelta(minutes=32)]
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

    async def test_incomplete_previous_session_cannot_supply_daily_levels(self):
        bars, config, session = scenario()
        del bars[30]
        with Journal(":memory:") as journal:
            result = await scan(
                FakeSource(bars),
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open + timedelta(minutes=30, seconds=30),
            )
            self.assertEqual(result.new_signals, ())
            self.assertTrue(
                any(
                    event.get("reason") == "previous_session_incomplete"
                    for event in journal.events()
                )
            )

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
        class FailingSource:
            async def get_bars(self, *args, **kwargs):
                raise RuntimeError("offline")

        _, config, session = scenario()
        with Journal(":memory:") as journal:
            with self.assertRaisesRegex(RuntimeError, "offline"):
                await scan(
                    FailingSource(),
                    ["AAPL"],
                    config,
                    journal,
                    clock=lambda: session.open + timedelta(minutes=30),
                )
            self.assertEqual(journal.bars_since(session.open - timedelta(days=1)), [])


if __name__ == "__main__":
    unittest.main()
