import unittest
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

from sweepflow.config import AppConfig
from sweepflow.data import bar_to_dict
from sweepflow.models import Bar, Direction, Signal
from sweepflow.storage import Journal

AT = datetime(2026, 9, 22, 14, 0, tzinfo=UTC)
DAY = date(2026, 9, 22)


def candle():
    return Bar("AAPL", AT, D("100"), D("102"), D("99"), D("101"), 10)


def signal():
    return Signal(
        "unique",
        "AAPL",
        Direction.LONG,
        D("100"),
        D("99"),
        D("103"),
        D("99.5"),
        D("100"),
        AT,
        AT + timedelta(minutes=30),
    )


class StorageTests(unittest.TestCase):
    def test_config_binding_is_durable_and_rejects_changed_rules(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite"
            config = AppConfig()
            with Journal(path) as journal:
                journal.bind({"config": config, "symbols": ("AAPL",)})
            with Journal(path) as journal:
                journal.bind({"config": config, "symbols": ("AAPL",)})
                with self.assertRaisesRegex(ValueError, "different configuration"):
                    journal.bind({"config": config, "symbols": ("MSFT",)})

    def test_identical_overlap_is_idempotent_and_revision_is_durable_without_quarantine(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite"
            original = candle()
            revised = replace(original, high=D("103"))
            with Journal(path) as journal:
                self.assertEqual(journal.store_bars([original], DAY), set())
                self.assertEqual(journal.store_bars([original], DAY), set())
                self.assertEqual(journal.store_bars([revised], DAY), {"AAPL"})
                self.assertEqual(journal.store_bars([revised], DAY), set())
                self.assertEqual(
                    journal.events(),
                    [
                        {
                            "event": "data_revision",
                            "symbol": "AAPL",
                            "session": DAY.isoformat(),
                            "start": AT.isoformat(),
                            "before": bar_to_dict(original),
                            "after": bar_to_dict(revised),
                            "changed_fields": ["high"],
                        }
                    ],
                )
            with Journal(path) as journal:
                self.assertEqual(journal.bars_since(AT), [revised])
                self.assertEqual(journal.quarantined(DAY), set())
                self.assertEqual(journal.quarantined(DAY + timedelta(days=1)), set())
                self.assertEqual(len(journal.events()), 1)

    def test_revisions_are_audited_per_candle_with_all_changed_fields(self):
        originals = [candle(), replace(candle(), start=AT + timedelta(minutes=5))]
        revisions = [
            replace(originals[0], high=D("103"), volume=20),
            replace(originals[1], close=D("100")),
        ]
        with Journal(":memory:") as journal:
            journal.store_bars(originals, DAY)
            self.assertEqual(journal.store_bars(revisions, DAY), {"AAPL"})
            events = journal.events()
            self.assertEqual(len(events), 2)
            self.assertEqual(
                {event["start"] for event in events}, {bar.start.isoformat() for bar in revisions}
            )
            self.assertEqual(set(events[0]["changed_fields"]), {"high", "volume"})
            self.assertEqual(events[1]["changed_fields"], ["close"])
            self.assertEqual(journal.quarantined(DAY), set())

    def test_failed_batch_rolls_back_both_revision_and_its_audit(self):
        original = candle()
        with Journal(":memory:") as journal:
            journal.store_bars([original], DAY)
            with self.assertRaises(AttributeError):
                journal.store_bars([replace(original, high=D("103")), object()], DAY)
            self.assertEqual(journal.bars_since(), [original])
            self.assertEqual(journal.events(), [])
            self.assertEqual(journal.quarantined(DAY), set())

    def test_equivalent_timezone_representation_is_not_revision(self):
        original = candle()
        equivalent = replace(original, start=AT.astimezone(ZoneInfo("America/New_York")))
        with Journal(":memory:") as journal:
            journal.store_bars([original], DAY)
            self.assertEqual(journal.store_bars([equivalent], DAY), set())
            self.assertEqual(journal.quarantined(DAY), set())
            self.assertEqual(journal.bars_since(AT), [original])

    def test_equivalent_decimal_representation_is_not_revision(self):
        original = candle()
        equivalent = replace(original, high=D("102.0000"))
        with Journal(":memory:") as journal:
            journal.store_bars([original], DAY)
            self.assertEqual(journal.store_bars([equivalent], DAY), set())
            self.assertEqual(journal.events(), [])

    def test_signals_remain_unique_after_reopening_journal(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite"
            with Journal(path) as journal:
                self.assertTrue(journal.save_signal(signal()))
                self.assertFalse(journal.save_signal(signal()))
                journal.record({"event": "price", "price": D("100.01"), "at": AT})
            with Journal(path) as journal:
                self.assertFalse(journal.save_signal(signal()))
                self.assertEqual(journal.events()[0]["price"], "100.01")

    def test_store_batch_rolls_back_on_unserializable_input(self):
        with Journal(":memory:") as journal:
            with self.assertRaises(AttributeError):
                journal.store_bars([candle(), object()], DAY)
            self.assertEqual(journal.bars_since(AT), [])

    def test_unchanged_batch_does_not_write_or_audit_rows(self):
        with Journal(":memory:") as journal:
            original = candle()
            journal.store_bars([original], DAY)
            changes = journal.connection.total_changes
            generation = journal.bar_generation
            equivalent = replace(original, high=D("102.0000"))
            assert journal.store_bars([equivalent], DAY) == set()
            assert journal.connection.total_changes == changes
            assert journal.bar_generation == generation
            assert journal.bars_since() == [original]

    def test_minute_repair_provenance_survives_restart_and_native_refresh(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "repaired.sqlite"
            original = candle()
            key = (original.symbol, original.start)
            with Journal(path) as journal:
                journal.store_bars([original], DAY, repaired_keys={key})
                assert journal.repaired_bars_since(AT) == {key}
            with Journal(path) as journal:
                assert journal.repaired_bars_since() == {key}
                revised = replace(original, high=D("103"))
                assert journal.store_bars([revised], DAY, repaired_keys={key}) == {"AAPL"}
                assert journal.events()[0]["changed_fields"] == ["high"]
                # Native candles supersede minute provenance, even at the same OHLC.
                journal.store_bars([revised], DAY)
                assert journal.repaired_bars_since() == set()
                assert len(journal.events()) == 1

    def test_failed_batch_rolls_back_minute_provenance_and_cache(self):
        original = candle()
        key = (original.symbol, original.start)
        with Journal(":memory:") as journal:
            with self.assertRaises(AttributeError):
                journal.store_bars([original, object()], DAY, repaired_keys={key})
            assert journal.repaired_bars_since() == set()
            assert journal.bars_since() == []


if __name__ == "__main__":
    unittest.main()
