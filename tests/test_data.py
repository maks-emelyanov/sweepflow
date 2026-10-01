import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from tempfile import TemporaryDirectory

from sweepflow.data import FiveMinuteAggregator, bar_from_dict, bar_to_dict, read_csv, write_csv
from sweepflow.models import Bar

START = datetime(2026, 9, 22, 13, 30, tzinfo=UTC)


def minute(index, symbol="AAPL"):
    return Bar(
        symbol,
        START + timedelta(minutes=index),
        D("100"),
        D(101 + index),
        D("99"),
        D("100.5"),
        volume=index + 1,
        duration=timedelta(minutes=1),
    )


class DataTests(unittest.TestCase):
    def test_aggregates_complete_session_aligned_ohlcv(self):
        aggregator = FiveMinuteAggregator()
        for index in range(4):
            self.assertIsNone(aggregator.on_bar(minute(index)))
        result = aggregator.on_bar(minute(4))
        self.assertEqual(result.start, START)
        self.assertEqual(result.duration, timedelta(minutes=5))
        self.assertEqual(
            (result.open, result.high, result.low, result.close),
            (D("100"), D("105"), D("99"), D("100.5")),
        )
        self.assertEqual(result.volume, 15)

    def test_missing_minute_never_produces_partial_candle_and_next_bucket_recovers(self):
        aggregator = FiveMinuteAggregator()
        for index in (0, 1, 3, 4):
            self.assertIsNone(aggregator.on_bar(minute(index)))
        for index in range(5, 9):
            self.assertIsNone(aggregator.on_bar(minute(index)))
        result = aggregator.on_bar(minute(9))
        self.assertEqual(result.start, START + timedelta(minutes=5))
        self.assertEqual(result.volume, 40)

    def test_missing_final_minute_cannot_bleed_into_next_bucket(self):
        aggregator = FiveMinuteAggregator()
        for index in (0, 1, 2, 3, 5, 6, 7, 8):
            self.assertIsNone(aggregator.on_bar(minute(index)))
        self.assertEqual(aggregator.on_bar(minute(9)).start, START + timedelta(minutes=5))

    def test_symbols_are_independent_and_duplicate_minutes_rejected(self):
        aggregator = FiveMinuteAggregator()
        for index in range(4):
            aggregator.on_bar(minute(index, "AAPL"))
            aggregator.on_bar(minute(index, "MSFT"))
        self.assertEqual(aggregator.on_bar(minute(4, "AAPL")).symbol, "AAPL")
        self.assertEqual(aggregator.on_bar(minute(4, "MSFT")).symbol, "MSFT")
        with self.assertRaises(ValueError):
            aggregator.on_bar(minute(4))

    def test_outside_session_is_ignored_and_nonminute_data_rejected(self):
        aggregator = FiveMinuteAggregator()
        self.assertIsNone(aggregator.on_bar(replace(minute(0), start=START - timedelta(minutes=1))))
        with self.assertRaises(ValueError):
            aggregator.on_bar(replace(minute(0), duration=timedelta(minutes=5)))

    def test_csv_roundtrip_sorts_and_preserves_decimal_prices(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "prices.csv"
            bars = [minute(1), minute(0, "MSFT"), minute(0)]
            write_csv(path, bars)
            self.assertEqual(read_csv(path), [minute(0), minute(0, "MSFT"), minute(1)])
            write_csv(path, [minute(0), minute(0)])
            with self.assertRaisesRegex(ValueError, "duplicate"):
                read_csv(path)

    def test_csv_requires_explicit_duration_and_timezone(self):
        row = bar_to_dict(minute(0))
        del row["duration_seconds"]
        with self.assertRaisesRegex(ValueError, "duration_seconds"):
            bar_from_dict(row)
        self.assertEqual(bar_from_dict(row, default_minutes=1), minute(0))
        row["timestamp"] = "2026-09-22T13:30:00"
        with self.assertRaises(ValueError):
            bar_from_dict(row, default_minutes=1)


if __name__ == "__main__":
    unittest.main()
