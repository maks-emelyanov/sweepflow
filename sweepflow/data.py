"""Validated CSV interchange and session-aligned, complete five-minute candles."""

from __future__ import annotations

import csv
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sweepflow.models import Bar
from sweepflow.sessions import SessionCalendar

MINUTE = timedelta(minutes=1)
FIVE_MINUTES = timedelta(minutes=5)
COLUMNS = ("symbol", "timestamp", "open", "high", "low", "close", "volume", "duration_seconds")


def bar_to_dict(bar: Bar) -> dict:
    return {
        "symbol": bar.symbol,
        "timestamp": bar.start.astimezone(UTC).isoformat(),
        **{key: str(getattr(bar, key)) for key in ("open", "high", "low", "close")},
        "volume": bar.volume,
        "duration_seconds": int(bar.duration.total_seconds()),
    }


def bar_from_dict(row: dict, *, default_minutes: int | None = None) -> Bar:
    seconds = row.get("duration_seconds")
    if not seconds and default_minutes is None:
        raise ValueError("Bars must declare duration_seconds (60 for replay, 300 for signals)")
    return Bar(
        symbol=row["symbol"].strip().upper(),
        start=datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")),
        **{key: Decimal(str(row[key])) for key in ("open", "high", "low", "close")},
        volume=int(row.get("volume") or 0),
        duration=timedelta(seconds=int(seconds) if seconds else default_minutes * 60),
    )


def read_csv(path: str | Path, *, default_minutes: int | None = None) -> list[Bar]:
    bars: list[Bar] = []
    seen: set[tuple[str, datetime]] = set()
    with Path(path).open(newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"symbol", "timestamp", "open", "high", "low", "close"}
        if not required.issubset(reader.fieldnames or ()):
            raise ValueError(f"CSV requires columns: {', '.join(sorted(required))}")
        for line, row in enumerate(reader, 2):
            try:
                bar = bar_from_dict(row, default_minutes=default_minutes)
                key = (bar.symbol, bar.start)
                if key in seen:
                    raise ValueError("duplicate symbol/timestamp")
                seen.add(key)
                bars.append(bar)
            except (ValueError, KeyError, ArithmeticError) as exc:
                raise ValueError(f"{path}:{line}: {exc}") from exc
    if not bars:
        raise ValueError("CSV contains no bars")
    return sorted(bars, key=lambda bar: (bar.start, bar.symbol))


def write_csv(path: str | Path, bars: Iterable[Bar]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(bar_to_dict(bar) for bar in bars)


class FiveMinuteAggregator:
    """Emit a candle only after all five distinct, consecutive RTH minutes exist."""

    def __init__(self, calendar: SessionCalendar | None = None) -> None:
        self.calendar = calendar or SessionCalendar()
        self._pending: dict[str, tuple[datetime, list[Bar]]] = {}
        self._last: dict[str, datetime] = {}

    def on_bar(self, bar: Bar) -> Bar | None:
        if bar.duration != MINUTE:
            raise ValueError("Aggregation requires one-minute input bars")
        if not self.calendar.is_regular_bar(bar):
            return None
        if bar.start.second or bar.start.microsecond:
            raise ValueError("Minute bars must start on a minute boundary")
        previous = self._last.get(bar.symbol)
        if previous is not None and bar.start <= previous:
            raise ValueError("Minute bars must increase strictly per symbol")
        self._last[bar.symbol] = bar.start
        session = self.calendar.session_for(bar.start)
        if session is None:
            return None
        offset = int((bar.start - session.open).total_seconds()) // 300
        bucket = session.open + offset * FIVE_MINUTES
        active_bucket, pending = self._pending.get(bar.symbol, (bucket, []))
        if active_bucket != bucket:
            pending = []
        pending.append(bar)
        self._pending[bar.symbol] = (bucket, pending)
        if bar.end != bucket + FIVE_MINUTES:
            return None
        del self._pending[bar.symbol]
        if len(pending) != 5 or any(
            item.start != bucket + index * MINUTE for index, item in enumerate(pending)
        ):
            return None
        return Bar(
            symbol=bar.symbol,
            start=bucket,
            open=pending[0].open,
            high=max(item.high for item in pending),
            low=min(item.low for item in pending),
            close=pending[-1].close,
            volume=sum(item.volume for item in pending),
            duration=FIVE_MINUTES,
        )
