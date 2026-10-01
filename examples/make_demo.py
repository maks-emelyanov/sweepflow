"""Generate synthetic candles for a deterministic smoke test, not performance evidence."""

from datetime import date, timedelta
from pathlib import Path

from sweepflow.data import write_csv
from sweepflow.models import Bar
from sweepflow.sessions import SessionCalendar


def demo_bars() -> list[Bar]:
    calendar = SessionCalendar()
    result = []
    for day in (date(2026, 9, 21), date(2026, 9, 22)):
        session = calendar.session(day)
        assert session is not None
        for bucket in range(78):
            if day.day == 21:
                ohlc = ("101", "102", "100.5", "101")
                if bucket == 0:
                    ohlc = ("101", "110", "100", "101")
                if bucket >= 73:
                    high = ("101.5", "101.8", "102", "101.8", "101.7")[bucket - 73]
                    ohlc = ("101", high, "100.5", "101")
            else:
                ohlc = {
                    0: ("101", "101.5", "99.5", "100.5"),  # PDL sweep
                    1: ("100.5", "103", "100.3", "102.5"),  # BOS closes above 102
                    2: ("102.5", "103.2", "102", "103"),  # FVG 101.5..102 confirmed
                    3: ("103", "103.2", "101.9", "102.2"),  # Later resting limit fill
                    4: ("102.2", "110", "102.2", "109"),  # Target after entry
                }.get(bucket, ("109", "109.2", "108.5", "109"))
            for minute in range(5):
                values = ohlc if minute == 0 else (ohlc[3],) * 4
                result.append(
                    Bar(
                        "SPY",
                        session.open + timedelta(minutes=bucket * 5 + minute),
                        *values,
                        volume=1000,
                        duration=timedelta(minutes=1),
                    )
                )
    return result


if __name__ == "__main__":
    target = Path("examples/demo_1m.csv")
    write_csv(target, demo_bars())
    print(f"Wrote {target}: synthetic data, not investment performance")
