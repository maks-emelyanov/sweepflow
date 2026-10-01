from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from sweepflow.config import StrategyConfig, load_config
from sweepflow.models import Bar
from sweepflow.sessions import SessionCalendar


@pytest.fixture(scope="module")
def calendar():
    return SessionCalendar()


def test_early_close_windows_follow_exchange_close(calendar):
    session = calendar.session(date(2025, 11, 28))
    assert session is not None
    assert session.close == datetime(2025, 11, 28, 18, tzinfo=UTC)
    at = datetime(2025, 11, 28, 17, 30, tzinfo=UTC)
    assert calendar.setup_window(at, StrategyConfig()) == (
        datetime(2025, 11, 28, 17, tzinfo=UTC),
        datetime(2025, 11, 28, 17, 55, tzinfo=UTC),
    )
    assert (
        calendar.setup_window(datetime(2025, 11, 28, 17, 55, tzinfo=UTC), StrategyConfig()) is None
    )


def test_holiday_weekend_and_dst(calendar):
    assert calendar.session(date(2025, 7, 4)) is None
    assert calendar.session(date(2025, 7, 5)) is None
    assert calendar.previous_session(date(2025, 7, 7)).label == date(2025, 7, 3)
    assert calendar.session(date(2025, 3, 7)).open.hour == 14
    assert calendar.session(date(2025, 3, 10)).open.hour == 13


def test_regular_bar_alignment_and_close(calendar):
    session = calendar.session(date(2025, 6, 3))
    assert session is not None

    def bar(start):
        return Bar("AAPL", start, Decimal(100), Decimal(100), Decimal(100), Decimal(100))

    assert calendar.is_regular_bar(bar(session.open))
    assert calendar.is_regular_bar(bar(session.close - timedelta(minutes=5)))
    assert not calendar.is_regular_bar(bar(session.open + timedelta(minutes=1)))
    assert not calendar.is_regular_bar(bar(session.close))
    with pytest.raises(ValueError, match="timezone"):
        calendar.session_for(datetime(2025, 6, 3))


def test_strict_config_parses_decimal_and_etfs(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text(
        'universe = "etfs"\nsymbols = ["spy", "SPY"]\n'
        "[strategy]\nminimum_rr = 3.1\n[risk]\nrisk_per_trade = 0.001\n"
        "[execution]\nallow_shorts = true\n"
    )
    loaded = load_config(config)
    assert loaded.strategy.minimum_rr == Decimal("3.1")
    assert loaded.risk.risk_per_trade == Decimal("0.001")
    assert loaded.symbols == ("SPY",)
    assert loaded.execution.allow_shorts
    config.write_text("[strategy]\nminumum_rr = 3.0\n")
    with pytest.raises(ValueError, match="Unknown"):
        load_config(config)


@pytest.mark.parametrize(
    "values",
    [
        {"pivot_right": 0},
        {"minimum_rr": "NaN"},
        {"tick_size": "0"},
        {"entry_mode": "market"},
        {"closing_start_minutes": 5, "closing_end_minutes": 10},
    ],
)
def test_config_rejects_invalid_parameters(values):
    with pytest.raises(ValueError):
        StrategyConfig(**values)
