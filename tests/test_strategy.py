from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

import pytest

from sweepflow.config import StrategyConfig
from sweepflow.models import Bar, Direction, SetupState
from sweepflow.sessions import SessionCalendar
from sweepflow.strategy import StrategyEngine

D = Decimal
TODAY = date(2025, 6, 3)
PRIOR = date(2025, 6, 2)


@pytest.fixture(scope="module")
def calendar():
    return SessionCalendar()


def make_bar(start, o="99.5", h="99.8", lo="99.2", c="99.5", symbol="AAPL"):
    return Bar(symbol, start, D(o), D(h), D(lo), D(c))


def history(calendar, symbol="AAPL"):
    session = calendar.session(PRIOR)
    bars = [make_bar(session.open + timedelta(minutes=5 * i), symbol=symbol) for i in range(78)]
    bars[3] = replace(bars[3], high=D("110"))
    bars[5] = replace(bars[5], low=D("99"))
    # Last confirmed swing: two strictly lower left highs and two <= right highs.
    bars[73] = replace(bars[73], high=D("100"))
    return bars


def daily_bar(calendar, *, day=PRIOR, high="110", low="99"):
    session = calendar.session(day)
    return replace(make_bar(session.open, h=high, lo=low), duration=session.close - session.open)


def warmed(calendar, config=None, bars=None, *, daily_bars=None):
    events = []
    engine = StrategyEngine(
        config=config, calendar=calendar, on_event=events.append, daily_bars=daily_bars
    )
    for item in bars if bars is not None else history(calendar):
        assert engine.on_bar(item) is None
    return engine, events


def setup_bars(calendar):
    start = calendar.session(TODAY).open
    return [
        make_bar(start, o="99.5", h="99.7", lo="98.8", c="99.4"),
        make_bar(start + timedelta(minutes=5), o="99.4", h="100.8", lo="99.3", c="100.6"),
        make_bar(start + timedelta(minutes=10), o="100.6", h="101", lo="100.2", c="100.7"),
    ]


def feed(engine, bars):
    results = [engine.on_bar(item) for item in bars]
    return [result for result in results if result is not None]


def mirror(bar):
    return replace(
        bar,
        open=D(200) - bar.open,
        high=D(200) - bar.low,
        low=D(200) - bar.high,
        close=D(200) - bar.close,
    )


@pytest.mark.parametrize("short", [False, True])
def test_complete_long_and_short_flow_and_audit(calendar, short):
    prior = history(calendar)
    current = setup_bars(calendar)
    if short:
        prior, current = [mirror(item) for item in prior], [mirror(item) for item in current]
    engine, events = warmed(calendar, bars=prior)
    (signal,) = feed(engine, current)
    assert signal.direction is (Direction.SHORT if short else Direction.LONG)
    # Project renames must preserve IDs already stored in journals and broker intents.
    assert signal.id == (
        "d1f7d4d0-36ee-591b-8dd8-1e9491640c46" if short else "c8bfe0f5-c8c7-5417-8cf9-9027931b5114"
    )
    assert signal.entry == D("99.8" if short else "100.2")
    assert signal.stop == D("101.21" if short else "98.79")
    assert signal.target == D("90" if short else "110")
    assert signal.metadata["previous_session_complete"] is True
    assert signal.metadata["previous_session_bars"] == 78
    assert signal.metadata["previous_session_expected_bars"] == 78
    assert signal.reward_risk >= D("2.5")
    assert signal.created_at == current[-1].end
    assert signal.expires_at == calendar.session(TODAY).open + timedelta(minutes=60)
    assert engine.state_for("AAPL").phase is SetupState.ENTRY_PENDING
    assert [event["kind"] for event in events][-3:] == ["sweep", "bos", "signal"]
    other, _ = warmed(calendar, bars=prior)
    (repeated,) = feed(other, current)
    assert repeated.id == signal.id


@pytest.mark.parametrize(
    "mode,expected", [("first_touch", "100.2"), ("midpoint", "99.95"), ("deep_edge", "99.7")]
)
def test_entry_modes(calendar, mode, expected):
    engine, _ = warmed(calendar, StrategyConfig(entry_mode=mode))
    (signal,) = feed(engine, setup_bars(calendar))
    assert signal.entry == D(expected)


def test_stop_uses_whole_sweep_to_confirmation_range(calendar):
    engine, _ = warmed(calendar)
    current = setup_bars(calendar)
    current[1] = replace(current[1], low=D("98.5"))
    (signal,) = feed(engine, current)
    assert signal.stop == D("98.49")


def test_pivot_confirmed_by_sweep_itself_cannot_replace_prior_structure(calendar):
    engine, _ = warmed(calendar)
    start = calendar.session(TODAY).open
    bars = [
        make_bar(start, h="99.9"),
        make_bar(start + timedelta(minutes=5), h="100.1"),
        make_bar(start + timedelta(minutes=10), h="99.8"),
        make_bar(start + timedelta(minutes=15), h="99.8", lo="98.8"),
    ]
    feed(engine, bars)
    state = engine.state_for("AAPL")
    assert state.setup.structure_level == D("100")
    assert state.swing_high == D("100.1")


def test_swing_left_strict_right_nonstrict(calendar):
    prior = history(calendar)
    prior[74] = replace(prior[74], high=D("100"))
    prior[75] = replace(prior[75], high=D("100"))
    engine, _ = warmed(calendar, bars=prior)
    engine.on_bar(setup_bars(calendar)[0])
    assert engine.state_for("AAPL").setup.structure_level == D("100")
    prior[72] = replace(prior[72], high=D("100"))
    engine, _ = warmed(calendar, bars=prior)
    # The first equal high (index 72) may itself qualify; index 73 must not.
    # A flat history has no pivots despite equal right-hand highs.
    state = engine.state_for("AAPL")
    state.history = [replace(item, high=D("100")) for item in state.history]
    state.swing_high = None
    engine.on_bar(setup_bars(calendar)[0])
    assert state.phase is SetupState.INVALIDATED
    assert state.setup is None


def test_sweep_candle_cannot_also_be_bos(calendar):
    engine, _ = warmed(calendar)
    current = setup_bars(calendar)
    sweep = replace(current[0], high=D("100.5"), close=D("100.2"))
    assert engine.on_bar(sweep) is None
    assert engine.state_for("AAPL").phase is SetupState.LIQUIDITY_SWEPT


def test_bos_requires_close_not_wick_and_must_arrive_by_third_bar(calendar):
    engine, events = warmed(calendar)
    sweep = setup_bars(calendar)[0]
    engine.on_bar(sweep)
    for number in range(1, 4):
        engine.on_bar(make_bar(sweep.start + timedelta(minutes=number * 5), h="101", c="100"))
    assert engine.state_for("AAPL").phase is SetupState.INVALIDATED
    assert events[-1]["reason"] == "bos_timed_out"


@pytest.mark.parametrize("delay", [1, 2, 3])
def test_bos_allowed_on_each_of_next_three_bars(calendar, delay):
    engine, _ = warmed(calendar)
    current = setup_bars(calendar)
    engine.on_bar(current[0])
    for number in range(1, delay):
        engine.on_bar(make_bar(current[0].start + timedelta(minutes=number * 5)))
    bos = replace(current[1], start=current[0].start + timedelta(minutes=delay * 5))
    c = replace(current[2], start=bos.end)
    engine.on_bar(bos)
    assert engine.on_bar(c) is not None


def test_only_immediately_next_candle_may_confirm_fvg(calendar):
    engine, events = warmed(calendar)
    current = setup_bars(calendar)
    current[2] = replace(current[2], low=D("99.7"))
    assert not feed(engine, current)
    assert events[-1]["reason"] == "bos_did_not_create_fvg"
    assert engine.on_bar(replace(setup_bars(calendar)[2], start=current[2].end)) is None


def test_rr_filter_rejects_before_signal_and_audits_ratio(calendar):
    engine, events = warmed(calendar, StrategyConfig(minimum_rr=D("10")))
    assert not feed(engine, setup_bars(calendar))
    assert events[-1]["reason"] == "insufficient_reward_risk"
    assert D(events[-1]["rr"]) < D("10")


def test_dual_sweep_ignored(calendar):
    engine, events = warmed(calendar)
    sweep = replace(setup_bars(calendar)[0], high=D("111"))
    assert engine.on_bar(sweep) is None
    assert engine.state_for("AAPL").phase is SetupState.WAITING
    assert events[-1]["reason"] == "ambiguous_dual_sweep"


def test_target_reached_before_fvg_invalidates(calendar):
    engine, events = warmed(calendar)
    current = setup_bars(calendar)
    current[1] = replace(current[1], high=D("110"))
    assert not feed(engine, current)
    assert events[-1]["reason"] == "target_touched_before_entry"


def test_fvg_cannot_confirm_at_window_end(calendar):
    engine, events = warmed(calendar)
    start = calendar.session(TODAY).open
    # Sweep/BOS at 10:15/10:20; C ends exactly at 10:30.
    feed(engine, [make_bar(start + timedelta(minutes=5 * i)) for i in range(9)])
    current = [
        replace(item, start=item.start + timedelta(minutes=45)) for item in setup_bars(calendar)
    ]
    assert not feed(engine, current)
    assert events[-1]["reason"] == "window_ended"


def test_missing_bar_halts_session(calendar):
    engine, events = warmed(calendar)
    current = setup_bars(calendar)
    engine.on_bar(current[0])
    assert engine.on_bar(current[2]) is None
    assert engine.state_for("AAPL").halted
    assert events[-1]["reason"] == "missing_candle"


@pytest.mark.parametrize("missing", [0, 30])
@pytest.mark.parametrize("short", [False, True])
def test_daily_bar_supplies_levels_with_partial_prior_session_and_tail_pivots(
    calendar, missing, short
):
    prior = history(calendar)
    current = setup_bars(calendar)
    daily = daily_bar(calendar)
    if short:
        prior, current = [mirror(item) for item in prior], [mirror(item) for item in current]
        daily = mirror(daily)
    del prior[missing]
    engine, events = warmed(calendar, bars=prior, daily_bars=[daily])
    assert engine.state_for("AAPL").halted
    (signal,) = feed(engine, current)
    state = engine.state_for("AAPL")
    assert not state.halted
    assert state.previous_high == max(bar.high for bar in prior)
    assert state.previous_low == min(bar.low for bar in prior)
    assert signal.target == D("90" if short else "110")
    assert signal.metadata["structure_level"] == "100"
    assert signal.metadata["previous_session_level_source"] == "daily"
    assert signal.metadata["previous_session_complete"] is False
    assert signal.metadata["previous_session_bars"] == 77
    assert signal.metadata["previous_session_expected_bars"] == 78
    quality = [event for event in events if event.get("kind") == "data_quality"]
    assert quality[-1]["reason"] == "previous_session_incomplete"
    assert quality[-1]["previous_session_bars"] == 77


@pytest.mark.parametrize("missing", [72, 74, 75, 77])
def test_prior_gaps_cannot_supply_pivots_across_missing_candles(calendar, missing):
    prior = history(calendar)
    del prior[missing]
    engine, events = warmed(calendar, bars=prior, daily_bars=[daily_bar(calendar)])
    assert not feed(engine, setup_bars(calendar))
    state = engine.state_for("AAPL")
    assert state.previous_high == D("110")
    assert state.previous_low == D("99")
    assert state.previous_session_complete is False
    assert not state.halted
    assert any(event.get("reason") == "no_pre_sweep_confirmed_pivot" for event in events)


def test_missing_prior_close_allows_setup_after_current_session_pivot(calendar):
    engine, _ = warmed(calendar, bars=history(calendar)[:-1], daily_bars=[daily_bar(calendar)])
    start = calendar.session(TODAY).open
    # Establish structure with consecutive current-session candles before sweeping.
    prefix = [make_bar(start + timedelta(minutes=5 * i)) for i in range(5)]
    prefix[2] = replace(prefix[2], high=D("100"))
    assert not feed(engine, prefix)
    current = [
        replace(bar, start=bar.start + timedelta(minutes=25)) for bar in setup_bars(calendar)
    ]
    (signal,) = feed(engine, current)
    assert signal.target == D("110")
    assert signal.metadata["previous_session_complete"] is False
    assert signal.metadata["structure_level"] == "100"


def test_missing_current_open_clears_prior_pivot_context_and_halts_session(calendar):
    engine, events = warmed(calendar)
    assert engine.state_for("AAPL").swing_high == D("100")
    current = setup_bars(calendar)
    assert engine.on_bar(current[1]) is None
    state = engine.state_for("AAPL")
    assert state.halted
    assert state.swing_high is None
    assert state.swing_low is None
    assert state.history == [current[1]]
    assert engine.on_bar(current[2]) is None
    assert any(event.get("reason") == "missing_session_open" for event in events)


def test_missing_prior_session_still_cannot_supply_levels(calendar):
    engine, events = warmed(calendar, bars=[])
    assert not feed(engine, setup_bars(calendar))
    state = engine.state_for("AAPL")
    assert state.previous_high is None
    assert state.previous_low is None
    assert state.previous_session_bars == 0
    assert any(event.get("reason") == "previous_session_missing" for event in events)


def test_stale_prior_session_is_not_a_daily_level(calendar):
    engine, _ = warmed(calendar)
    shifted = [replace(item, start=item.start + timedelta(days=1)) for item in setup_bars(calendar)]
    assert not feed(engine, shifted)
    assert engine.state_for("AAPL").previous_high is None


def test_daily_high_overrides_complete_intraday_range(calendar):
    engine, _ = warmed(calendar, daily_bars=[daily_bar(calendar, high="112")])
    (signal,) = feed(engine, setup_bars(calendar))
    assert signal.target == D("112")
    assert signal.metadata["pdh"] == "112"
    assert signal.metadata["previous_session_level_source"] == "daily"


def test_daily_low_controls_sweep_detection(calendar):
    engine, _ = warmed(calendar, daily_bars=[daily_bar(calendar, low="98")])
    assert not feed(engine, setup_bars(calendar))
    state = engine.state_for("AAPL")
    assert state.previous_low == D("98")
    assert state.phase is SetupState.WAITING
    assert state.attempts == 0


def test_daily_levels_allow_setup_without_prior_intraday_history(calendar):
    engine, _ = warmed(calendar, bars=[], daily_bars=[daily_bar(calendar)])
    start = calendar.session(TODAY).open
    prefix = [make_bar(start + timedelta(minutes=5 * i)) for i in range(5)]
    prefix[2] = replace(prefix[2], high=D("100"))
    assert not feed(engine, prefix)
    current = [
        replace(bar, start=bar.start + timedelta(minutes=25)) for bar in setup_bars(calendar)
    ]
    (signal,) = feed(engine, current)
    assert signal.target == D("110")
    assert signal.metadata["previous_session_level_source"] == "daily"
    assert signal.metadata["previous_session_bars"] == 0
    assert signal.metadata["previous_session_complete"] is False


@pytest.mark.parametrize("daily_date", [None, date(2025, 5, 30), TODAY])
def test_missing_or_wrong_session_daily_cannot_fall_back_to_intraday(calendar, daily_date):
    daily = [] if daily_date is None else [daily_bar(calendar, day=daily_date)]
    engine, events = warmed(calendar, daily_bars=daily)
    assert not feed(engine, setup_bars(calendar))
    state = engine.state_for("AAPL")
    assert state.previous_session_complete is True
    assert state.previous_high is None
    assert state.previous_low is None
    assert state.previous_session_level_source is None
    assert any(event.get("reason") == "previous_session_daily_bar_missing" for event in events)


def test_offline_partial_intraday_history_cannot_supply_daily_levels(calendar):
    prior = history(calendar)
    del prior[30]
    engine, events = warmed(calendar, bars=prior)
    assert not feed(engine, setup_bars(calendar))
    assert engine.state_for("AAPL").previous_high is None
    assert any(event.get("reason") == "previous_session_incomplete" for event in events)


def test_daily_inputs_reject_intraday_or_conflicting_bars(calendar):
    engine = StrategyEngine(calendar=calendar)
    with pytest.raises(ValueError, match="complete regular exchange session"):
        engine.set_daily_bars([history(calendar)[0]])
    daily = daily_bar(calendar)
    with pytest.raises(ValueError, match="Conflicting daily bars"):
        engine.set_daily_bars([daily, replace(daily, high=D("112"))])


def test_duplicate_ignored_revision_and_incomplete_rejected(calendar):
    engine, _ = warmed(calendar)
    bar = setup_bars(calendar)[0]
    engine.on_bar(bar)
    assert engine.on_bar(bar) is None
    with pytest.raises(ValueError, match="chronological"):
        engine.on_bar(replace(bar, high=D("100")))
    with pytest.raises(ValueError, match="incomplete"):
        engine.on_bar(setup_bars(calendar)[1], now=bar.end)
    with pytest.raises(ValueError, match="five-minute"):
        engine.on_bar(replace(setup_bars(calendar)[1], duration=timedelta(minutes=1)))


def test_order_events_and_position_blocking(calendar):
    engine, _ = warmed(calendar)
    feed(engine, setup_bars(calendar))
    engine.on_order_event("AAPL", "filled")
    assert engine.state_for("AAPL").phase is SetupState.POSITION_OPEN
    engine.on_order_event("AAPL", "rejected")
    assert engine.state_for("AAPL").holding
    engine.on_order_event("AAPL", "closed")
    assert engine.state_for("AAPL").phase is SetupState.COMPLETE
    assert not engine.state_for("AAPL").holding
    engine, _ = warmed(calendar)
    engine.set_position("AAPL", True)
    assert not feed(engine, setup_bars(calendar))


def test_symbols_have_independent_sessions_and_setups(calendar):
    engine, _ = warmed(calendar)
    for bar in history(calendar, symbol="MSFT"):
        engine.on_bar(bar)
    aapl = setup_bars(calendar)
    msft = [replace(bar, symbol="MSFT") for bar in aapl]
    signals = []
    for left, right in zip(aapl, msft, strict=True):
        signals.extend(feed(engine, [left, right]))
    assert {item.symbol for item in signals} == {"AAPL", "MSFT"}
    assert len({item.id for item in signals}) == 2


def test_one_setup_per_session_and_configured_retry(calendar):
    for maximum in (1, 2):
        engine, _ = warmed(calendar, StrategyConfig(max_setups_per_symbol_per_session=maximum))
        current = setup_bars(calendar)
        bad_c = replace(current[2], low=D("99.7"))
        feed(engine, [current[0], current[1], bad_c])
        next_sweep = replace(current[0], start=bad_c.end)
        engine.on_bar(next_sweep)
        assert engine.state_for("AAPL").attempts == maximum
        expected = SetupState.INVALIDATED if maximum == 1 else SetupState.LIQUIDITY_SWEPT
        assert engine.state_for("AAPL").phase is expected


def test_reward_risk_minimum_is_inclusive(calendar):
    prior = history(calendar)
    prior[3] = replace(prior[3], high=D("103.70"))
    engine, _ = warmed(calendar, bars=prior)
    current = setup_bars(calendar)
    current[0] = replace(current[0], low=D("98.81"))
    (signal,) = feed(engine, current)
    assert signal.reward_risk == D("2.5")
