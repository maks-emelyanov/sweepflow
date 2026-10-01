from dataclasses import replace
from decimal import Decimal

import pytest

from examples.make_demo import demo_bars
from sweepflow.cli import main
from sweepflow.config import AppConfig
from sweepflow.data import FiveMinuteAggregator, write_csv
from sweepflow.replay import replay
from sweepflow.storage import Journal


def test_end_to_end_setup_fill_and_target_are_causal():
    bars = demo_bars()
    with Journal(":memory:") as journal:
        result = replay(bars, AppConfig(), journal)
        events = journal.events()
    assert result.signals == result.trades == result.wins == 1
    assert result.pnl == Decimal("792")
    accepted = next(event for event in events if event.get("status") == "accepted")
    filled = next(event for event in events if event.get("status") == "filled")
    closed = next(event for event in events if event.get("status") == "closed")
    assert accepted["at"] < filled["at"] < closed["at"]
    assert accepted["quantity"] == 99
    assert closed["reason"] == "take_profit"


def test_replay_refuses_missing_minute_before_processing_orders():
    bars = demo_bars()
    del bars[405]
    with pytest.raises(ValueError, match="all 390"):
        replay(bars, AppConfig())


def test_five_minute_data_cannot_be_used_to_simulate_fills():
    aggregator = FiveMinuteAggregator()
    bars = [out for item in demo_bars() if (out := aggregator.on_bar(item)) is not None]
    with pytest.raises(ValueError, match="one-minute"):
        replay(bars, AppConfig())


def test_multi_symbol_replay_is_independent_of_input_order():
    spy = demo_bars()
    aapl = [replace(bar, symbol="AAPL") for bar in spy]
    first = replay(spy + aapl, AppConfig())
    second = replay(list(reversed(aapl + spy)), AppConfig())
    assert first == second
    assert first.trades == 2
    assert first.pnl == Decimal("1584")


def test_cli_runs_and_effective_config_is_audited(tmp_path, capsys):
    source = tmp_path / "bars.csv"
    db = tmp_path / "audit.sqlite"
    write_csv(source, demo_bars())
    assert main(["replay", str(source), "--db", str(db), "--allow-shorts"]) == 0
    assert '"pnl": "792' in capsys.readouterr().out
    with Journal(db) as journal:
        start = next(event for event in journal.events() if event.get("event") == "run_started")
    assert start["config"]["execution"]["allow_shorts"] is True


def test_live_command_reports_unsupported_without_network(capsys):
    assert main(["live"]) == 1
    assert "atomic entry/stop/target" in capsys.readouterr().err
