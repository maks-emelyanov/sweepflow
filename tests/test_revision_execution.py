"""Revised inputs must replace only unfilled entries, never replay real fills."""

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from test_alpaca_execution import NOW, FakeAlpaca, signal

from sweepflow.alpaca import AlpacaAPIError
from sweepflow.alpaca_execution import AlpacaPaperBroker
from sweepflow.config import AppConfig
from sweepflow.storage import Journal


@pytest.fixture
def execution(tmp_path):
    client = FakeAlpaca()
    with Journal(tmp_path / "revisions.sqlite") as journal:
        broker = AlpacaPaperBroker(client, AppConfig(), journal)
        yield broker, client, journal


def revision_pair():
    original = signal(metadata={"sweep_at": (NOW - timedelta(minutes=15)).isoformat()})
    revised = replace(original, id="revised-prices", entry=Decimal("100.1"))
    return original, revised


def finish_original(broker, client):
    client.fill_entry()
    order = client.orders["order-1"]
    order["legs"][0]["status"] = "canceled"
    order["legs"][1].update(status="filled", filled_qty=order["qty"])
    client.position("AAPL", 0)
    broker.sync(NOW)


def test_revised_pending_prices_replace_confirmed_zero_fill_entry(execution):
    broker, client, journal = execution
    original, revised = revision_pair()
    broker.submit(original, NOW)
    broker.invalidate_pending({revised.id}, NOW)
    assert broker.submit(revised, NOW)["status"] == "submitted"
    assert client.orders["order-1"]["status"] == "canceled"
    assert client.submitted[-1]["limit_price"] == "100.1"
    assert len(client.submitted) == 2
    restarted = AlpacaPaperBroker(client, broker.config, journal)
    assert restarted.submit(revised, NOW)["reason"] == "duplicate_signal"
    assert len(client.submitted) == 2


def test_replacement_waits_for_broker_cancellation_confirmation(execution):
    broker, client, _ = execution
    original, revised = revision_pair()
    broker.submit(original, NOW)
    client.cancel_ack = False
    broker.invalidate_pending({revised.id}, NOW)
    assert "closing:AAPL" in broker.submit(revised, NOW)["reason"]
    assert len(client.submitted) == 1
    client.cancel_ack = True
    assert broker.submit(revised, NOW)["status"] == "submitted"
    assert len(client.submitted) == 2


def test_restored_signal_geometry_uses_durable_distinct_submission_ids(execution):
    broker, client, journal = execution
    original, revised = revision_pair()
    broker.submit(original, NOW)
    for current in (revised, original, revised, original):
        broker.invalidate_pending({current.id}, NOW)
        assert broker.submit(current, NOW)["status"] == "submitted"
        broker = AlpacaPaperBroker(client, broker.config, journal)
        assert broker.submit(current, NOW)["reason"] == "duplicate_signal"
    assert len(client.submitted) == 5
    assert len({order["client_order_id"] for order in client.submitted}) == 5
    assert client.orders["order-5"]["limit_price"] == "100"


def test_restored_signal_with_unknown_resubmission_never_posts_twice(execution):
    broker, client, journal = execution
    original, _ = revision_pair()
    broker.submit(original, NOW)
    broker.invalidate_pending(set(), NOW)
    client.submit_error = AlpacaAPIError("lost restored-order response")
    assert broker.submit(original, NOW)["status"] == "unknown"
    client.submit_error = None
    restarted = AlpacaPaperBroker(client, broker.config, journal)
    assert restarted.submit(original, NOW)["reason"] == "duplicate_signal"
    assert len(client.submitted) == 2


def test_expiration_does_not_enable_same_signal_resubmission(execution):
    broker, client, _ = execution
    original, _ = revision_pair()
    broker.submit(original, NOW)
    after_expiry = NOW + timedelta(minutes=21)
    broker.sync(after_expiry)
    assert client.orders["order-1"]["status"] == "canceled"
    assert broker.submit(original, after_expiry)["reason"] == "duplicate_signal"
    assert len(client.submitted) == 1


def test_unknown_submission_cannot_be_replaced_by_revision(execution):
    broker, client, _ = execution
    original, revised = revision_pair()
    client.submit_error = AlpacaAPIError("lost response")
    assert broker.submit(original, NOW)["status"] == "unknown"
    client.submit_error = None
    broker.invalidate_pending({revised.id}, NOW)
    assert "unknown_submission:AAPL" in broker.submit(revised, NOW)["reason"]
    assert len(client.submitted) == 1


def test_revision_preserves_filled_position_and_original_protective_exits(execution):
    broker, client, _ = execution
    original, revised = revision_pair()
    broker.submit(original, NOW)
    client.fill_entry()
    broker.invalidate_pending({revised.id}, NOW)
    assert broker.submit(revised, NOW)["reason"] == "setup_already_filled"
    assert client.canceled == []
    assert len(client.submitted) == 1
    assert client.orders["order-1"]["stop_loss"]["stop_price"] == "99"
    assert client.positions["AAPL"]["qty"] == "20"


@pytest.mark.parametrize("reverse_direction", [False, True])
def test_revision_cannot_reenter_completed_setup_after_restart(execution, reverse_direction):
    broker, client, journal = execution
    broker.config = replace(
        broker.config,
        strategy=replace(broker.config.strategy, max_setups_per_symbol_per_session=2),
    )
    original, revised = revision_pair()
    broker.submit(original, NOW)
    finish_original(broker, client)
    # Confirmations and timezone notation may change without creating a new setup.
    revised = replace(
        revised,
        created_at=NOW + timedelta(seconds=30),
        metadata={
            "sweep_at": (NOW - timedelta(minutes=15))
            .astimezone(ZoneInfo("America/New_York"))
            .isoformat()
        },
    )
    if reverse_direction:
        revised = replace(revised, direction="SHORT", stop=Decimal("101"), target=Decimal("97"))
    restarted = AlpacaPaperBroker(client, broker.config, journal)
    assert (
        restarted.submit(revised, NOW + timedelta(seconds=30))["reason"] == "setup_already_filled"
    )
    assert len(client.submitted) == 1


def test_fill_during_cancellation_blocks_revision_even_after_flattening(execution):
    broker, client, journal = execution
    original, revised = revision_pair()
    broker.submit(original, NOW)
    client.cancel_hook = lambda: client.fill_entry(5)
    broker.invalidate_pending({revised.id}, NOW)
    assert "closing:AAPL" in broker.submit(revised, NOW)["reason"]
    assert len(client.submitted) == 2
    assert client.submitted[-1]["type"] == "market"
    client.orders["order-2"].update(status="filled", filled_qty="5")
    client.position("AAPL", 0)
    broker.sync(NOW)
    restarted = AlpacaPaperBroker(client, broker.config, journal)
    assert restarted.submit(revised, NOW)["reason"] == "setup_already_filled"
    assert len(client.submitted) == 2


def test_moved_sweep_cannot_reset_actual_session_fill_limit(execution):
    broker, client, journal = execution
    original, revised = revision_pair()
    broker.submit(original, NOW)
    finish_original(broker, client)
    revised = replace(revised, metadata={"sweep_at": (NOW - timedelta(minutes=10)).isoformat()})
    restarted = AlpacaPaperBroker(client, broker.config, journal)
    assert restarted.submit(revised, NOW)["reason"] == "session_setup_limit"
    assert len(client.submitted) == 1


def test_configured_second_distinct_setup_is_still_allowed(execution):
    broker, client, _ = execution
    broker.config = replace(
        broker.config,
        strategy=replace(broker.config.strategy, max_setups_per_symbol_per_session=2),
    )
    original, revised = revision_pair()
    broker.submit(original, NOW)
    finish_original(broker, client)
    revised = replace(revised, metadata={"sweep_at": (NOW - timedelta(minutes=10)).isoformat()})
    assert broker.submit(revised, NOW)["status"] == "submitted"
    assert len(client.submitted) == 2


def test_session_fill_limit_resets_on_next_session(execution):
    broker, client, _ = execution
    original, revised = revision_pair()
    broker.submit(original, NOW)
    finish_original(broker, client)
    tomorrow = NOW + timedelta(days=1)
    client.clock["next_close"] = "2026-09-29T20:00:00Z"
    revised = replace(
        revised,
        created_at=tomorrow,
        expires_at=tomorrow + timedelta(minutes=20),
        metadata={"sweep_at": (tomorrow - timedelta(minutes=15)).isoformat()},
    )
    assert broker.submit(revised, tomorrow)["status"] == "submitted"
    assert len(client.submitted) == 2
