from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from sweepflow.alpaca import AlpacaAPIError
from sweepflow.alpaca_execution import AlpacaPaperBroker
from sweepflow.config import AppConfig, ExecutionConfig
from sweepflow.models import Direction, Signal
from sweepflow.storage import Journal

NOW = datetime(2026, 9, 28, 14, tzinfo=UTC)


def signal(identity="signal-one", symbol="AAPL", short=False, **changes):
    values = dict(
        id=identity,
        symbol=symbol,
        direction=Direction.SHORT if short else Direction.LONG,
        entry=Decimal("100"),
        stop=Decimal("101" if short else "99"),
        target=Decimal("97" if short else "103"),
        fvg_low=Decimal("99.5"),
        fvg_high=Decimal("100.5"),
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=20),
    )
    values.update(changes)
    return Signal(**values)


class FakeAlpaca:
    def __init__(self):
        self.account = dict(
            id="paper-account",
            status="ACTIVE",
            equity="10000",
            last_equity="10000",
            buying_power="40000",
            shorting_enabled=True,
        )
        self.clock = dict(is_open=True, next_close="2026-09-28T20:00:00Z")
        self.positions = {}
        self.orders = {}
        self.submitted = []
        self.canceled = []
        self.asset = dict(
            status="active", tradable=True, shortable=True, easy_to_borrow=True, marginable=True
        )
        self.submit_error = None
        self.error_after_accept = False
        self.cancel_ack = True
        self.cancel_hook = None
        self.before_post = None

    def get_account(self):
        return deepcopy(self.account)

    def get_clock(self):
        return deepcopy(self.clock)

    def get_positions(self):
        return deepcopy(list(self.positions.values()))

    def get_open_orders(self):
        terminal = {"filled", "canceled", "expired", "rejected"}
        return deepcopy(
            [
                o
                for o in self.orders.values()
                if o["status"] not in terminal
                or any(leg["status"] not in terminal for leg in o.get("legs", []))
            ]
        )

    def get_order(self, identity):
        return deepcopy(self.orders[identity])

    def get_order_by_client_id(self, identity):
        return deepcopy(
            next((o for o in self.orders.values() if o["client_order_id"] == identity), None)
        )

    def get_asset(self, symbol):
        return deepcopy(self.asset)

    def submit_order(self, payload):
        if self.before_post:
            self.before_post(payload)
        self.submitted.append(deepcopy(payload))
        if self.submit_error and not self.error_after_accept:
            raise self.submit_error
        identity = f"order-{len(self.orders) + 1}"
        order = dict(payload, id=identity, status="new", filled_qty="0", legs=[])
        if payload.get("order_class") == "bracket":
            side = "sell" if payload["side"] == "buy" else "buy"
            for suffix, kind, extra in [
                ("stop", "stop", payload["stop_loss"]),
                ("target", "limit", payload["take_profit"]),
            ]:
                order["legs"].append(
                    dict(
                        id=f"{identity}-{suffix}",
                        symbol=payload["symbol"],
                        client_order_id=f"{identity}-{suffix}",
                        status="held",
                        side=side,
                        qty=payload["qty"],
                        filled_qty="0",
                        type=kind,
                        **extra,
                    )
                )
        self.orders[identity] = order
        if self.submit_error:
            raise self.submit_error
        return deepcopy(order)

    def cancel_order(self, identity):
        self.canceled.append(identity)
        if self.cancel_hook:
            hook, self.cancel_hook = self.cancel_hook, None
            hook()
        for order in self.orders.values():
            group = [order, *order.get("legs", [])]
            if any(item["id"] == identity for item in group):
                for item in group:
                    if item["status"] not in {"filled", "canceled", "expired", "rejected"}:
                        item["status"] = "canceled" if self.cancel_ack else "pending_cancel"
                return
        raise AlpacaAPIError("not found", 404)

    def fill_entry(self, qty=None):
        order = self.orders["order-1"]
        qty = int(qty if qty is not None else order["qty"])
        order.update(
            filled_qty=str(qty), status="filled" if qty == int(order["qty"]) else "partially_filled"
        )
        self.position(order["symbol"], qty if order["side"] == "buy" else -qty)

    def position(self, symbol, qty):
        if not qty:
            self.positions.pop(symbol, None)
        else:
            self.positions[symbol] = dict(
                symbol=symbol,
                qty=str(qty),
                side="long" if qty > 0 else "short",
                current_price="100",
                market_value=str(qty * 100),
            )


@pytest.fixture
def setup(tmp_path):
    client, journal = FakeAlpaca(), Journal(tmp_path / "paper.sqlite")
    broker = AlpacaPaperBroker(client, AppConfig(), journal)
    yield broker, client, journal
    journal.close()


def test_actual_equity_whole_share_gtc_bracket_and_durable_intent(setup):
    broker, client, journal = setup

    def committed(payload):
        state = journal.get_alpaca_state("paper-account")
        assert state["intents"][payload["client_order_id"]]["state"] == "unknown"

    client.before_post = committed
    result = broker.submit(signal(), NOW)
    assert result["quantity"] == 20  # Real 10k account, not configured replay 100k.
    # Stable broker IDs preserve ownership and retries across project renames.
    assert result["client_order_id"] == "liq-entry-fa93ccbdf9084ca3ac9f57934e8bc6bb"
    assert client.submitted[0] == dict(
        symbol="AAPL",
        qty="20",
        side="buy",
        type="limit",
        limit_price="100",
        time_in_force="gtc",
        order_class="bracket",
        extended_hours=False,
        client_order_id=result["client_order_id"],
        take_profit={"limit_price": "103"},
        stop_loss={"stop_price": "99"},
    )
    assert broker.submit(signal(), NOW)["reason"] == "duplicate_signal"
    restarted = AlpacaPaperBroker(client, AppConfig(), journal)
    assert restarted.submit(signal(), NOW)["reason"] == "duplicate_signal"
    assert len(client.submitted) == 1


def test_unknown_post_never_retries_even_after_restart(setup):
    broker, client, journal = setup
    client.submit_error = AlpacaAPIError("timeout")
    assert broker.submit(signal(), NOW)["status"] == "unknown"
    client.submit_error = None
    restarted = AlpacaPaperBroker(client, AppConfig(), journal)
    assert "unknown_submission:AAPL" in restarted.sync(NOW)["blocked"]
    restarted.submit(signal(), NOW)
    restarted.submit(signal("second", "MSFT"), NOW)
    assert len(client.submitted) == 1


def test_unknown_accepted_post_is_reconciled_by_client_id(setup):
    broker, client, _ = setup
    client.submit_error, client.error_after_accept = AlpacaAPIError("response lost"), True
    assert broker.submit(signal(), NOW)["status"] == "submitted"
    assert len(client.submitted) == 1
    assert broker.sync(NOW)["blocked"] == []


@pytest.mark.parametrize(
    "short_enabled,borrowable,expected",
    [(False, True, "shorts_disabled"), (True, False, "asset_not_shortable"), (True, True, None)],
)
def test_short_account_asset_and_configuration_gates(setup, short_enabled, borrowable, expected):
    broker, client, _ = setup
    broker.config = replace(broker.config, execution=ExecutionConfig(allow_shorts=short_enabled))
    client.asset["easy_to_borrow"] = borrowable
    result = broker.submit(signal(short=True), NOW)
    if expected:
        assert result["reason"] == expected
        assert client.submitted == []
    else:
        assert client.submitted[0]["side"] == "sell"
        assert result["status"] == "submitted"


def test_external_position_and_orders_block_without_mutation(setup):
    broker, client, _ = setup
    client.position("TSLA", 3)
    result = broker.submit(signal(), NOW)
    assert "unmanaged_or_mismatched_position:TSLA" in result["reason"]
    client.positions.clear()
    client.orders["external"] = dict(
        id="external", symbol="TSLA", status="new", filled_qty="0", qty="3"
    )
    assert "unmanaged_open_orders" in broker.submit(signal(), NOW)["reason"]
    assert client.submitted == client.canceled == []


def test_partial_entry_waits_for_cancel_confirmation_then_closes_actual_qty(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.fill_entry(7)
    client.cancel_ack = False
    result = broker.sync(NOW)
    assert "closing:AAPL" in result["blocked"]
    assert len(client.submitted) == 1
    client.cancel_ack = True
    # Another share fills before cancellation completes.
    client.cancel_hook = lambda: client.fill_entry(8)
    broker.sync(NOW)
    assert client.submitted[1]["qty"] == "8"
    assert client.submitted[1]["side"] == "sell"
    assert client.submitted[1]["type"] == "market"
    broker.sync(NOW)
    assert len(client.submitted) == 2


def test_full_filled_held_bracket_exits_survive_expiry_and_invalidation(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.fill_entry()
    result = broker.invalidate_pending(set(), NOW + timedelta(minutes=30))
    assert result["blocked"] == []
    assert client.canceled == []
    assert len(client.submitted) == 1


def test_unprotected_position_is_flattened_with_confirmed_cancel(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.fill_entry()
    client.orders["order-1"]["legs"][0]["status"] = "canceled"
    broker.sync(NOW)
    assert client.submitted[-1]["type"] == "market"
    assert client.submitted[-1]["qty"] == "20"


def test_ownership_mismatch_blocks_without_touching_exits(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.fill_entry()
    client.position("AAPL", 25)
    result = broker.sync(NOW.replace(hour=19, minute=59, second=30))
    assert "unmanaged_or_mismatched_position:AAPL" in result["blocked"]
    assert len(client.submitted) == 1
    assert client.canceled == []


def test_exit_fill_during_cancel_never_reverses_position(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.fill_entry()

    def exit_filled():
        client.orders["order-1"]["legs"][0].update(filled_qty="20", status="filled")
        client.position("AAPL", 0)

    client.cancel_hook = exit_filled
    broker.sync(NOW.replace(hour=19, minute=59, second=30))
    assert len(client.submitted) == 1
    assert next(iter(broker.state["intents"].values()))["state"] == "completed"


def test_both_exit_legs_fill_reverse_exposure_is_repaired(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.fill_entry()
    for leg in client.orders["order-1"]["legs"]:
        leg.update(filled_qty="20", status="filled")
    client.position("AAPL", -20)
    result = broker.sync(NOW)
    assert "closing:AAPL" in result["blocked"]
    assert client.submitted[-1]["side"] == "buy"
    assert client.submitted[-1]["qty"] == "20"


def test_closed_market_preserves_protection_but_cancels_expired_unfilled_entry(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.clock["is_open"] = False
    broker.sync(NOW + timedelta(hours=7))
    assert "order-1" in client.canceled
    assert len(client.submitted) == 1


def test_closed_market_filled_position_retains_gtc_exits(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.fill_entry()
    client.clock["is_open"] = False
    broker.sync(NOW + timedelta(days=1))
    assert client.canceled == []
    assert len(client.submitted) == 1
    client.clock.update(is_open=True, next_close="2026-09-29T20:00:00Z")
    broker.sync(NOW + timedelta(days=1))
    assert client.submitted[-1]["type"] == "market"


def test_daily_halt_uses_last_close_persists_after_recovery_restart(setup):
    broker, client, journal = setup
    client.account["equity"] = "9890"
    assert broker.sync(NOW)["halted"]
    client.account["equity"] = "10050"
    restarted = AlpacaPaperBroker(client, AppConfig(), journal)
    assert restarted.sync(NOW)["halted"]
    assert "daily_loss_limit" in restarted.submit(signal(), NOW)["reason"]
    assert not restarted.sync(NOW + timedelta(days=1))["halted"]


def test_daily_halt_flattens_managed_exposure(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.fill_entry()
    client.account["equity"] = "9890"
    assert broker.sync(NOW)["halted"]
    assert client.submitted[-1]["type"] == "market"


def test_pending_invalidation_cancel_is_persisted(setup):
    broker, client, journal = setup
    broker.submit(signal(), NOW)
    broker.invalidate_pending(set(), NOW)
    assert "order-1" in client.canceled
    assert len(client.submitted) == 1
    state = journal.get_alpaca_state("paper-account")
    assert next(iter(state["intents"].values()))["state"] == "completed"


@pytest.mark.parametrize("age,reason", [(91, "stale_signal"), (1200, "signal_not_current")])
def test_old_signals_rejected(setup, age, reason):
    broker, client, _ = setup
    assert broker.submit(signal(), NOW + timedelta(seconds=age))["reason"] == reason
    assert client.submitted == []


def test_unsupported_price_precision_rejected(setup):
    broker, client, _ = setup
    result = broker.submit(signal(entry=Decimal("100.001")), NOW)
    assert result["reason"] == "unsupported_price_precision"
    assert client.submitted == []


def test_risk_and_buying_power_reserve_pending_entries(setup):
    broker, client, _ = setup
    client.account["buying_power"] = "2500"
    assert broker.submit(signal(), NOW)["quantity"] == 20
    assert broker.submit(signal("second", "MSFT"), NOW)["quantity"] == 5


def test_eod_blocks_new_entries(setup):
    broker, client, _ = setup
    at = NOW.replace(hour=19, minute=59, second=10)
    result = broker.submit(signal(created_at=at, expires_at=at + timedelta(minutes=1)), at)
    assert "session_end" in result["reason"]
    assert client.submitted == []


def test_overfill_repair_completion_clears_signed_exposure(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.fill_entry()
    for leg in client.orders["order-1"]["legs"]:
        leg.update(filled_qty="20", status="filled")
    client.position("AAPL", -20)
    broker.sync(NOW)
    client.orders["order-2"].update(status="filled", filled_qty="20")
    client.position("AAPL", 0)
    result = broker.sync(NOW)
    assert result["blocked"] == []
    assert result["managed_symbols"] == []
    assert len(client.submitted) == 2


def test_pending_entry_can_cancel_despite_external_same_symbol_position(setup):
    broker, client, _ = setup
    broker.submit(signal(), NOW)
    client.position("AAPL", 5)
    result = broker.invalidate_pending(set(), NOW)
    assert "order-1" in client.canceled
    assert "unmanaged_or_mismatched_position:AAPL" in result["blocked"]
    assert len(client.submitted) == 1
    assert client.positions["AAPL"]["qty"] == "5"


def test_unknown_close_cannot_be_duplicated(setup):
    broker, client, journal = setup
    broker.submit(signal(), NOW)
    client.fill_entry(5)
    client.submit_error = AlpacaAPIError("timeout")
    broker.sync(NOW)
    assert client.submitted[-1]["client_order_id"] == "liq-close-35ba31b068e7a15d083ac7557c55ad50"
    client.submit_error = None
    restarted = AlpacaPaperBroker(client, AppConfig(), journal)
    assert "unknown_submission:AAPL" in restarted.sync(NOW)["blocked"]
    assert len(client.submitted) == 2


def test_full_account_snapshot_is_persisted(setup):
    broker, client, journal = setup
    snapshot = broker.sync(NOW)
    assert snapshot["buying_power"] == "40000"
    state = journal.get_alpaca_state("paper-account")
    assert state["account"]["equity"] == "10000"
    assert state["positions"] == state["open_orders"] == []


def test_slow_asset_request_rechecks_freshness_before_post(setup, monkeypatch):
    import sweepflow.alpaca_execution as module

    broker, client, _ = setup
    current = [NOW]

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return current[0]

    monkeypatch.setattr(module, "datetime", FakeDatetime)

    def slow_asset(symbol):
        current[0] = NOW + timedelta(seconds=100)
        return deepcopy(client.asset)

    client.get_asset = slow_asset
    assert broker.submit(signal())["reason"] == "stale_signal"
    assert client.submitted == []


def test_journal_failure_prevents_post(setup, monkeypatch):
    broker, client, journal = setup
    original = journal.save_alpaca_state

    def fail_on_intent(account, state):
        if state["intents"]:
            raise RuntimeError("disk full")
        original(account, state)

    monkeypatch.setattr(journal, "save_alpaca_state", fail_on_intent)
    with pytest.raises(RuntimeError, match="disk full"):
        broker.submit(signal(), NOW)
    assert client.submitted == []


def test_partial_short_closes_with_buy_and_completes(setup):
    broker, client, _ = setup
    broker.config = replace(broker.config, execution=ExecutionConfig(allow_shorts=True))
    broker.submit(signal(short=True), NOW)
    client.fill_entry(7)
    broker.sync(NOW)
    assert client.submitted[-1]["side"] == "buy"
    assert client.submitted[-1]["qty"] == "7"
    client.orders["order-2"].update(status="filled", filled_qty="7")
    client.position("AAPL", 0)
    result = broker.sync(NOW)
    assert result["managed_symbols"] == []
    assert result["blocked"] == []


def test_daily_remaining_risk_limits_quantity_before_drawdown_limit(setup):
    broker, client, _ = setup
    client.account["equity"] = "9905"
    result = broker.submit(signal(), NOW)
    assert result["quantity"] == 5  # $100 daily budget minus $95 realized drawdown.


def test_maximum_concurrent_symbols_counts_pending_orders(setup):
    broker, client, _ = setup
    broker.config = replace(
        broker.config, risk=replace(broker.config.risk, max_concurrent_positions=1)
    )
    broker.submit(signal(), NOW)
    assert broker.submit(signal("other", "MSFT"), NOW)["reason"] == "position_limit"
    assert len(client.submitted) == 1
