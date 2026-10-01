import json
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal as D
from zoneinfo import ZoneInfo

from sweepflow.config import ExecutionConfig, RiskConfig
from sweepflow.execution import PaperBroker
from sweepflow.models import Bar, Direction, Signal

NY = ZoneInfo("America/New_York")
BASE = datetime(2026, 9, 22, 10, 0, tzinfo=NY)


def signal(symbol="AAPL", *, direction=Direction.LONG, identifier=None, **changes):
    prices = ("100", "99", "103", "99.5", "100")
    if direction == Direction.SHORT:
        prices = ("100", "101", "97", "100", "100.5")
    result = Signal(
        id=identifier or symbol,
        symbol=symbol,
        direction=direction,
        entry=D(prices[0]),
        stop=D(prices[1]),
        target=D(prices[2]),
        fvg_low=D(prices[3]),
        fvg_high=D(prices[4]),
        created_at=BASE,
        expires_at=BASE + timedelta(minutes=30),
    )
    return replace(result, **changes)


def bar(minute=0, *, symbol="AAPL", o="101", h="102", low="99.5", c="101"):
    return Bar(
        symbol,
        BASE + timedelta(minutes=minute),
        D(o),
        D(h),
        D(low),
        D(c),
        duration=timedelta(minutes=1),
    )


class PaperBrokerTests(unittest.TestCase):
    def test_fixed_risk_and_symbol_notional_are_whole_share_limits(self):
        broker = PaperBroker()
        accepted = broker.submit(signal())
        self.assertEqual(accepted.quantity, 200)  # 20% notional caps the 250 risk shares.
        self.assertEqual(broker.reserved_risk, D("200"))
        small_account = PaperBroker(
            RiskConfig(account_equity=D("1000"), max_symbol_notional=D("0.8"))
        )
        self.assertEqual(small_account.submit(signal()).quantity, 2)

    def test_pending_orders_reserve_slots_notional_and_daily_risk(self):
        broker = PaperBroker(RiskConfig(max_concurrent_positions=10, max_total_notional=D("0.5")))
        self.assertEqual(broker.submit(signal("A")).quantity, 200)
        self.assertEqual(broker.submit(signal("B")).quantity, 200)
        self.assertEqual(broker.submit(signal("C")).quantity, 100)
        self.assertEqual(broker.submit(signal("D")).reason, "insufficient_risk_or_buying_power")
        broker.cancel("A", BASE)
        self.assertEqual(broker.submit(signal("E")).quantity, 200)
        slots = PaperBroker()
        for symbol in ("A", "B", "C", "D"):
            self.assertEqual(slots.submit(signal(symbol)).status, "accepted")
        self.assertEqual(slots.submit(signal("E")).reason, "position_limit")

    def test_daily_risk_reservations_limit_new_order_before_actual_losses(self):
        broker = PaperBroker(RiskConfig(risk_per_trade=D("0.01"), max_symbol_notional=D("0.8")))
        first = signal(stop=D("98"))
        self.assertEqual(broker.submit(first).quantity, 500)
        self.assertEqual(broker.reserved_risk, D("1000"))
        self.assertEqual(broker.submit(signal("MSFT")).reason, "insufficient_risk_or_buying_power")
        broker.cancel("AAPL", BASE)
        self.assertEqual(broker.reserved_risk, 0)
        self.assertEqual(broker.submit(signal("IBM")).status, "accepted")

    def test_reject_duplicate_id_and_active_symbol(self):
        broker = PaperBroker()
        broker.submit(signal())
        self.assertEqual(broker.submit(signal()).reason, "duplicate_signal")
        self.assertEqual(broker.submit(signal(identifier="second")).reason, "symbol_already_active")
        broker.cancel("AAPL", BASE)
        self.assertEqual(broker.submit(signal()).reason, "duplicate_signal")

    def test_cannot_fill_inside_confirmation_candle_or_after_expiry(self):
        broker = PaperBroker()
        broker.submit(signal())
        self.assertEqual(broker.on_bar(bar(-1, low="98")), [])
        self.assertIn("AAPL", broker.pending)
        events = broker.on_bar(bar(30, low="98"))
        self.assertEqual([(e.status, e.reason) for e in events], [("cancelled", "expired")])
        self.assertEqual(broker.realized_pnl, 0)

    def test_explicit_submission_time_prevents_historical_fills(self):
        broker = PaperBroker()
        broker.submit(signal(), at=BASE + timedelta(minutes=2))
        self.assertEqual(broker.on_bar(bar(1, low="98")), [])
        self.assertIn("AAPL", broker.pending)
        self.assertEqual(broker.on_bar(bar(2))[0].status, "filled")

    def test_entry_plus_stop_counts_loss_and_stop_wins_both_exits(self):
        broker = PaperBroker()
        broker.submit(signal())
        events = broker.on_bar(bar(low="98", h="104"))
        self.assertEqual([e.status for e in events], ["filled", "closed"])
        self.assertEqual(events[-1].reason, "stop_loss")
        self.assertEqual(events[-1].pnl, D("-200"))
        self.assertFalse(broker.positions)
        self.assertFalse(broker.pending)

    def test_fvg_invalidation_does_not_optimistically_cancel_marketable_order(self):
        broker = PaperBroker()
        broker.submit(signal())
        events = broker.on_bar(bar(o="99.25", h="100", low="99.1", c="99.5"))
        self.assertEqual([e.status for e in events], ["filled"])
        self.assertEqual(events[0].price, D("100"))
        self.assertIn("AAPL", broker.positions)

    def test_stop_gap_uses_worse_open_for_new_or_existing_position(self):
        for existing in (False, True):
            with self.subTest(existing=existing):
                broker = PaperBroker()
                broker.submit(signal())
                if existing:
                    broker.on_bar(bar())
                events = broker.on_bar(bar(1, o="97", h="98", low="96", c="97"))
                self.assertEqual(events[-1].price, D("97"))
                self.assertEqual(events[-1].pnl, D("-600"))

    def test_target_before_entry_cancels_and_releases_reservations(self):
        broker = PaperBroker()
        broker.submit(signal())
        events = broker.on_bar(bar(o="102", h="104", low="101", c="103"))
        self.assertEqual(events[0].reason, "target_before_entry")
        self.assertEqual(broker.pending_notional, 0)
        self.assertEqual(broker.reserved_risk, 0)
        known_open = PaperBroker()
        known_open.submit(signal())
        events = known_open.on_bar(bar(o="103", h="104", low="98", c="101"))
        self.assertEqual(events[0].status, "cancelled")
        self.assertEqual(known_open.realized_pnl, 0)

    def test_ambiguous_entry_target_gets_no_same_minute_profit(self):
        broker = PaperBroker()
        broker.submit(signal())
        events = broker.on_bar(bar(h="104"))
        self.assertEqual([e.status for e in events], ["filled"])
        self.assertIn("AAPL", broker.positions)
        events = broker.on_bar(bar(1, o="102", h="104", low="101", c="103"))
        self.assertEqual(events[-1].reason, "take_profit")
        self.assertEqual(events[-1].pnl, D("600"))
        self.assertFalse(broker.positions)
        self.assertEqual(broker.on_bar(bar(2, low="98")), [])  # Linked stop is gone.

    def test_marketable_open_then_target_can_exit_on_entry_minute(self):
        broker = PaperBroker()
        broker.submit(signal())
        events = broker.on_bar(bar(o="100", h="104", low="99.5", c="103"))
        self.assertEqual([e.status for e in events], ["filled", "closed"])
        self.assertEqual(events[-1].pnl, D("600"))

    def test_shorts_disabled_by_default_and_symmetric_when_enabled(self):
        short = signal(direction=Direction.SHORT)
        self.assertEqual(PaperBroker().submit(short).reason, "shorts_disabled")
        broker = PaperBroker(execution=ExecutionConfig(allow_shorts=True))
        broker.submit(short)
        events = broker.on_bar(bar(o="99", h="102", low="96", c="100"))
        self.assertEqual(events[-1].reason, "stop_loss")
        self.assertEqual(events[-1].pnl, D("-200"))
        gap = PaperBroker(execution=ExecutionConfig(allow_shorts=True))
        gap.submit(short)
        events = gap.on_bar(bar(o="103", h="104", low="102", c="103"))
        self.assertEqual(events[-1].price, D("103"))
        self.assertEqual(events[-1].pnl, D("-600"))

    def test_realized_daily_breaker_cancels_other_orders_and_latches(self):
        broker = PaperBroker()
        broker.submit(signal())
        broker.submit(signal("MSFT"))
        events = broker.on_bar(bar(o="94", h="95", low="93", c="94"))
        self.assertTrue(broker.halted)
        self.assertIn("circuit_breaker", [e.status for e in events])
        self.assertEqual(events[-1].reason, "daily_loss_limit")
        self.assertFalse(broker.pending)
        self.assertEqual(broker.submit(signal("IBM")).reason, "daily_loss_limit")
        broker.start_session(date(2026, 9, 23))
        self.assertFalse(broker.halted)
        self.assertEqual(broker.daily_pnl, 0)

    def test_daily_loss_includes_unrealized_losses_in_other_positions(self):
        broker = PaperBroker()
        broker.submit(signal())
        broker.submit(signal("MSFT"))
        # AAPL remains open and contributes a marked $180 loss.
        broker.on_bar(bar(o="100", h="100", low="99.1", c="99.1"))
        events = broker.on_bar(bar(symbol="MSFT", o="95.8", h="96", low="95", c="95.8"))
        self.assertEqual(broker.realized_pnl, D("-1020"))
        self.assertTrue(broker.halted)
        self.assertFalse(broker.positions)
        self.assertEqual(events[-1].symbol, "AAPL")
        self.assertEqual(events[-1].price, D("99.1"))

    def test_batch_uses_all_current_marks_before_daily_breaker(self):
        broker = PaperBroker()
        broker.submit(signal("ZZZ"))
        broker.submit(signal("AAPL"))
        broker.on_bars(
            [
                bar(symbol="ZZZ", o="100", h="100", low="99.1", c="99.1"),
                bar(symbol="AAPL", o="101", h="102", low="100.5", c="101"),
            ]
        )
        events = broker.on_bars(
            [
                bar(1, symbol="AAPL", o="95.8", h="96", low="95", c="95.8"),
                bar(1, symbol="ZZZ", o="99.5", h="100.5", low="99.2", c="100"),
            ]
        )
        self.assertEqual(broker.daily_pnl, D("-840"))
        self.assertFalse(broker.halted)
        self.assertNotIn("circuit_breaker", [event.status for event in events])
        self.assertIn("ZZZ", broker.positions)

    def test_batch_rejects_bad_input_before_mutation(self):
        broker = PaperBroker()
        broker.submit(signal())
        snapshot = broker.to_dict()
        for bars in (
            [bar(), bar()],
            [bar(), bar(1, symbol="MSFT")],
            [bar(), replace(bar(symbol="MSFT"), duration=timedelta(minutes=5))],
        ):
            with self.subTest(bars=bars), self.assertRaises(ValueError):
                broker.on_bars(bars)
            self.assertEqual(broker.to_dict(), snapshot)

    def test_expiry_is_exclusive_and_session_flatten_closes_and_cancels(self):
        broker = PaperBroker()
        broker.submit(signal())
        broker.submit(signal("MSFT"))
        broker.on_bar(bar())
        events = broker.flatten_session(BASE + timedelta(hours=6), {"AAPL": D("102")})
        self.assertEqual([e.status for e in events], ["cancelled", "closed"])
        self.assertEqual(events[-1].pnl, D("400"))
        self.assertFalse(broker.pending)
        self.assertFalse(broker.positions)

    def test_requires_minute_data_and_strict_chronology(self):
        broker = PaperBroker()
        with self.assertRaises(ValueError):
            broker.on_bar(replace(bar(), duration=timedelta(minutes=5)))
        broker.on_bar(bar())
        with self.assertRaises(ValueError):
            broker.on_bar(bar())
        with self.assertRaises(ValueError):
            broker.on_bar(bar(-1, symbol="MSFT"))
        self.assertEqual(
            broker.submit(signal(created_at=BASE - timedelta(minutes=1))).reason, "stale_signal"
        )

    def test_state_round_trip_keeps_open_brackets_pending_orders_and_deduplication(self):
        broker = PaperBroker(execution=ExecutionConfig(allow_shorts=True))
        broker.submit(signal())
        broker.submit(signal("MSFT", direction=Direction.SHORT))
        broker.on_bar(bar())
        encoded = json.loads(json.dumps(broker.to_dict()))
        restored = PaperBroker.from_dict(encoded)
        self.assertEqual(restored.to_dict(), encoded)
        self.assertEqual(restored.submit(signal()).reason, "duplicate_signal")
        expected = broker.on_bar(bar(1, o="102", h="104", low="101", c="103"))
        actual = restored.on_bar(bar(1, o="102", h="104", low="101", c="103"))
        self.assertEqual(actual, expected)
        self.assertEqual(restored.equity, broker.equity)

    def test_audit_emits_json_safe_events(self):
        audit = []
        broker = PaperBroker(audit=audit.append)
        broker.submit(signal())
        broker.on_bar(bar(low="98"))
        self.assertEqual([event["status"] for event in audit], ["accepted", "filled", "closed"])
        json.dumps(audit, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
