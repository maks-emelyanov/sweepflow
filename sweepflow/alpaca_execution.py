"""Durable, strategy-owned Alpaca paper brackets and reconciliation.

Every POST has a committed client-ID intent first. Unknown submissions are
looked up, never blindly retried. Closing requires confirmed cancellation and
a fresh position matching journal-owned fills. No account-wide mutations.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import ROUND_FLOOR, Decimal
from typing import Any
from zoneinfo import ZoneInfo

from .alpaca import AlpacaAPIError
from .config import AppConfig
from .execution import _signal_data
from .models import Direction, Signal, aware, decimal
from .storage import Journal

ZERO = Decimal("0")
NY = ZoneInfo("America/New_York")
TERMINAL = {"filled", "canceled", "expired", "rejected"}
PROTECTIVE = {"new", "accepted", "pending_new", "partially_filled", "held"}


def _time(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    aware(result, "Alpaca timestamp")
    return result


def _qty(order: dict, field: str = "filled_qty") -> Decimal:
    return decimal(order.get(field) or "0")


def _position_qty(position: dict) -> Decimal:
    qty = abs(_qty(position, "qty"))
    return -qty if position.get("side") == "short" or _qty(position, "qty") < 0 else qty


def _orders(order: dict) -> list[dict]:
    return [order, *(order.get("legs") or [])]


def _client_id(kind: str, identity: str) -> str:
    # Persisted broker ID prefix; changing it would break order reconciliation.
    return f"liq-{kind}-" + hashlib.sha256(identity.encode()).hexdigest()[:32]


def _setup_identity(signal: dict) -> tuple[str, datetime]:
    # Prices, direction and confirmation can change when history is corrected.
    # Keep execution ownership attached to the original sweep across revisions.
    sweep_at = (signal.get("metadata") or {}).get("sweep_at") or signal["created_at"]
    return signal["symbol"], _time(sweep_at)


class AlpacaPaperBroker:
    """Reconcile real paper-account state before sizing strategy entries.

    The caller holds an account-wide process lock. Ambiguous position ownership
    stops management of that symbol and blocks all new entries.
    """

    def __init__(
        self,
        client: Any,
        config: AppConfig,
        journal: Journal,
        *,
        eod_seconds: int = 60,
        max_signal_age_seconds: int = 90,
    ) -> None:
        if eod_seconds < 0 or max_signal_age_seconds < 0:
            raise ValueError("Execution time limits must be nonnegative")
        self.client, self.config, self.journal = client, config, journal
        self.eod_seconds = eod_seconds
        self.max_signal_age_seconds = max_signal_age_seconds
        self.account_id: str | None = None
        self.state: dict = {}
        self.account: dict = {}
        self.clock: dict = {}
        self.positions: dict[str, dict] = {}
        self.open_orders: list[dict] = []
        self.blocked: list[str] = []

    def _save(self) -> None:
        assert self.account_id is not None
        self.journal.save_alpaca_state(self.account_id, self.state)

    def _event(self, status: str, **values: Any) -> dict:
        event = {"component": "alpaca_paper", "status": status, **values}
        self.journal.record(event)
        return event

    def _load(self) -> None:
        account_id = str(self.account.get("id") or "")
        if not account_id:
            raise ValueError("Alpaca account response is missing its ID")
        if self.account_id is not None and self.account_id != account_id:
            raise ValueError("Alpaca account changed during execution")
        if self.account_id is None:
            self.account_id = account_id
            self.state = self.journal.get_alpaca_state(account_id) or {"version": 1, "intents": {}}
            if self.state.get("version") != 1:
                raise ValueError("Unsupported Alpaca journal version")

    def _lookup(self, intent: dict) -> None:
        if intent.get("state") in {"completed", "rejected"}:
            return
        previous = intent.get("order")
        order = (
            self.client.get_order(previous["id"])
            if previous
            else self.client.get_order_by_client_id(intent["client_order_id"])
        )
        if order is None:
            intent["state"] = "unknown"
            return
        if order.get("client_order_id") != intent["client_order_id"]:
            raise ValueError("Alpaca returned an order with a different client ID")
        if order.get("symbol") != intent["payload"]["symbol"]:
            raise ValueError("Alpaca returned an order for a different symbol")
        intent.update(order=order, state="submitted")

    def _post(self, intent: dict) -> None:
        intent["state"] = "unknown"
        self._save()  # Commit BEFORE the network boundary, including crashes.
        try:
            order = self.client.submit_order(intent["payload"])
        except AlpacaAPIError as exc:
            found = self.client.get_order_by_client_id(intent["client_order_id"])
            if found is not None:
                intent.update(order=found, state="submitted")
            elif exc.status in {400, 401, 403, 404, 405, 422}:
                intent.update(state="rejected", rejection_status=exc.status)
            self._save()
            self._event(
                "submission_error",
                client_order_id=intent["client_order_id"],
                http_status=exc.status,
                reconciliation=intent["state"],
            )
            return
        intent.update(order=order, state="submitted")
        self._save()

    def _remaining(self, intent: dict) -> Decimal:
        order = intent.get("order") or {}
        return (
            _qty(order)
            - sum((_qty(leg) for leg in order.get("legs") or []), ZERO)
            + sum(
                (
                    _qty(close.get("order") or {})
                    * (1 if close["payload"]["side"] == intent["payload"]["side"] else -1)
                    for close in intent.get("closes", [])
                ),
                ZERO,
            )
        )

    def _owned_signed(self, intent: dict) -> Decimal:
        return self._remaining(intent) * (1 if intent["payload"]["side"] == "buy" else -1)

    def _known_order_ids(self) -> set[str]:
        return {
            order["id"]
            for intent in self.state["intents"].values()
            for item in [intent, *intent.get("closes", [])]
            if item.get("order")
            for order in _orders(item["order"])
        }

    def _protects(self, intent: dict) -> bool:
        order, qty = intent.get("order") or {}, self._remaining(intent)
        if order.get("status") != "filled" or qty <= 0:
            return False
        side = "sell" if intent["payload"]["side"] == "buy" else "buy"
        legs = [
            leg
            for leg in order.get("legs") or []
            if leg.get("status") in PROTECTIVE
            and leg.get("side") == side
            and _qty(leg, "qty") - _qty(leg) >= qty
        ]
        stop, target = decimal(intent["signal"]["stop"]), decimal(intent["signal"]["target"])
        return any(
            leg.get("type") == "stop" and decimal(leg.get("stop_price") or 0) == stop
            for leg in legs
        ) and any(
            leg.get("type") == "limit" and decimal(leg.get("limit_price") or 0) == target
            for leg in legs
        )

    def _cancel(self, order: dict) -> None:
        if order.get("status") in TERMINAL:
            return
        try:
            self.client.cancel_order(order["id"])
        except AlpacaAPIError as exc:
            if exc.status not in {404, 422}:
                raise
        # DELETE only requests cancellation. A fresh GET must confirm it.

    def _close(self, intent: dict, now: datetime) -> None:
        if intent.get("state") in {"unknown", "rejected", "completed"}:
            return
        if not self.clock.get("is_open"):
            # Cancel expired GTC entry remainders before the next opening; fully
            # filled parents retain their broker-held protective exits.
            order = intent.get("order") or {}
            if order.get("status") not in TERMINAL:
                self._cancel(order)
                self._lookup(intent)
                self._save()
            return
        closes = intent.get("closes", [])
        if any(c.get("state") == "unknown" for c in closes):
            return
        if any(
            (c.get("order") or {}).get("status") not in TERMINAL
            for c in closes
            if c.get("state") != "rejected"
        ):
            return
        order = intent.get("order") or {}
        if order.get("status") not in TERMINAL:
            # This is our entry remainder even if someone else acquired the
            # symbol. Cancel it before checking position ownership.
            self._cancel(order)
            self._lookup(intent)
            self._save()
            if (intent.get("order") or {}).get("status") not in TERMINAL:
                return
            self.positions = {p["symbol"]: p for p in self.client.get_positions()}
        symbol = intent["payload"]["symbol"]
        actual = _position_qty(self.positions[symbol]) if symbol in self.positions else ZERO
        if self._owned_signed(intent) != actual:
            return
        known = self._known_order_ids()
        if any(
            o.get("symbol") == symbol and o.get("id") not in known
            for root in self.open_orders
            for o in _orders(root)
        ):
            return
        for order in _orders(intent.get("order") or {}):
            self._cancel(order)
        self._lookup(intent)
        self._save()
        if any(o.get("status") not in TERMINAL for o in _orders(intent.get("order") or {})):
            return
        self.positions = {p["symbol"]: p for p in self.client.get_positions()}
        remaining, signed = self._remaining(intent), self._owned_signed(intent)
        actual = _position_qty(self.positions[symbol]) if symbol in self.positions else ZERO
        if signed != actual or remaining != remaining.to_integral_value():
            return
        if remaining == 0:
            intent["state"] = "completed"
            self._save()
            return
        self.clock = self.client.get_clock()
        if not self.clock.get("is_open"):
            return
        closes = intent.setdefault("closes", [])
        cid = _client_id("close", f"{intent['client_order_id']}:{len(closes)}")
        close = {
            "client_order_id": cid,
            "created_at": now.isoformat(),
            "payload": {
                "symbol": symbol,
                "qty": str(int(abs(remaining))),
                "side": "sell" if signed > 0 else "buy",
                "type": "market",
                "time_in_force": "day",
                "client_order_id": cid,
            },
        }
        closes.append(close)
        self._post(close)
        self._event(
            "flatten_submitted",
            symbol=symbol,
            reason=intent["close_reason"],
            client_order_id=cid,
            quantity=int(abs(remaining)),
            reconciliation=close["state"],
        )

    def sync(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(UTC)
        aware(now, "sync time")
        self.account = self.client.get_account()
        self._load()
        self.clock = self.client.get_clock()
        self.positions = {p["symbol"]: p for p in self.client.get_positions()}
        self.open_orders = self.client.get_open_orders()
        day, equity = now.astimezone(NY).date().isoformat(), decimal(self.account["equity"])
        if self.state.get("day") != day:
            baseline = decimal(self.account.get("last_equity") or equity)
            if baseline <= 0:
                raise ValueError("Alpaca daily equity baseline must be positive")
            self.state.update(day=day, baseline=str(baseline), halted=False)
        daily_pnl = equity - decimal(self.state["baseline"])
        if daily_pnl <= -decimal(self.state["baseline"]) * self.config.risk.max_daily_loss:
            self.state["halted"] = True
        self._save()
        eod = (
            bool(self.clock.get("is_open"))
            and (_time(self.clock["next_close"]) - now).total_seconds() <= self.eod_seconds
        )
        for intent in self.state["intents"].values():
            if intent.get("state") in {"completed", "rejected"}:
                continue
            self._lookup(intent)
            for close in intent.get("closes", []):
                self._lookup(close)
            if intent.get("state") in {"rejected", "completed", "unknown"}:
                continue
            order, remaining = intent.get("order") or {}, self._remaining(intent)
            past_session = (
                _time(intent["signal"]["created_at"]).astimezone(NY).date().isoformat() < day
            )
            if self.state["halted"]:
                intent["close_reason"] = "daily_loss_limit"
            elif eod or past_session:
                intent["close_reason"] = "session_end"
            elif 0 < _qty(order) < _qty(order, "qty"):
                intent["close_reason"] = "partial_entry"
            elif remaining < 0:
                intent["close_reason"] = "overfilled_exit"
            elif remaining > 0 and not self._protects(intent):
                intent["close_reason"] = "unprotected_position"
            elif (
                now >= _time(intent["signal"]["expires_at"]) and order.get("status") not in TERMINAL
            ):
                intent["close_reason"] = "entry_expired"
            closes_terminal = all(
                c.get("state") == "rejected"
                or (
                    c.get("state") != "unknown" and (c.get("order") or {}).get("status") in TERMINAL
                )
                for c in intent.get("closes", [])
            )
            if (
                remaining == 0
                and closes_terminal
                and all(o.get("status") in TERMINAL for o in _orders(order))
            ):
                intent["state"] = "completed"
            self._save()
            if intent.get("close_reason"):
                self._close(intent, now)
        self.positions = {p["symbol"]: p for p in self.client.get_positions()}
        self.open_orders = self.client.get_open_orders()
        self.blocked = self._blocked(eod)
        active = [
            i
            for i in self.state["intents"].values()
            if i.get("state") not in {"completed", "rejected"}
        ]
        snapshot = {
            "status": "synced",
            "account_id": self.account_id,
            "market_open": bool(self.clock.get("is_open")),
            "managed_symbols": sorted({i["payload"]["symbol"] for i in active}),
            "positions": len(self.positions),
            "orders": len(self.open_orders),
            "blocked": self.blocked,
            "equity": str(equity),
            "daily_pnl": str(daily_pnl),
            "halted": self.state["halted"],
            "buying_power": str(self.account["buying_power"]),
        }
        self.state["snapshot"] = {**snapshot, "at": now.isoformat()}
        self.state.update(
            account=self.account,
            positions=list(self.positions.values()),
            open_orders=self.open_orders,
        )
        self._save()
        return snapshot

    def _blocked(self, eod: bool) -> list[str]:
        reasons = []
        if not self.clock.get("is_open"):
            reasons.append("market_closed")
        if eod:
            reasons.append("session_end")
        if self.state["halted"]:
            reasons.append("daily_loss_limit")
        if self.account.get("status") != "ACTIVE" or any(
            self.account.get(key, False)
            for key in ("trading_blocked", "account_blocked", "trade_suspended_by_user")
        ):
            reasons.append("account_not_tradable")
        owned: dict[str, Decimal] = {}
        known = self._known_order_ids()
        for intent in self.state["intents"].values():
            if intent.get("state") in {"completed", "rejected"}:
                continue
            symbol = intent["payload"]["symbol"]
            owned[symbol] = owned.get(symbol, ZERO) + self._owned_signed(intent)
            if intent.get("state") == "unknown" or any(
                c.get("state") == "unknown" for c in intent.get("closes", [])
            ):
                reasons.append(f"unknown_submission:{symbol}")
            if intent.get("close_reason"):
                reasons.append(f"closing:{symbol}")
        for symbol in set(owned) | set(self.positions):
            actual = _position_qty(self.positions[symbol]) if symbol in self.positions else ZERO
            if owned.get(symbol, ZERO) != actual:
                reasons.append(f"unmanaged_or_mismatched_position:{symbol}")
        if any(o.get("id") not in known for root in self.open_orders for o in _orders(root)):
            reasons.append("unmanaged_open_orders")
        return sorted(set(reasons))

    def submit(self, signal: Signal, now: datetime | None = None) -> dict:
        real_time = now is None
        now = now or datetime.now(UTC)
        self.sync(now)
        if real_time:
            now = datetime.now(UTC)
        cid = _client_id("entry", signal.id)

        def reject(reason: str) -> dict:
            return self._event("rejected", symbol=signal.symbol, signal_id=signal.id, reason=reason)

        prior_versions = [
            intent
            for intent in self.state["intents"].values()
            if intent["signal"]["id"] == signal.id
        ]
        if prior_versions:
            if not all(
                intent.get("state") == "completed"
                and intent.get("close_reason") == "signal_invalidated"
                and (intent.get("order") or {}).get("status") == "canceled"
                and _qty(intent["order"]) == ZERO
                and all(order.get("status") in TERMINAL for order in _orders(intent["order"]))
                and not intent.get("closes")
                for intent in prior_versions
            ):
                return reject("duplicate_signal")
            # Corrections may restore an earlier geometry (A -> B -> A). Reuse
            # the setup only after every prior same-ID attempt is confirmed
            # canceled without fills, using a fresh durable broker client ID.
            cid = _client_id("entry", f"{signal.id}:revision:{len(prior_versions)}")
        if cid in self.state["intents"]:
            return reject("duplicate_signal")
        if self.blocked:
            return reject(",".join(self.blocked))
        filled = [
            intent["signal"]
            for intent in self.state["intents"].values()
            if _qty(intent.get("order") or {}) > ZERO
        ]
        setup_identity = _setup_identity(_signal_data(signal))
        if any(_setup_identity(previous) == setup_identity for previous in filled):
            # A revised ID cannot enter twice, even after the original position
            # has closed or a partial entry has been flattened and restarted.
            return reject("setup_already_filled")
        filled_setups = {
            _setup_identity(previous)
            for previous in filled
            if previous["symbol"] == signal.symbol
            and _time(previous["created_at"]).astimezone(NY).date()
            == signal.created_at.astimezone(NY).date()
        }
        if len(filled_setups) >= self.config.strategy.max_setups_per_symbol_per_session:
            # A revision can also move the sweep timestamp. Preserve the actual
            # per-session fill limit independently of reconstructed history.
            return reject("session_setup_limit")
        if now < signal.created_at or now >= signal.expires_at:
            return reject("signal_not_current")
        if (now - signal.created_at).total_seconds() > self.max_signal_age_seconds:
            return reject("stale_signal")
        if any(
            price != price.quantize(Decimal("0.01") if price >= 1 else Decimal("0.0001"))
            for price in (signal.entry, signal.stop, signal.target)
        ):
            return reject("unsupported_price_precision")
        if signal.risk_per_share < Decimal("0.01"):
            return reject("stop_too_close")
        if signal.reward_risk < self.config.strategy.minimum_rr:
            return reject("insufficient_reward_risk")
        short = signal.direction is Direction.SHORT
        if short and not self.config.execution.allow_shorts:
            return reject("shorts_disabled")
        asset = self.client.get_asset(signal.symbol)
        if asset.get("status") != "active" or not asset.get("tradable"):
            return reject("asset_not_tradable")
        if short and not all(
            (
                self.account.get("shorting_enabled"),
                asset.get("shortable"),
                asset.get("easy_to_borrow"),
                asset.get("marginable"),
            )
        ):
            return reject("asset_not_shortable")
        active = [
            i
            for i in self.state["intents"].values()
            if i.get("state") not in {"completed", "rejected"}
        ]
        symbols = set(self.positions) | {i["payload"]["symbol"] for i in active}
        if signal.symbol in symbols:
            return reject("symbol_already_active")
        if len(symbols) >= self.config.risk.max_concurrent_positions:
            return reject("position_limit")
        pending_notional, reserved_risk = ZERO, ZERO
        for intent in active:
            order = intent.get("order") or {}
            stop, entry = decimal(intent["signal"]["stop"]), decimal(intent["signal"]["entry"])
            pending = (
                max(ZERO, _qty(order, "qty") - _qty(order))
                if order.get("status") not in TERMINAL
                else ZERO
            )
            pending_notional += pending * entry
            reserved_risk += pending * abs(entry - stop)
            remaining = self._remaining(intent)
            if remaining > 0:
                mark = decimal(self.positions[intent["payload"]["symbol"]]["current_price"])
                distance = mark - stop if intent["payload"]["side"] == "buy" else stop - mark
                reserved_risk += max(ZERO, distance) * remaining
        equity = decimal(self.account["equity"])
        daily_remaining = (
            decimal(self.state["baseline"]) * self.config.risk.max_daily_loss
            + equity
            - decimal(self.state["baseline"])
            - reserved_risk
        )
        risk_budget = max(ZERO, min(equity * self.config.risk.risk_per_trade, daily_remaining))
        gross = sum((abs(decimal(p["market_value"])) for p in self.positions.values()), ZERO)
        # Buying power already reserves orders: subtracting pending again is conservative.
        notional = max(
            ZERO,
            min(
                equity * self.config.risk.max_symbol_notional,
                equity * self.config.risk.max_total_notional - gross - pending_notional,
                decimal(self.account["buying_power"]) - pending_notional,
            ),
        )
        quantity = int(
            min(risk_budget / signal.risk_per_share, notional / signal.entry).to_integral_value(
                rounding=ROUND_FLOOR
            )
        )
        if quantity < 1:
            return reject("insufficient_risk_or_buying_power")
        if real_time:
            self.clock = self.client.get_clock()
            now = datetime.now(UTC)
            if not self.clock.get("is_open"):
                return reject("market_closed")
            if (_time(self.clock["next_close"]) - now).total_seconds() <= self.eod_seconds:
                return reject("session_end")
            if (
                now >= signal.expires_at
                or (now - signal.created_at).total_seconds() > self.max_signal_age_seconds
            ):
                return reject("stale_signal")
        payload = {
            "symbol": signal.symbol,
            "qty": str(quantity),
            "side": "sell" if short else "buy",
            "type": "limit",
            "limit_price": str(signal.entry),
            "time_in_force": "gtc",
            "order_class": "bracket",
            "client_order_id": cid,
            "take_profit": {"limit_price": str(signal.target)},
            "stop_loss": {"stop_price": str(signal.stop)},
            "extended_hours": False,
        }
        intent = {
            "client_order_id": cid,
            "signal": _signal_data(signal),
            "payload": payload,
            "closes": [],
            "created_at": now.isoformat(),
        }
        self.state["intents"][cid] = intent
        self._post(intent)
        return self._event(
            intent["state"],
            symbol=signal.symbol,
            signal_id=signal.id,
            client_order_id=cid,
            quantity=quantity,
        )

    def invalidate_pending(self, active_ids: set[str], now: datetime | None = None) -> dict:
        now = now or datetime.now(UTC)
        self.sync(now)
        for intent in self.state["intents"].values():
            if intent.get("state") in {"completed", "rejected"}:
                continue
            order = intent.get("order") or {}
            if intent["signal"]["id"] not in active_ids and order.get("status") != "filled":
                intent["close_reason"] = "signal_invalidated"
        self._save()
        return self.sync(now)
