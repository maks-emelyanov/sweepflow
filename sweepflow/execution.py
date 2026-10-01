"""Deterministic, long/short paper brackets; this module never sends live orders.

Execution needs completed one-minute bars. Ambiguous entry/stop minutes count a
fill and a stop, and an existing position touching both exits stops out. A limit
entry always fills at its limit (no favorable gap improvement); a stop gapping
through its trigger fills at the worse opening price. An ambiguous target touch
on the entry minute earns no profit unless the opening price already filled the
entry. These deliberately conservative conventions cannot reconstruct ticks.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

from .config import ExecutionConfig, RiskConfig
from .models import Bar, Direction, Signal

ZERO = Decimal("0")
NY = ZoneInfo("America/New_York")


def _aware(at: datetime) -> None:
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError("Execution timestamps must be timezone-aware")


def _json(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Nonfinite decimal in broker state")
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json(item) for item in value]
    if value is None or isinstance(value, (str, int, bool, float)):
        return value
    raise TypeError(f"Cannot serialize broker state value {type(value).__name__}")


def _signal_data(signal: Signal) -> dict[str, Any]:
    return _json(asdict(signal))


def _signal_from_data(data: Mapping[str, Any]) -> Signal:
    values = dict(data)
    values["direction"] = Direction(values["direction"])
    for key in ("entry", "stop", "target", "fvg_low", "fvg_high"):
        values[key] = Decimal(values[key])
    for key in ("created_at", "expires_at"):
        values[key] = datetime.fromisoformat(values[key])
    return Signal(**values)


@dataclass(frozen=True)
class BrokerEvent:
    status: str
    symbol: str
    signal_id: str
    at: datetime
    quantity: int = 0
    price: Decimal | None = None
    pnl: Decimal = ZERO
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return _json(asdict(self))


@dataclass(frozen=True)
class PendingOrder:
    signal: Signal
    quantity: int
    submitted_at: datetime


@dataclass
class Position:
    signal: Signal
    quantity: int
    entry_price: Decimal
    opened_at: datetime
    mark_price: Decimal

    @property
    def unrealized_pnl(self) -> Decimal:
        change = self.mark_price - self.entry_price
        return change * self.quantity * (1 if self.signal.direction == Direction.LONG else -1)

    @property
    def remaining_risk(self) -> Decimal:
        distance = self.mark_price - self.signal.stop
        if self.signal.direction == Direction.SHORT:
            distance = -distance
        return max(ZERO, distance) * self.quantity


class PaperBroker:
    """Paper ledger with fixed-risk sizing, reserved buying power, and OCO exits.

    ``max_*_notional`` values are fractions of marked equity. Shorts reserve
    their full absolute notional, never increasing buying power. Daily loss is
    measured from session-opening equity using realized and marked P&L. The
    daily breaker cancels entries and flattens positions at their latest marks;
    it remains latched until a new session. Submit bars in chronological order
    across symbols. Call ``flatten_session`` at the exchange calendar's close.
    """

    def __init__(
        self,
        risk: RiskConfig | None = None,
        execution: ExecutionConfig | None = None,
        audit: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.risk = risk or RiskConfig()
        self.execution = execution or ExecutionConfig()
        self.initial_equity = self.risk.account_equity
        self.realized_pnl = ZERO
        self.pending: dict[str, PendingOrder] = {}
        self.positions: dict[str, Position] = {}
        self.halted = False
        self.session_date: date | None = None
        self.session_open_equity = self.initial_equity
        self._seen_ids: set[str] = set()
        self._last_bar_end: dict[str, datetime] = {}
        self._latest_bar_start: datetime | None = None
        self._audit = audit

    @property
    def equity(self) -> Decimal:
        return (
            self.initial_equity
            + self.realized_pnl
            + sum((position.unrealized_pnl for position in self.positions.values()), ZERO)
        )

    @property
    def daily_pnl(self) -> Decimal:
        return self.equity - self.session_open_equity

    @property
    def gross_exposure(self) -> Decimal:
        return sum((p.mark_price * p.quantity for p in self.positions.values()), ZERO)

    @property
    def pending_notional(self) -> Decimal:
        return sum((p.signal.entry * p.quantity for p in self.pending.values()), ZERO)

    @property
    def reserved_risk(self) -> Decimal:
        return sum((p.remaining_risk for p in self.positions.values()), ZERO) + sum(
            (p.signal.risk_per_share * p.quantity for p in self.pending.values()), ZERO
        )

    @property
    def buying_power(self) -> Decimal:
        return max(ZERO, self.equity - self.gross_exposure - self.pending_notional)

    def _emit(self, event: BrokerEvent) -> BrokerEvent:
        if self._audit:
            self._audit({"component": "paper_broker", **event.to_dict()})
        return event

    def start_session(self, session_date: date) -> None:
        if session_date == self.session_date:
            return
        if self.session_date is not None and session_date < self.session_date:
            raise ValueError("Sessions cannot move backwards")
        if self.positions or self.pending:
            raise ValueError("Flatten and cancel the prior session before starting another")
        self.session_date = session_date
        self.session_open_equity = self.equity
        self.halted = False

    def _ensure_session(self, at: datetime) -> None:
        _aware(at)
        self.start_session(at.astimezone(NY).date())

    def submit(self, signal: Signal, at: datetime | None = None) -> BrokerEvent:
        at = at or signal.created_at
        _aware(at)
        _aware(signal.created_at)
        _aware(signal.expires_at)
        self._ensure_session(at)

        def reject(reason: str) -> BrokerEvent:
            return self._emit(BrokerEvent("rejected", signal.symbol, signal.id, at, reason=reason))

        if signal.id in self._seen_ids:
            return reject("duplicate_signal")
        self._seen_ids.add(signal.id)
        if self.halted:
            return reject("daily_loss_limit")
        if at < signal.created_at or at >= signal.expires_at:
            return reject("signal_not_current")
        previous_end = self._last_bar_end.get(signal.symbol)
        if previous_end is not None and signal.created_at < previous_end:
            return reject("stale_signal")
        if signal.direction == Direction.SHORT and not self.execution.allow_shorts:
            return reject("shorts_disabled")
        if signal.symbol in self.pending or signal.symbol in self.positions:
            return reject("symbol_already_active")
        if len(self.pending) + len(self.positions) >= self.risk.max_concurrent_positions:
            return reject("position_limit")
        prices = (signal.entry, signal.stop, signal.target, signal.fvg_low, signal.fvg_high)
        if any(not p.is_finite() or p <= ZERO for p in prices):
            return reject("invalid_prices")
        if not signal.fvg_low <= signal.entry <= signal.fvg_high:
            return reject("entry_outside_fvg")
        if signal.direction == Direction.LONG:
            valid = signal.stop < signal.entry < signal.target
        elif signal.direction == Direction.SHORT:
            valid = signal.target < signal.entry < signal.stop
        else:
            valid = False
        if not valid:
            return reject("invalid_bracket")
        daily_remaining = (
            self.session_open_equity * self.risk.max_daily_loss
            + self.daily_pnl
            - self.reserved_risk
        )
        risk_budget = max(ZERO, min(self.equity * self.risk.risk_per_trade, daily_remaining))
        available_notional = max(
            ZERO,
            min(
                self.equity * self.risk.max_symbol_notional,
                self.equity * self.risk.max_total_notional
                - self.gross_exposure
                - self.pending_notional,
                self.buying_power,
            ),
        )
        shares = int(
            min(
                risk_budget / signal.risk_per_share,
                available_notional / signal.entry,
            ).to_integral_value(rounding=ROUND_FLOOR)
        )
        if shares < 1:
            return reject("insufficient_risk_or_buying_power")
        self.pending[signal.symbol] = PendingOrder(signal, shares, at)
        return self._emit(
            BrokerEvent("accepted", signal.symbol, signal.id, at, shares, signal.entry)
        )

    def cancel(self, symbol: str, at: datetime, reason: str = "cancelled") -> list[BrokerEvent]:
        _aware(at)
        order = self.pending.pop(symbol, None)
        if order is None:
            return []
        return [
            self._emit(
                BrokerEvent("cancelled", symbol, order.signal.id, at, order.quantity, reason=reason)
            )
        ]

    def _close(self, symbol: str, price: Decimal, at: datetime, reason: str) -> BrokerEvent:
        position = self.positions.pop(symbol)
        multiplier = 1 if position.signal.direction == Direction.LONG else -1
        pnl = (price - position.entry_price) * position.quantity * multiplier
        self.realized_pnl += pnl
        return self._emit(
            BrokerEvent(
                "closed", symbol, position.signal.id, at, position.quantity, price, pnl, reason
            )
        )

    def _exit(self, bar: Bar, *, allow_target: bool = True) -> list[BrokerEvent]:
        position = self.positions[bar.symbol]
        signal = position.signal
        if signal.direction == Direction.LONG:
            stop_hit = bar.low <= signal.stop
            target_hit = bar.high >= signal.target
            stop_price = min(signal.stop, bar.open)
        else:
            stop_hit = bar.high >= signal.stop
            target_hit = bar.low <= signal.target
            stop_price = max(signal.stop, bar.open)
        if stop_hit:
            return [self._close(bar.symbol, stop_price, bar.end, "stop_loss")]
        if target_hit and allow_target:
            return [self._close(bar.symbol, signal.target, bar.end, "take_profit")]
        position.mark_price = bar.close
        return []

    def _validate_bar(self, bar: Bar) -> None:
        if bar.duration != timedelta(minutes=1):
            raise ValueError("Paper execution requires one-minute bars")
        _aware(bar.start)
        if bar.start.second or bar.start.microsecond:
            raise ValueError("One-minute execution bars must start on a minute boundary")
        previous_end = self._last_bar_end.get(bar.symbol)
        if previous_end is not None and bar.start < previous_end:
            raise ValueError("Duplicate or out-of-order execution bar")
        if self._latest_bar_start is not None and bar.start < self._latest_bar_start:
            raise ValueError("Execution bars must be chronological across symbols")

    def on_bar(self, bar: Bar) -> list[BrokerEvent]:
        """Process one bar; use on_bars for synchronized multi-symbol marks."""
        return self._on_bar(bar, check_breaker=True)

    def on_bars(self, bars: list[Bar]) -> list[BrokerEvent]:
        """Process one minute across symbols, then check marked daily losses.

        Deferring the breaker until every current mark is available avoids a
        stale mark from one symbol incorrectly liquidating another. No signals
        may be submitted until this synchronous batch has returned.
        """
        if not bars:
            return []
        if len({bar.start for bar in bars}) != 1:
            raise ValueError("A batch must contain one common minute timestamp")
        if len({bar.symbol for bar in bars}) != len(bars):
            raise ValueError("A batch cannot contain duplicate symbols")
        for bar in bars:
            self._validate_bar(bar)
        events: list[BrokerEvent] = []
        for bar in sorted(bars, key=lambda item: item.symbol):
            events.extend(self._on_bar(bar, check_breaker=False))
        events.extend(self._check_daily_loss(bars[0].end))
        return events

    def _on_bar(self, bar: Bar, *, check_breaker: bool) -> list[BrokerEvent]:
        self._validate_bar(bar)
        self._ensure_session(bar.start)
        self._last_bar_end[bar.symbol] = bar.end
        self._latest_bar_start = bar.start
        events: list[BrokerEvent] = []
        if bar.symbol in self.positions:
            events.extend(self._exit(bar))
        elif bar.symbol in self.pending:
            order = self.pending[bar.symbol]
            signal = order.signal
            if bar.start >= signal.expires_at:
                events.extend(self.cancel(bar.symbol, bar.start, "expired"))
            elif bar.start >= max(signal.created_at, order.submitted_at):
                long = signal.direction == Direction.LONG
                target_at_open = bar.open >= signal.target if long else bar.open <= signal.target
                entry_hit = bar.low <= signal.entry if long else bar.high >= signal.entry
                target_hit = bar.high >= signal.target if long else bar.low <= signal.target
                # A target already reached at the open is known to precede entry.
                if target_at_open or (target_hit and not entry_hit):
                    events.extend(self.cancel(bar.symbol, bar.end, "target_before_entry"))
                elif entry_hit:
                    del self.pending[bar.symbol]
                    self.positions[bar.symbol] = Position(
                        signal, order.quantity, signal.entry, bar.end, bar.close
                    )
                    events.append(
                        self._emit(
                            BrokerEvent(
                                "filled",
                                bar.symbol,
                                signal.id,
                                bar.end,
                                order.quantity,
                                signal.entry,
                            )
                        )
                    )
                    marketable_at_open = (
                        bar.open <= signal.entry if long else bar.open >= signal.entry
                    )
                    events.extend(self._exit(bar, allow_target=marketable_at_open))
        if check_breaker:
            events.extend(self._check_daily_loss(bar.end))
        return events

    def _check_daily_loss(self, at: datetime) -> list[BrokerEvent]:
        events: list[BrokerEvent] = []
        if not self.halted and self.daily_pnl <= -(
            self.session_open_equity * self.risk.max_daily_loss
        ):
            self.halted = True
            events.append(
                self._emit(
                    BrokerEvent(
                        "circuit_breaker", "", "", at, pnl=self.daily_pnl, reason="daily_loss_limit"
                    )
                )
            )
            events.extend(self.flatten_session(at, reason="daily_loss_limit"))
        return events

    def flatten_session(
        self,
        at: datetime,
        prices: Mapping[str, Decimal] | None = None,
        *,
        reason: str = "session_end",
    ) -> list[BrokerEvent]:
        _aware(at)
        marks = {
            symbol: (prices or {}).get(symbol, p.mark_price) for symbol, p in self.positions.items()
        }
        if any(not price.is_finite() or price <= ZERO for price in marks.values()):
            raise ValueError("Flatten prices must be finite and positive")
        events: list[BrokerEvent] = []
        for symbol in list(self.pending):
            events.extend(self.cancel(symbol, at, reason))
        for symbol, price in marks.items():
            events.append(self._close(symbol, price, at, reason))
        return events

    def to_dict(self) -> dict[str, Any]:
        """JSON-compatible snapshot; pair it atomically with strategy/feed state."""
        return _json(
            {
                "version": 1,
                "risk": asdict(self.risk),
                "execution": asdict(self.execution),
                "initial_equity": self.initial_equity,
                "realized_pnl": self.realized_pnl,
                "session_date": self.session_date,
                "session_open_equity": self.session_open_equity,
                "halted": self.halted,
                "seen_ids": sorted(self._seen_ids),
                "last_bar_end": self._last_bar_end,
                "latest_bar_start": self._latest_bar_start,
                "pending": [
                    {
                        "signal": _signal_data(p.signal),
                        "quantity": p.quantity,
                        "submitted_at": p.submitted_at,
                    }
                    for p in self.pending.values()
                ],
                "positions": [
                    {
                        "signal": _signal_data(p.signal),
                        "quantity": p.quantity,
                        "entry_price": p.entry_price,
                        "opened_at": p.opened_at,
                        "mark_price": p.mark_price,
                    }
                    for p in self.positions.values()
                ],
            }
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        audit: Callable[[dict[str, Any]], None] | None = None,
    ) -> PaperBroker:
        if data.get("version") != 1:
            raise ValueError("Unsupported paper broker snapshot version")
        risk_data = dict(data["risk"])
        for key in (
            "account_equity",
            "risk_per_trade",
            "max_daily_loss",
            "max_symbol_notional",
            "max_total_notional",
        ):
            risk_data[key] = Decimal(risk_data[key])
        broker = cls(RiskConfig(**risk_data), ExecutionConfig(**data["execution"]), audit)
        broker.initial_equity = Decimal(data["initial_equity"])
        broker.realized_pnl = Decimal(data["realized_pnl"])
        broker.session_date = (
            date.fromisoformat(data["session_date"]) if data["session_date"] else None
        )
        broker.session_open_equity = Decimal(data["session_open_equity"])
        broker.halted = bool(data["halted"])
        broker._seen_ids = set(data["seen_ids"])
        broker._last_bar_end = {
            key: datetime.fromisoformat(value) for key, value in data["last_bar_end"].items()
        }
        broker._latest_bar_start = (
            datetime.fromisoformat(data["latest_bar_start"]) if data["latest_bar_start"] else None
        )
        for entry in data["pending"]:
            signal = _signal_from_data(entry["signal"])
            broker.pending[signal.symbol] = PendingOrder(
                signal, int(entry["quantity"]), datetime.fromisoformat(entry["submitted_at"])
            )
        for entry in data["positions"]:
            signal = _signal_from_data(entry["signal"])
            broker.positions[signal.symbol] = Position(
                signal,
                int(entry["quantity"]),
                Decimal(entry["entry_price"]),
                datetime.fromisoformat(entry["opened_at"]),
                Decimal(entry["mark_price"]),
            )
        if set(broker.pending) & set(broker.positions):
            raise ValueError("Snapshot contains both an order and a position for one symbol")
        return broker
