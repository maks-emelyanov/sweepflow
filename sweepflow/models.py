"""Shared, validated price and event types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any


def decimal(value: Decimal | str | int | float) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Prices and monetary amounts must be finite")
    return result


def aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must have a timezone")


class Direction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class SetupState(StrEnum):
    WAITING = "WAITING"
    LIQUIDITY_SWEPT = "LIQUIDITY_SWEPT"
    BOS_CONFIRMED = "BOS_CONFIRMED"
    ENTRY_PENDING = "ENTRY_PENDING"
    POSITION_OPEN = "POSITION_OPEN"
    COMPLETE = "COMPLETE"
    INVALIDATED = "INVALIDATED"


@dataclass(frozen=True)
class Bar:
    symbol: str
    start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = 0
    duration: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        aware(self.start, "Bar.start")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        if not self.symbol:
            raise ValueError("Bar.symbol is required")
        for name in ("open", "high", "low", "close"):
            object.__setattr__(self, name, decimal(getattr(self, name)))
        if (
            self.low <= 0
            or not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high
        ):
            raise ValueError("Bar prices must be positive and satisfy low <= open/close <= high")
        if isinstance(self.volume, bool) or not isinstance(self.volume, int) or self.volume < 0:
            raise ValueError("Bar.volume must be a nonnegative integer")
        if self.duration <= timedelta(0):
            raise ValueError("Bar.duration must be positive")

    @property
    def end(self) -> datetime:
        return self.start + self.duration


@dataclass(frozen=True)
class Signal:
    id: str
    symbol: str
    direction: Direction
    entry: Decimal
    stop: Decimal
    target: Decimal
    fvg_low: Decimal
    fvg_high: Decimal
    created_at: datetime
    expires_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        aware(self.created_at, "Signal.created_at")
        aware(self.expires_at, "Signal.expires_at")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "direction", Direction(self.direction))
        for name in ("entry", "stop", "target", "fvg_low", "fvg_high"):
            object.__setattr__(self, name, decimal(getattr(self, name)))
        if self.expires_at <= self.created_at:
            raise ValueError("Signal must expire after creation")
        if not self.fvg_low <= self.entry <= self.fvg_high:
            raise ValueError("Signal entry must lie inside its FVG")
        if self.direction is Direction.LONG and not 0 < self.stop < self.entry < self.target:
            raise ValueError("Long signal requires stop < entry < target")
        if self.direction is Direction.SHORT and not 0 < self.target < self.entry < self.stop:
            raise ValueError("Short signal requires target < entry < stop")

    @property
    def risk_per_share(self) -> Decimal:
        return abs(self.entry - self.stop)

    @property
    def reward_risk(self) -> Decimal:
        return abs(self.target - self.entry) / self.risk_per_share
