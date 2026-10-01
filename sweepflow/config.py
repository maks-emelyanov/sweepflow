"""Strict TOML configuration for the strategy and paper execution."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, fields
from decimal import Decimal
from pathlib import Path
from typing import Any

from .models import decimal


@dataclass(frozen=True)
class StrategyConfig:
    pivot_left: int = 2
    pivot_right: int = 2
    max_bars_sweep_to_bos: int = 3
    entry_mode: str = "first_touch"
    tick_size: Decimal = Decimal("0.01")
    stop_buffer_ticks: int = 1
    minimum_rr: Decimal = Decimal("2.5")
    max_setups_per_symbol_per_session: int = 1
    opening_start_minutes: int = 0
    opening_end_minutes: int = 60
    closing_start_minutes: int = 60
    closing_end_minutes: int = 5

    def __post_init__(self) -> None:
        for name in ("tick_size", "minimum_rr"):
            object.__setattr__(self, name, decimal(getattr(self, name)))
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        for name in (
            "pivot_left",
            "pivot_right",
            "max_bars_sweep_to_bos",
            "stop_buffer_ticks",
            "max_setups_per_symbol_per_session",
        ):
            if (
                isinstance(getattr(self, name), bool)
                or not isinstance(getattr(self, name), int)
                or getattr(self, name) < 1
            ):
                raise ValueError(f"{name} must be a positive integer")
        for name in (
            "opening_start_minutes",
            "opening_end_minutes",
            "closing_start_minutes",
            "closing_end_minutes",
        ):
            if (
                isinstance(getattr(self, name), bool)
                or not isinstance(getattr(self, name), int)
                or getattr(self, name) < 0
            ):
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.opening_start_minutes >= self.opening_end_minutes:
            raise ValueError("Opening window start must precede end")
        if self.closing_end_minutes >= self.closing_start_minutes:
            raise ValueError("Closing start offset must exceed closing end offset")
        if self.entry_mode not in {"first_touch", "midpoint", "deep_edge"}:
            raise ValueError("entry_mode must be first_touch, midpoint, or deep_edge")


@dataclass(frozen=True)
class RiskConfig:
    account_equity: Decimal = Decimal("100000")
    risk_per_trade: Decimal = Decimal("0.0025")
    max_daily_loss: Decimal = Decimal("0.01")
    max_concurrent_positions: int = 4
    max_symbol_notional: Decimal = Decimal("0.20")
    max_total_notional: Decimal = Decimal("0.80")

    def __post_init__(self) -> None:
        for name in (
            "account_equity",
            "risk_per_trade",
            "max_daily_loss",
            "max_symbol_notional",
            "max_total_notional",
        ):
            object.__setattr__(self, name, decimal(getattr(self, name)))
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        for name in (
            "risk_per_trade",
            "max_daily_loss",
            "max_symbol_notional",
            "max_total_notional",
        ):
            if getattr(self, name) > 1:
                raise ValueError(f"{name} must not exceed 1")
        if self.max_symbol_notional > self.max_total_notional:
            raise ValueError("Symbol notional limit must not exceed total notional limit")
        if (
            isinstance(self.max_concurrent_positions, bool)
            or not isinstance(self.max_concurrent_positions, int)
            or self.max_concurrent_positions < 1
        ):
            raise ValueError("max_concurrent_positions must be a positive integer")


@dataclass(frozen=True)
class ExecutionConfig:
    allow_shorts: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.allow_shorts, bool):
            raise ValueError("allow_shorts must be a boolean")


@dataclass(frozen=True)
class AppConfig:
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    symbols: tuple[str, ...] = ()
    universe: str = "sp500"


def _section[T](cls: type[T], values: Any) -> T:
    if not isinstance(values, dict):
        raise ValueError(f"{cls.__name__} must be a TOML table")
    unknown = set(values) - {item.name for item in fields(cls)}
    if unknown:
        raise ValueError(f"Unknown {cls.__name__} settings: {', '.join(sorted(unknown))}")
    return cls(**values)


def load_config(path: str | Path | None = None) -> AppConfig:
    if path is None:
        return AppConfig()
    with Path(path).open("rb") as handle:
        raw = tomllib.load(handle, parse_float=Decimal)
    unknown = set(raw) - {"strategy", "risk", "execution", "symbols", "universe"}
    if unknown:
        raise ValueError(f"Unknown configuration settings: {', '.join(sorted(unknown))}")
    symbols = raw.get("symbols", [])
    if not isinstance(symbols, list) or not all(
        isinstance(symbol, str) and symbol.strip() for symbol in symbols
    ):
        raise ValueError("symbols must be an array of nonempty strings")
    universe = raw.get("universe", "sp500")
    if universe not in {"sp500", "etfs", "custom"}:
        raise ValueError("universe must be sp500, etfs, or custom")
    return AppConfig(
        strategy=_section(StrategyConfig, raw.get("strategy", {})),
        risk=_section(RiskConfig, raw.get("risk", {})),
        execution=_section(ExecutionConfig, raw.get("execution", {})),
        symbols=tuple(dict.fromkeys(symbol.upper().strip() for symbol in symbols)),
        universe=universe,
    )
