"""Deterministic completed-candle liquidity sweep → BOS → FVG strategy.

Only an immediately preceding, complete regular session supplies daily levels.
A gap in the current session disables that symbol until the next session. Replay
missing history chronologically in a fresh engine to repair an interrupted feed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .config import StrategyConfig
from .models import Bar, Direction, SetupState, Signal, aware
from .sessions import Session, SessionCalendar


@dataclass
class Setup:
    direction: Direction
    sweep: Bar
    structure_level: Decimal
    window_end: datetime
    low: Decimal
    high: Decimal
    bars_since_sweep: int = 0
    bos: Bar | None = None
    before_bos: Bar | None = None


@dataclass
class SymbolState:
    phase: SetupState = SetupState.WAITING
    session: Session | None = None
    previous_high: Decimal | None = None
    previous_low: Decimal | None = None
    session_bars: list[Bar] = field(default_factory=list)
    history: list[Bar] = field(default_factory=list)
    swing_high: Decimal | None = None
    swing_low: Decimal | None = None
    setup: Setup | None = None
    signal: Signal | None = None
    attempts: int = 0
    halted: bool = False
    holding: bool = False
    last_bar: Bar | None = None


class StrategyEngine:
    def __init__(
        self,
        config: StrategyConfig | None = None,
        calendar: SessionCalendar | None = None,
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.config = config or StrategyConfig()
        self.calendar = calendar or SessionCalendar()
        self.on_event = on_event
        self.states: dict[str, SymbolState] = {}

    def state_for(self, symbol: str) -> SymbolState:
        return self.states.setdefault(symbol.upper(), SymbolState())

    def _emit(self, symbol: str, at: datetime, kind: str, **details: Any) -> None:
        if self.on_event is not None:
            self.on_event(
                {
                    "type": "strategy",
                    "kind": kind,
                    "symbol": symbol,
                    "at": at.isoformat(),
                    "status": self.state_for(symbol).phase.value,
                    **details,
                }
            )

    def _invalidate(
        self, symbol: str, state: SymbolState, at: datetime, reason: str, **details: Any
    ) -> None:
        state.phase = SetupState.INVALIDATED
        self._emit(
            symbol,
            at,
            "invalidated",
            reason=reason,
            signal_id=state.signal.id if state.signal else None,
            **details,
        )

    def set_position(self, symbol: str, exists: bool) -> None:
        state = self.state_for(symbol)
        state.holding = exists
        if exists:
            state.phase = SetupState.POSITION_OPEN
        elif state.phase is SetupState.POSITION_OPEN:
            state.phase = SetupState.COMPLETE

    def on_order_event(self, symbol: str, status: str, at: datetime | None = None) -> None:
        state = self.state_for(symbol)
        timestamp = at or (state.last_bar.end if state.last_bar else datetime.now(UTC))
        aware(timestamp, "order event timestamp")
        status = status.lower()
        if status == "filled":
            state.holding = True
            state.phase = SetupState.POSITION_OPEN
        elif status == "closed":
            state.holding = False
            state.phase = SetupState.COMPLETE
        elif status in {"cancelled", "rejected", "expired"}:
            if state.holding:
                return
            state.phase = SetupState.INVALIDATED
        elif status == "accepted":
            return
        else:
            raise ValueError(f"Unknown order status: {status}")
        self._emit(symbol.upper(), timestamp, "order_update", order_status=status)

    def _new_session(self, symbol: str, state: SymbolState, session: Session, first: Bar) -> None:
        prior = self.calendar.previous_session(session.label)
        previous_complete = bool(
            state.session
            and state.session.label == prior.label
            and state.session_bars
            and not state.halted
            and state.session_bars[0].start == prior.open
            and state.session_bars[-1].end == prior.close
            and len(state.session_bars) == (prior.close - prior.open) // timedelta(minutes=5)
        )
        if state.phase in {
            SetupState.LIQUIDITY_SWEPT,
            SetupState.BOS_CONFIRMED,
            SetupState.ENTRY_PENDING,
        }:
            self._invalidate(symbol, state, first.start, "session_ended")
        if previous_complete:
            state.previous_high = max(bar.high for bar in state.session_bars)
            state.previous_low = min(bar.low for bar in state.session_bars)
        else:
            state.previous_high = state.previous_low = None
            state.history.clear()
            state.swing_high = state.swing_low = None
        state.session = session
        state.session_bars = []
        state.setup = None
        state.signal = None
        state.attempts = 0
        state.halted = first.start != session.open
        state.phase = SetupState.POSITION_OPEN if state.holding else SetupState.WAITING
        self._emit(
            symbol,
            first.start,
            "session_started",
            previous_session=prior.label.isoformat(),
            pdh=str(state.previous_high) if state.previous_high is not None else None,
            pdl=str(state.previous_low) if state.previous_low is not None else None,
        )
        if state.halted:
            self._emit(symbol, first.start, "data_gap", reason="missing_session_open")
        if not previous_complete:
            self._emit(symbol, first.start, "warmup", reason="previous_session_incomplete")

    def _update_pivots(self, state: SymbolState, bar: Bar) -> None:
        state.history.append(bar)
        size = self.config.pivot_left + self.config.pivot_right + 1
        state.history[:] = state.history[-size:]
        if len(state.history) != size:
            return
        pivot = state.history[self.config.pivot_left]
        left = state.history[: self.config.pivot_left]
        right = state.history[self.config.pivot_left + 1 :]
        if all(pivot.high > item.high for item in left) and all(
            pivot.high >= item.high for item in right
        ):
            state.swing_high = pivot.high
        if all(pivot.low < item.low for item in left) and all(
            pivot.low <= item.low for item in right
        ):
            state.swing_low = pivot.low

    def on_bar(self, bar: Bar, now: datetime | None = None) -> Signal | None:
        """Consume one chronological, completed RTH five-minute candle.

        Identical replayed bars are ignored. Revisions or older bars raise so a
        caller cannot silently trade an inconsistent history. Order fills should
        be delivered before this method for the same completed candle.
        """
        if bar.duration != timedelta(minutes=5):
            raise ValueError("Strategy requires completed five-minute bars")
        timestamp = now or datetime.now(UTC)
        aware(timestamp, "now")
        if bar.end > timestamp:
            raise ValueError("Cannot consume an incomplete/future candle")
        if not self.calendar.is_regular_bar(bar):
            raise ValueError("Bar must be aligned to a regular exchange session")
        state = self.state_for(bar.symbol)
        if state.last_bar is not None and bar.start <= state.last_bar.start:
            if bar == state.last_bar:
                return None
            raise ValueError(
                "Bars must be chronological; rebuild engine after a historical revision"
            )
        session = self.calendar.session_for(bar.start)
        assert session is not None
        if state.session is None or session.label != state.session.label:
            self._new_session(bar.symbol, state, session, bar)
        elif state.last_bar is not None and bar.start != state.last_bar.end:
            state.halted = True
            state.history.clear()
            state.swing_high = state.swing_low = None
            self._invalidate(bar.symbol, state, bar.start, "missing_candle")
        previous_bar = state.last_bar
        state.session_bars.append(bar)
        state.last_bar = bar
        signal = None
        if not state.halted and not state.holding:
            signal = self._process(state, bar, previous_bar)
        # Confirm pivots only AFTER processing the current candle. A pivot whose
        # last right-hand candle is the sweep did not exist before that sweep.
        if not state.halted:
            self._update_pivots(state, bar)
        return signal

    def _process(self, state: SymbolState, bar: Bar, previous_bar: Bar | None) -> Signal | None:
        if state.previous_high is None or state.previous_low is None:
            return None
        window = self.calendar.setup_window(bar.end, self.config)
        if state.setup is not None and state.phase in {
            SetupState.LIQUIDITY_SWEPT,
            SetupState.BOS_CONFIRMED,
            SetupState.ENTRY_PENDING,
        }:
            if window is None or bar.end >= state.setup.window_end:
                self._invalidate(bar.symbol, state, bar.end, "window_ended")
                return None
            if state.setup.direction is Direction.LONG and bar.high >= state.previous_high:
                self._invalidate(bar.symbol, state, bar.end, "target_touched_before_entry")
                return None
            if state.setup.direction is Direction.SHORT and bar.low <= state.previous_low:
                self._invalidate(bar.symbol, state, bar.end, "target_touched_before_entry")
                return None
            if state.phase is SetupState.ENTRY_PENDING:
                assert state.signal is not None
                if (state.signal.direction is Direction.LONG and bar.low <= state.signal.stop) or (
                    state.signal.direction is Direction.SHORT and bar.high >= state.signal.stop
                ):
                    self._invalidate(bar.symbol, state, bar.end, "stop_touched_before_entry")
                elif (
                    state.signal.direction is Direction.LONG and bar.close < state.signal.fvg_low
                ) or (
                    state.signal.direction is Direction.SHORT and bar.close > state.signal.fvg_high
                ):
                    self._invalidate(bar.symbol, state, bar.end, "fvg_invalidated")
                return None
        if window is None:
            return None
        if state.phase in {SetupState.INVALIDATED, SetupState.COMPLETE}:
            if state.attempts >= self.config.max_setups_per_symbol_per_session:
                return None
            state.phase = SetupState.WAITING
            state.setup = None
            state.signal = None
        if state.phase is SetupState.WAITING:
            if state.attempts >= self.config.max_setups_per_symbol_per_session:
                return None
            low_sweep, high_sweep = bar.low < state.previous_low, bar.high > state.previous_high
            if low_sweep and high_sweep:
                self._emit(bar.symbol, bar.end, "rejected", reason="ambiguous_dual_sweep")
                return None
            if not (low_sweep or high_sweep):
                return None
            direction = Direction.LONG if low_sweep else Direction.SHORT
            structure = state.swing_high if low_sweep else state.swing_low
            state.attempts += 1
            if structure is None:
                self._invalidate(bar.symbol, state, bar.end, "no_pre_sweep_confirmed_pivot")
                return None
            # Even touching the opposite target on the sweep consumes the idea:
            # there is no untouched daily liquidity target left to trade toward.
            if (low_sweep and bar.high >= state.previous_high) or (
                high_sweep and bar.low <= state.previous_low
            ):
                self._invalidate(bar.symbol, state, bar.end, "target_touched_before_entry")
                return None
            state.setup = Setup(direction, bar, structure, window[1], bar.low, bar.high)
            state.phase = SetupState.LIQUIDITY_SWEPT
            self._emit(
                bar.symbol,
                bar.end,
                "sweep",
                direction=direction.value,
                structure_level=str(structure),
                sweep_price=str(bar.low if low_sweep else bar.high),
            )
            return None
        setup = state.setup
        if setup is None:
            return None
        setup.low = min(setup.low, bar.low)
        setup.high = max(setup.high, bar.high)
        if state.phase is SetupState.LIQUIDITY_SWEPT:
            setup.bars_since_sweep += 1
            if setup.bars_since_sweep > self.config.max_bars_sweep_to_bos:
                self._invalidate(bar.symbol, state, bar.end, "bos_timed_out")
                return None
            bos = (
                bar.close > setup.structure_level
                if setup.direction is Direction.LONG
                else bar.close < setup.structure_level
            )
            if bos:
                setup.bos, setup.before_bos = bar, previous_bar
                state.phase = SetupState.BOS_CONFIRMED
                self._emit(
                    bar.symbol,
                    bar.end,
                    "bos",
                    bos_close=str(bar.close),
                    structure_level=str(setup.structure_level),
                )
            elif setup.bars_since_sweep == self.config.max_bars_sweep_to_bos:
                self._invalidate(bar.symbol, state, bar.end, "bos_timed_out")
            return None
        if state.phase is SetupState.BOS_CONFIRMED:
            return self._confirm_fvg(state, bar)
        return None

    def _confirm_fvg(self, state: SymbolState, bar: Bar) -> Signal | None:
        setup = state.setup
        assert setup is not None and setup.bos is not None and setup.before_bos is not None
        bullish = setup.direction is Direction.LONG
        a = setup.before_bos
        valid = bar.low > a.high if bullish else bar.high < a.low
        if not valid:
            self._invalidate(bar.symbol, state, bar.end, "bos_did_not_create_fvg")
            return None
        low, high = (a.high, bar.low) if bullish else (bar.high, a.low)
        if self.config.entry_mode == "midpoint":
            entry = (low + high) / 2
        elif self.config.entry_mode == "deep_edge":
            entry = low if bullish else high
        else:
            entry = high if bullish else low
        tick = self.config.tick_size
        # Buy limits round down and sell limits up to avoid worsening the chosen
        # entry. Stops round away from price; targets round toward the entry.
        entry = (entry / tick).to_integral_value(
            rounding=ROUND_FLOOR if bullish else ROUND_CEILING
        ) * tick
        buffer = tick * self.config.stop_buffer_ticks
        stop = setup.low - buffer if bullish else setup.high + buffer
        stop = (stop / tick).to_integral_value(
            rounding=ROUND_FLOOR if bullish else ROUND_CEILING
        ) * tick
        raw_target = state.previous_high if bullish else state.previous_low
        assert raw_target is not None
        target = (raw_target / tick).to_integral_value(
            rounding=ROUND_FLOOR if bullish else ROUND_CEILING
        ) * tick
        risk = entry - stop if bullish else stop - entry
        reward = target - entry if bullish else entry - target
        if not low <= entry <= high or stop <= 0 or target <= 0 or risk <= 0 or reward <= 0:
            self._invalidate(bar.symbol, state, bar.end, "invalid_price_geometry")
            return None
        rr = reward / risk
        if rr < self.config.minimum_rr:
            self._invalidate(
                bar.symbol,
                state,
                bar.end,
                "insufficient_reward_risk",
                entry=str(entry),
                stop=str(stop),
                target=str(target),
                rr=str(rr),
                minimum_rr=str(self.config.minimum_rr),
            )
            return None
        # Persisted identity seed: keep stable across project/package renames so
        # journal replay recognizes existing signals and their broker orders.
        identifier = str(
            uuid5(
                NAMESPACE_URL,
                f"liquidity:{bar.symbol}:{setup.sweep.start.isoformat()}:{setup.direction.value}:{bar.end.isoformat()}:{entry}:{stop}:{target}",
            )
        )
        signal = Signal(
            id=identifier,
            symbol=bar.symbol,
            direction=setup.direction,
            entry=entry,
            stop=stop,
            target=target,
            fvg_low=low,
            fvg_high=high,
            created_at=bar.end,
            expires_at=setup.window_end,
            metadata={
                "session": state.session.label.isoformat() if state.session else None,
                "pdh": str(state.previous_high),
                "pdl": str(state.previous_low),
                "sweep_at": setup.sweep.start.isoformat(),
                "sweep_price": str(setup.sweep.low if bullish else setup.sweep.high),
                "structure_level": str(setup.structure_level),
                "bos_at": setup.bos.start.isoformat(),
                "bos_close": str(setup.bos.close),
                "rr": str(rr),
                "entry_mode": self.config.entry_mode,
            },
        )
        state.signal = signal
        state.phase = SetupState.ENTRY_PENDING
        self._emit(
            bar.symbol,
            bar.end,
            "signal",
            signal_id=identifier,
            direction=setup.direction.value,
            entry=str(entry),
            stop=str(stop),
            target=str(target),
            fvg_low=str(low),
            fvg_high=str(high),
            rr=str(rr),
            expires_at=setup.window_end.isoformat(),
            **{key: value for key, value in signal.metadata.items() if key != "rr"},
        )
        return signal
