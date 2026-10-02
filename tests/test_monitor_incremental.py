"""Incremental scans must preserve correction, backfill, and freshness behavior."""

import asyncio
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

import pytest
from test_monitor import FakeSource, scenario

from sweepflow.monitor import scan
from sweepflow.storage import Journal
from sweepflow.strategy import StrategyEngine


def test_unchanged_history_is_not_replayed_but_pending_signal_remains_eligible():
    async def check():
        bars, config, session = scenario()
        now = session.open + timedelta(minutes=30, seconds=30)
        calls = []
        original = StrategyEngine.on_bar

        def counted(engine, bar, **kwargs):
            calls.append(bar)
            return original(engine, bar, **kwargs)

        with Journal(":memory:") as journal, patch.object(StrategyEngine, "on_bar", counted):
            first = await scan(FakeSource(bars), ["AAPL"], config, journal, clock=lambda: now)
            calls.clear()
            second = await scan(
                FakeSource(bars),
                ["AAPL"],
                config,
                journal,
                clock=lambda: now + timedelta(seconds=30),
            )
            assert calls == []
            assert second.daily_bars_fetched == 1
            assert second.revised_symbols == ()
            assert second.eligible_signals == first.new_signals
            stale = await scan(
                FakeSource(bars),
                ["AAPL"],
                config,
                journal,
                clock=lambda: now + timedelta(seconds=61),
            )
            assert stale.eligible_signals == ()
            assert stale.active_signal_ids == first.active_signal_ids

    asyncio.run(check())


def test_daily_revision_rebuilds_affected_symbol_and_keeps_other_pending_signal():
    async def check():
        bars, config, session = scenario()
        history = bars + [replace(bar, symbol="MSFT") for bar in bars]
        now = session.open + timedelta(minutes=30, seconds=30)
        source = FakeSource(history)
        calls = Counter()
        original = StrategyEngine.on_bar

        def counted(engine, bar, **kwargs):
            calls[bar.symbol] += 1
            return original(engine, bar, **kwargs)

        with Journal(":memory:") as journal, patch.object(StrategyEngine, "on_bar", counted):
            first = await scan(source, ["AAPL", "MSFT"], config, journal, clock=lambda: now)
            assert len(first.new_signals) == 2
            source.daily_bars = [
                replace(bar, high=Decimal("112"))
                if bar.symbol == "AAPL" and bar.start < session.open
                else bar
                for bar in source.daily_bars
            ]
            calls.clear()
            revised = await scan(source, ["AAPL", "MSFT"], config, journal, clock=lambda: now)
            assert calls == {"AAPL": len(bars)}
            signals = {item.symbol: item for item in revised.eligible_signals}
            assert signals["AAPL"].target == Decimal("112")
            assert signals["MSFT"] == next(s for s in first.new_signals if s.symbol == "MSFT")
            assert revised.revised_symbols == ("AAPL",)

    asyncio.run(check())


def test_failed_scan_discards_partially_advanced_engine_before_recovery():
    async def check():
        bars, config, session = scenario()
        now = session.open + timedelta(minutes=30, seconds=30)
        source = FakeSource(bars)
        with Journal(":memory:") as journal:
            first = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            source.bars = list(bars)
            source.bars[81] = replace(bars[81], low=Decimal("98"))
            with patch.object(StrategyEngine, "on_bar", side_effect=RuntimeError("interrupted")):
                with pytest.raises(RuntimeError, match="interrupted"):
                    await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            recovered = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            assert recovered.eligible_signals == ()
            source.bars = bars
            restored = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            assert restored.eligible_signals == first.new_signals
            assert restored.new_signals == ()

    asyncio.run(check())


def test_other_journal_connection_correction_invalidates_cached_state(tmp_path):
    async def check():
        bars, config, session = scenario()
        now = session.open + timedelta(minutes=30, seconds=30)
        path = tmp_path / "shared.sqlite"
        source = FakeSource(bars)
        with Journal(path) as journal:
            await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            revised = replace(source.daily_bars[0], high=Decimal("112"))
            with Journal(path) as writer:
                writer.store_daily_bars([revised], session.label)
            # A successful data request may omit a cached bar without deleting it.
            source.bars = bars[1:]
            source.daily_bars = []
            result = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            assert result.eligible_signals[0].target == Decimal("112")

    asyncio.run(check())


def test_daily_correction_committed_during_fetch_rebuilds_current_poll(tmp_path):
    async def check():
        bars, config, session = scenario()
        now = session.open + timedelta(minutes=30, seconds=30)
        path = tmp_path / "concurrent-daily.sqlite"
        source = FakeSource(bars)
        with Journal(path) as journal, Journal(path) as writer:
            first = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            original = first.new_signals[0]
            revised = replace(source.daily_bars[0], high=Decimal("112"))

            class ConcurrentSource(FakeSource):
                async def get_daily_bars(self, symbols, previous, *, now):
                    # The scan has already captured its persisted inputs. A
                    # separate writer commits a correction during the fetch.
                    writer.store_daily_bars([revised], session.label)
                    return {}

            corrected = await scan(
                ConcurrentSource([], daily_bars=[]),
                ["AAPL"],
                config,
                journal,
                clock=lambda: now + timedelta(seconds=15),
            )
            signal = corrected.eligible_signals[0]
            assert original.target == Decimal("110")
            assert signal.target == Decimal("112")
            assert signal.created_at == original.created_at
            assert signal.id != original.id
            assert corrected.new_signals == (signal,)
            assert corrected.daily_bars_fetched == 0
            assert corrected.missing_daily_symbols == ()
            assert journal.daily_bars_since() == [revised]

    asyncio.run(check())


@pytest.mark.parametrize("warm_cache", [False, True])
def test_concurrent_append_waits_for_later_cutoff_then_uses_persisted_candle(tmp_path, warm_cache):
    async def check():
        bars, config, session = scenario()
        current_time = session.open + timedelta(minutes=29, seconds=59)
        path = tmp_path / "concurrent-append.sqlite"
        with Journal(path) as journal, Journal(path) as writer:
            if warm_cache:
                await scan(
                    FakeSource(bars[:-1]),
                    ["AAPL"],
                    config,
                    journal,
                    clock=lambda: current_time,
                )

            class ConcurrentSource(FakeSource):
                async def get_bars(self, symbols, start, end, *, now):
                    nonlocal current_time
                    # This read retains its pre-confirmation cutoff while a
                    # later writer commits the newly completed candle.
                    current_time = session.open + timedelta(minutes=30, seconds=30)
                    writer.store_bars([bars[-1]], session.label)
                    return await super().get_bars(symbols, start, end, now=now)

            deferred = await scan(
                ConcurrentSource(bars),
                ["AAPL"],
                config,
                journal,
                clock=lambda: current_time,
            )
            assert deferred.status == "ok"
            assert deferred.new_signals == ()
            assert deferred.eligible_signals == ()
            assert bars[-1] in journal.bars_since()

            caught_up = await scan(
                FakeSource([], daily_bars=[]),
                ["AAPL"],
                config,
                journal,
                clock=lambda: current_time,
            )
            assert caught_up.status == "ok"
            assert caught_up.bars_fetched == 0
            assert len(caught_up.new_signals) == 1
            assert caught_up.new_signals[0].created_at == bars[-1].end
            assert caught_up.eligible_signals == caught_up.new_signals

    asyncio.run(check())


def test_repaired_history_provenance_revalidates_prices_after_restart(tmp_path):
    async def check():
        bars, config, session = scenario()
        now = session.open + timedelta(minutes=30, seconds=30)
        repaired_key = (bars[0].symbol, bars[0].start)

        class RepairedSource(FakeSource):
            async def get_bars_with_repair(self, symbols, start, end, *, now, known, repaired):
                self.received_provenance = set(repaired)
                self.last_repaired_keys = {repaired_key}
                self.last_repaired_bars = 1
                return await self.get_bars(symbols, start, end, now=now)

        path = tmp_path / "minute-repaired.sqlite"
        source = RepairedSource(bars)
        with Journal(path) as journal:
            first = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            assert first.new_signals[0].target == Decimal("110")
            assert first.repaired_bars == 1
            assert journal.repaired_bars_since() == {repaired_key}
        source.bars = [replace(bars[0], high=Decimal("112")), *bars[1:]]
        with Journal(path) as journal:
            corrected = await scan(source, ["AAPL"], config, journal, clock=lambda: now)
            assert source.received_provenance == {repaired_key}
            assert corrected.eligible_signals[0].target == Decimal("110")
            assert corrected.eligible_signals == first.new_signals
            assert corrected.revised_symbols == ("AAPL",)
            assert journal.repaired_bars_since() == {repaired_key}

    asyncio.run(check())
