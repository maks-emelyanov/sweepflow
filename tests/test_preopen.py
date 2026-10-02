"""Preparation must warm history without treating it as an executable scan."""

import asyncio
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from test_monitor import FakeSource, daily_history, scenario

from sweepflow.config import AppConfig
from sweepflow.models import Bar, Signal
from sweepflow.monitor import PreparationResult, ScanResult, prepare_session, scan
from sweepflow.paper import run_paper
from sweepflow.sessions import SessionCalendar
from sweepflow.storage import Journal


def test_preparation_persists_previous_session_without_emitting_signals():
    async def check():
        bars, config, session = scenario()
        previous = SessionCalendar().previous_session(session.label)
        source = FakeSource(bars)
        now = session.open - timedelta(minutes=30)
        with Journal(":memory:") as journal:
            result = await prepare_session(source, ["AAPL"], config, journal, clock=lambda: now)
            assert result.status == "ready"
            assert result.symbols == 1
            assert result.bars_fetched == 78
            assert result.missing_symbols == ()
            assert result.daily_bars_fetched == 1
            assert result.missing_daily_symbols == ()
            assert source.calls == [(("AAPL",), previous.open, previous.close, now)]
            assert source.daily_calls == [(("AAPL",), previous, now)]
            assert journal.bars_since() == bars[:78]
            assert journal.daily_bars_since() == daily_history(bars[:78])
            assert journal.connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 0
            assert not any(
                event.get("event") == "alpaca-paper_signal" for event in journal.events()
            )

    asyncio.run(check())


@pytest.mark.parametrize("when", ["at_open", "after_close", "holiday", "weekend"])
def test_preparation_outside_preopen_does_not_fetch(when):
    async def check():
        bars, config, session = scenario()
        now = {
            "at_open": session.open,
            "after_close": session.close + timedelta(minutes=1),
            "holiday": datetime(2025, 7, 4, 13, tzinfo=UTC),
            "weekend": datetime(2025, 7, 5, 13, tzinfo=UTC),
        }[when]
        source = FakeSource(bars)
        with Journal(":memory:") as journal:
            result = await prepare_session(source, ["AAPL"], config, journal, clock=lambda: now)
            assert result.status == "not_preopen"
            assert source.calls == []
            assert source.daily_calls == []
            assert journal.bars_since() == []
            assert journal.daily_bars_since() == []

    asyncio.run(check())


@pytest.mark.parametrize(
    "day", [date(2025, 3, 10), date(2025, 6, 2), date(2025, 7, 7), date(2025, 12, 1)]
)
def test_preparation_respects_prior_session_holidays_dst_and_early_closes(day):
    async def check():
        calendar = SessionCalendar()
        session = calendar.session(day)
        previous = calendar.previous_session(day)
        count = int((previous.close - previous.open) / timedelta(minutes=5))
        bars = [
            Bar(
                "AAPL",
                previous.open + timedelta(minutes=5 * index),
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
            )
            for index in range(count)
        ]
        source = FakeSource(bars)
        with Journal(":memory:") as journal:
            result = await prepare_session(
                source,
                ["AAPL"],
                AppConfig(),
                journal,
                clock=lambda: session.open - timedelta(minutes=30),
            )
            assert result.status == "ready"
            assert result.bars_fetched == count
            assert result.daily_bars_fetched == 1
            assert source.calls[0][1:3] == (previous.open, previous.close)
            assert source.daily_calls[0][1] == previous
            assert journal.daily_bars_since() == daily_history(bars)

    asyncio.run(check())


def test_preparation_reports_missing_history_and_repairs_it_on_retry():
    async def check():
        bars, config, session = scenario()
        source = FakeSource([bar for index, bar in enumerate(bars) if index != 30])
        now = session.open - timedelta(minutes=30)
        with Journal(":memory:") as journal:
            incomplete = await prepare_session(
                source, ["AAPL", "MSFT"], config, journal, clock=lambda: now
            )
            assert incomplete.status == "incomplete"
            assert incomplete.missing_symbols == ("AAPL", "MSFT")
            assert incomplete.missing_daily_symbols == ("MSFT",)
            source.bars = bars + [replace(bar, symbol="MSFT") for bar in bars]
            source.daily_bars = daily_history(source.bars)
            repaired = await prepare_session(
                source, ["AAPL", "MSFT"], config, journal, clock=lambda: now
            )
            assert repaired.status == "ready"
            assert repaired.missing_symbols == ()
            assert repaired.missing_daily_symbols == ()
            assert repaired.quarantined == ()

    asyncio.run(check())


def test_incomplete_preparation_allows_fresh_current_session_signal():
    async def check():
        bars, config, session = scenario()
        source = FakeSource([bar for index, bar in enumerate(bars) if index != 30])
        with Journal(":memory:") as journal:
            prepared = await prepare_session(
                source,
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open - timedelta(minutes=30),
            )
            assert prepared.status == "incomplete"
            assert prepared.missing_symbols == ("AAPL",)
            assert prepared.bars_fetched == 77
            assert prepared.daily_bars_fetched == 1
            assert prepared.missing_daily_symbols == ()
            assert journal.connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 0
            result = await scan(
                source,
                ["AAPL"],
                config,
                journal,
                mode="alpaca-paper",
                clock=lambda: session.open + timedelta(minutes=30, seconds=30),
            )
            assert len(result.new_signals) == 1
            signal = result.new_signals[0]
            assert signal.target == Decimal("110")
            assert signal.metadata["previous_session_complete"] is False
            assert signal.metadata["previous_session_bars"] == 77
            assert signal.metadata["previous_session_expected_bars"] == 78
            assert signal.metadata["previous_session_level_source"] == "daily"
            assert result.quarantined == ()
            assert result.stale_symbols == ()

    asyncio.run(check())


def test_preparation_refreshes_revised_daily_levels_without_quarantine_or_signals():
    async def check():
        bars, config, session = scenario()
        previous = SessionCalendar().previous_session(session.label)
        with Journal(":memory:") as journal:
            journal.store_bars(bars[:78], previous.label)
            journal.store_daily_bars(daily_history(bars[:78]), session.label)
            revised_daily = [replace(bar, high=Decimal("112")) for bar in daily_history(bars[:78])]
            source = FakeSource(bars, daily_bars=revised_daily)
            result = await prepare_session(
                source,
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open - timedelta(minutes=30),
            )
            assert result.status == "ready"
            assert result.quarantined == ()
            assert result.revised_symbols == ("AAPL",)
            assert source.calls[0][1:3] == (previous.open, previous.close)
            assert journal.bars_since() == bars[:78]
            assert journal.daily_bars_since() == revised_daily
            assert journal.quarantined(session.label) == set()
            assert journal.quarantined(previous.label) == set()
            assert journal.connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 0
            assert journal.events()[0]["event"] == "data_revision"
            assert journal.events()[0]["timeframe"] == "day"
            assert journal.events()[0]["session"] == session.label.isoformat()

    asyncio.run(check())


def test_preparation_reports_missing_daily_despite_complete_intraday():
    async def check():
        bars, config, session = scenario()
        source = FakeSource(bars, daily_bars=[])
        with Journal(":memory:") as journal:
            result = await prepare_session(
                source,
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open - timedelta(minutes=30),
            )
            assert result.status == "incomplete"
            assert result.missing_symbols == ()
            assert result.missing_daily_symbols == ("AAPL",)
            assert result.daily_bars_fetched == 0
            assert journal.daily_bars_since() == []
            assert journal.connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 0

    asyncio.run(check())


def test_prepared_daily_levels_survive_restart_without_previous_intraday(tmp_path):
    async def check():
        bars, config, session = scenario()
        source = FakeSource(bars)
        source.bars = []
        path = tmp_path / "prepared.sqlite"
        with Journal(path) as journal:
            result = await prepare_session(
                source,
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open - timedelta(minutes=30),
            )
            assert result.missing_symbols == ("AAPL",)
            assert result.missing_daily_symbols == ()
            assert journal.daily_bars_since() == daily_history(bars[:78])
        source.bars = bars[78:]
        source.daily_bars = []
        with Journal(path) as journal:
            scanned = await scan(
                source,
                ["AAPL"],
                config,
                journal,
                mode="alpaca-paper",
                clock=lambda: session.open + timedelta(minutes=30, seconds=30),
            )
            assert len(scanned.new_signals) == 1
            assert scanned.new_signals[0].target == Decimal("110")
            assert scanned.new_signals[0].metadata["previous_session_level_source"] == "daily"
            assert scanned.daily_bars_fetched == 0
            assert scanned.missing_daily_symbols == ()

    asyncio.run(check())


def test_preparation_restart_repairs_first_cached_gap():
    async def check():
        bars, config, session = scenario()
        source = FakeSource(bars)
        with Journal(":memory:") as journal:
            journal.store_bars(
                [bar for index, bar in enumerate(bars[:78]) if index != 30], session.label
            )
            result = await prepare_session(
                source,
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open - timedelta(minutes=30),
            )
            assert result.status == "ready"
            assert source.calls[0][1] == bars[0].start
            assert journal.bars_since() == bars[:78]

    asyncio.run(check())


def test_prepared_history_is_reused_at_open_and_older_pivots_are_retained():
    async def check():
        bars, config, session = scenario()
        calendar = SessionCalendar()
        previous = calendar.previous_session(session.label)
        older_session = calendar.previous_session(previous.label)
        older_bar = replace(bars[0], start=older_session.open)
        source = FakeSource(bars)
        with Journal(":memory:") as journal:
            journal.store_bars([older_bar], older_session.label)
            await prepare_session(
                source,
                ["AAPL"],
                config,
                journal,
                clock=lambda: session.open - timedelta(minutes=30),
            )
            assert older_bar in journal.bars_since()
            result = await scan(
                source,
                ["AAPL"],
                config,
                journal,
                mode="alpaca-paper",
                clock=lambda: session.open + timedelta(minutes=30, seconds=30),
            )
            assert source.calls[-1][1] == previous.open
            assert len(result.new_signals) == 1
            assert older_bar in journal.bars_since()

    asyncio.run(check())


class SourceContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


class Broker:
    def __init__(self, *, market_open=False):
        self.calls = 0
        self.market_open = market_open
        self.invalidations = []
        self.submissions = []

    def sync(self):
        self.calls += 1
        return {"market_open": self.market_open}

    def invalidate_pending(self, active_ids):
        self.invalidations.append(active_ids)

    def submit(self, signal):
        self.submissions.append(signal)
        return {"status": "accepted"}


async def wait_until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait(), timeout=2)


def test_paper_prepares_before_open_while_reconciling_then_starts_scanning(monkeypatch):
    async def check():
        _, config, session = scenario()
        now = session.open - timedelta(minutes=30)
        broker = Broker()
        preparations = []
        scans = []

        async def prepare(*args, **kwargs):
            preparations.append(now)
            await wait_until(lambda: broker.calls >= 3)
            return PreparationResult("ready", 1, 78)

        async def live_scan(*args, **kwargs):
            scans.append(now)
            return ScanResult("ok", 1, 0, ())

        monkeypatch.setattr("sweepflow.paper.prepare_session", prepare)
        monkeypatch.setattr("sweepflow.paper.scan", live_scan)
        with Journal(":memory:") as journal:
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    config,
                    journal,
                    poll_seconds=0.001,
                    scan_seconds=0.001,
                    clock=lambda: now,
                    source_factory=SourceContext,
                    emit=lambda _: None,
                )
            )
            try:
                await wait_until(lambda: broker.calls >= 6)
                assert len(preparations) == 1
                assert scans == []
                assert broker.submissions == []
                assert broker.invalidations == []
                now = session.open + timedelta(minutes=5)
                broker.market_open = True
                await wait_until(lambda: bool(scans))
                assert scans[0] >= session.open
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


def test_paper_retries_incomplete_and_failed_preparation_without_trading(monkeypatch):
    async def check():
        _, config, session = scenario()
        broker = Broker()
        attempts = []

        async def prepare(*args, **kwargs):
            attempts.append(broker.calls)
            if len(attempts) == 1:
                return PreparationResult("incomplete", 1, 77, missing_symbols=("AAPL",))
            if len(attempts) == 2:
                raise RuntimeError("historical feed unavailable")
            return PreparationResult("ready", 1, 78)

        async def live_scan(*args, **kwargs):
            pytest.fail("No live scan is allowed before the market opens")

        monkeypatch.setattr("sweepflow.paper.prepare_session", prepare)
        monkeypatch.setattr("sweepflow.paper.scan", live_scan)
        with Journal(":memory:") as journal:
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    config,
                    journal,
                    poll_seconds=0.001,
                    scan_seconds=0.001,
                    clock=lambda: session.open - timedelta(minutes=30),
                    source_factory=SourceContext,
                    emit=lambda _: None,
                )
            )
            try:
                await wait_until(lambda: len(attempts) >= 3)
                await wait_until(lambda: broker.calls >= attempts[-1] + 3)
                assert len(attempts) == 3
                assert broker.submissions == []
                assert broker.invalidations == []
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


def test_scan_finishing_after_market_close_does_not_submit(monkeypatch):
    async def check():
        broker = Broker(market_open=True)
        now = datetime.now(UTC)
        signal = Signal(
            "completed-at-close",
            "AAPL",
            "LONG",
            "100",
            "99",
            "110",
            "100",
            "101",
            now - timedelta(seconds=1),
            now + timedelta(minutes=20),
        )

        async def live_scan(*args, **kwargs):
            broker.market_open = False
            return ScanResult(
                "ok", 1, 1, (signal,), eligible_signals=(signal,), active_signal_ids=(signal.id,)
            )

        monkeypatch.setattr("sweepflow.paper.scan", live_scan)
        with Journal(":memory:") as journal:
            await asyncio.wait_for(
                run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    once=True,
                    poll_seconds=0.001,
                    source_factory=SourceContext,
                    clock=lambda: now,
                    emit=lambda _: None,
                ),
                timeout=2,
            )
        assert broker.submissions == []

    asyncio.run(check())


def test_preparation_finishing_after_open_does_not_become_a_trading_scan(monkeypatch):
    async def check():
        _, config, session = scenario()
        now = session.open - timedelta(seconds=1)
        broker = Broker()
        preparing = asyncio.Event()
        finish_preparation = asyncio.Event()
        scans = []

        async def prepare(*args, **kwargs):
            preparing.set()
            await finish_preparation.wait()
            return PreparationResult("ready", 1, 78)

        async def live_scan(*args, **kwargs):
            scans.append(now)
            return ScanResult("ok", 1, 0, ())

        monkeypatch.setattr("sweepflow.paper.prepare_session", prepare)
        monkeypatch.setattr("sweepflow.paper.scan", live_scan)
        with Journal(":memory:") as journal:
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    config,
                    journal,
                    poll_seconds=0.001,
                    scan_seconds=0.001,
                    clock=lambda: now,
                    source_factory=SourceContext,
                    emit=lambda _: None,
                )
            )
            try:
                await asyncio.wait_for(preparing.wait(), timeout=2)
                now = session.open
                broker.market_open = True
                syncs = broker.calls
                await wait_until(lambda: broker.calls > syncs + 1)
                assert broker.invalidations == []
                assert broker.submissions == []
                finish_preparation.set()
                await wait_until(lambda: bool(scans))
                assert scans[0] == session.open
                assert not any(
                    event.get("event") == "paper_execution_error" for event in journal.events()
                )
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())
