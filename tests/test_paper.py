import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from test_monitor import FakeSource, scenario

from sweepflow.alpaca import AlpacaAPIError
from sweepflow.cli import parser
from sweepflow.config import AppConfig
from sweepflow.models import Signal
from sweepflow.monitor import ScanResult, bind_monitor, scan
from sweepflow.paper import paper_account_lock, run_paper
from sweepflow.storage import Journal


def signal_now():
    now = datetime.now(UTC)
    return Signal(
        "paper-test",
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


class SourceContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


class Broker:
    def __init__(self, market_open=True):
        self.calls = 0
        self.invalidations = []
        self.submissions = []
        self.market_open = market_open

    def sync(self):
        self.calls += 1
        return {"market_open": self.market_open}

    def invalidate_pending(self, active_ids):
        self.invalidations.append(active_ids)

    def submit(self, signal):
        self.submissions.append(signal)
        return {"status": "accepted"}


def test_cli_exposes_paper_and_sync_without_live_endpoint_option():
    args = parser().parse_args(["paper", "--once", "--symbols", "AAPL", "--allow-shorts"])
    assert args.once and args.allow_shorts
    assert args.db == Path("data/alpaca-paper.sqlite")
    assert args.env_file == Path(".env")
    assert parser().parse_args(["alpaca-sync"]).command == "alpaca-sync"


def test_paper_account_lock_excludes_other_journals_for_same_account():
    with paper_account_lock("fake-test-account"):
        with pytest.raises(ValueError, match="Another"):
            with paper_account_lock("fake-test-account"):
                pass
        with paper_account_lock("another-fake-test-account"):
            pass
    with paper_account_lock("fake-test-account"):
        pass


def test_durable_alpaca_state_reopens_and_rejects_account_change(tmp_path):
    path = tmp_path / "paper.sqlite"
    state = {"intents": {"fixed-client-id": {"status": "unknown"}}, "halted": True}
    with Journal(path) as journal:
        journal.save_alpaca_state("account", state)
    with Journal(path) as journal:
        assert journal.get_alpaca_state("account") == state
        with pytest.raises(ValueError, match="different Alpaca"):
            journal.save_alpaca_state("different-account", {})
        assert journal.get_alpaca_state("different-account") is None


def test_restart_can_recover_signal_journaled_before_broker_submission():
    async def test():
        bars, config, session = scenario()
        now = session.open + timedelta(minutes=30, seconds=30)
        with Journal(":memory:") as journal:
            first = await scan(
                FakeSource(bars), ["AAPL"], config, journal, mode="alpaca-paper", clock=lambda: now
            )
            restarted = await scan(
                FakeSource(bars), ["AAPL"], config, journal, mode="alpaca-paper", clock=lambda: now
            )
            assert len(first.new_signals) == 1
            assert restarted.new_signals == ()
            assert restarted.eligible_signals == first.new_signals
            assert restarted.active_signal_ids == (first.new_signals[0].id,)

    asyncio.run(test())


def test_trailing_feed_gap_cancels_pending_but_age_alone_does_not():
    async def test():
        bars, config, session = scenario()
        with Journal(":memory:") as journal:
            old = await scan(
                FakeSource(bars),
                ["AAPL"],
                config,
                journal,
                mode="alpaca-paper",
                clock=lambda: session.open + timedelta(minutes=33),
            )
            assert old.new_signals == ()
            assert len(old.active_signal_ids) == 1
            stale = await scan(
                FakeSource(bars),
                ["AAPL"],
                config,
                journal,
                mode="alpaca-paper",
                clock=lambda: session.open + timedelta(minutes=40),
            )
            assert stale.active_signal_ids == ()
            assert stale.stale_symbols == ("AAPL",)

    asyncio.run(test())


def test_paper_universe_can_change_without_losing_broker_state():
    async def test():
        bars, config, session = scenario()
        with Journal(":memory:") as journal:
            journal.save_alpaca_state("fake", {"intent": "must-survive"})
            for symbols in (["AAPL"], ["AAPL", "MSFT"]):
                await scan(
                    FakeSource(bars),
                    symbols,
                    config,
                    journal,
                    mode="alpaca-paper",
                    clock=lambda: session.open + timedelta(minutes=30),
                )
            assert journal.get_alpaca_state("fake") == {"intent": "must-survive"}

    asyncio.run(test())


def test_once_keeps_submitted_bracket_and_uses_eligible_saved_signal():
    async def test():
        broker = Broker()
        signal = signal_now()

        async def fake_scan(*args, **kwargs):
            return ScanResult(
                "ok", 1, 1, (), eligible_signals=(signal,), active_signal_ids=(signal.id,)
            )

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", fake_scan):
            await run_paper(
                broker,
                ["AAPL"],
                AppConfig(),
                journal,
                once=True,
                source_factory=SourceContext,
                poll_seconds=0.001,
                emit=lambda _: None,
            )
        assert broker.submissions == [signal]
        assert broker.invalidations == [{signal.id}]

    asyncio.run(test())


def test_reconciliation_continues_while_market_data_scan_waits():
    async def test():
        broker = Broker()

        async def slow_scan(*args, **kwargs):
            while broker.calls < 3:
                await asyncio.sleep(0.001)
            return ScanResult("ok", 1, 0, ())

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", slow_scan):
            await asyncio.wait_for(
                run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    once=True,
                    source_factory=SourceContext,
                    poll_seconds=0.001,
                    emit=lambda _: None,
                ),
                timeout=1,
            )
        assert broker.calls >= 4

    asyncio.run(test())


def test_feed_failure_cancels_entries_before_error():
    async def test():
        broker = Broker()

        async def failed(*args, **kwargs):
            raise RuntimeError("feed unavailable")

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", failed):
            with pytest.raises(RuntimeError, match="feed unavailable"):
                await run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    once=True,
                    source_factory=SourceContext,
                    poll_seconds=0.001,
                    emit=lambda _: None,
                )
        assert broker.invalidations and all(item == set() for item in broker.invalidations)
        assert broker.submissions == []

    asyncio.run(test())


def test_closed_market_sync_does_not_require_robinhood():
    async def test():
        broker = Broker(market_open=False)
        with Journal(":memory:") as journal, patch("sweepflow.paper.scan") as scanner:
            await run_paper(broker, ["AAPL"], AppConfig(), journal, once=True, emit=lambda _: None)
            scanner.assert_not_called()
        assert broker.submissions == []

    asyncio.run(test())


def test_transient_submission_failure_keeps_reconciliation_running():
    async def test():
        finished = asyncio.Event()
        signal = signal_now()

        class FlakyBroker(Broker):
            attempts = 0

            def submit(self, signal):
                self.attempts += 1
                if self.attempts == 1:
                    raise RuntimeError("temporary lookup failure")
                return super().submit(signal)

        broker = FlakyBroker()

        async def fake_scan(*args, **kwargs):
            return ScanResult(
                "ok", 1, 1, (), eligible_signals=(signal,), active_signal_ids=(signal.id,)
            )

        def emit(event):
            if event.get("event") == "paper_scan" and broker.submissions:
                finished.set()

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", fake_scan):
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    source_factory=SourceContext,
                    poll_seconds=0.001,
                    scan_seconds=0.001,
                    emit=emit,
                )
            )
            try:
                await asyncio.wait_for(finished.wait(), timeout=1)
                assert broker.attempts == 2 and broker.calls >= 3
                assert any(e.get("event") == "paper_execution_error" for e in journal.events())
                assert {"event": "paper_execution_recovered", "errors": 1} in journal.events()
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(test())


def test_paper_short_entry_toggle_preserves_existing_journal():
    with Journal(":memory:") as journal:
        config = AppConfig()
        bind_monitor(journal, config, ["AAPL"], "alpaca-paper")
        journal.save_alpaca_state("account", {"intents": {"active": {"state": "submitted"}}})
        bind_monitor(
            journal,
            replace(config, execution=replace(config.execution, allow_shorts=True)),
            ["AAPL"],
            "alpaca-paper",
        )
        assert journal.get_alpaca_state("account")["intents"]["active"]["state"] == "submitted"


def test_slow_scans_restart_from_the_start_deadline_while_broker_keeps_polling():
    async def test():
        now = 0.0
        starts = []
        second_started = asyncio.Event()
        broker = Broker()

        async def fake_scan(*args, **kwargs):
            nonlocal now
            starts.append(now)
            if len(starts) == 1:
                now = 40.0
                return ScanResult("ok", 1, 0, ())
            second_started.set()
            await asyncio.Event().wait()

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", fake_scan):
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    source_factory=SourceContext,
                    poll_seconds=0.001,
                    scan_seconds=30,
                    monotonic=lambda: now,
                    emit=lambda _: None,
                )
            )
            try:
                await asyncio.wait_for(second_started.wait(), timeout=1)
                assert starts == [0.0, 40.0]
                syncs = broker.calls
                await asyncio.sleep(0.01)
                assert broker.calls > syncs
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(test())


def test_fast_scans_wait_for_the_next_start_deadline():
    async def test():
        now = 0.0
        starts = []
        completed = asyncio.Event()
        second_started = asyncio.Event()
        broker = Broker()

        async def fake_scan(*args, **kwargs):
            starts.append(now)
            if len(starts) == 2:
                second_started.set()
            return ScanResult("ok", 1, 0, ())

        def emit(event):
            if event.get("event") == "paper_scan":
                completed.set()

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", fake_scan):
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    source_factory=SourceContext,
                    poll_seconds=0.001,
                    scan_seconds=30,
                    monotonic=lambda: now,
                    emit=emit,
                )
            )
            try:
                await asyncio.wait_for(completed.wait(), timeout=1)
                now = 29.0
                await asyncio.sleep(0.01)
                assert starts == [0.0]
                now = 30.0
                await asyncio.wait_for(second_started.wait(), timeout=1)
                assert starts == [0.0, 30.0]
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(test())


def test_slow_feed_failure_has_retry_cooldown_and_reports_recovery():
    async def test():
        now = 0.0
        starts = []
        failed = asyncio.Event()
        recovered = asyncio.Event()
        emitted = []
        broker = Broker()

        async def fake_scan(*args, **kwargs):
            nonlocal now
            starts.append(now)
            if len(starts) == 1:
                now = 40.0
                raise RuntimeError("secret upstream details")
            return ScanResult("ok", 1, 0, ())

        def emit(event):
            emitted.append(event)
            if event.get("event") == "paper_feed_error":
                failed.set()
            elif event.get("event") == "paper_scan":
                recovered.set()

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", fake_scan):
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    source_factory=SourceContext,
                    poll_seconds=0.001,
                    scan_seconds=30,
                    monotonic=lambda: now,
                    emit=emit,
                )
            )
            try:
                await asyncio.wait_for(failed.wait(), timeout=1)
                syncs = broker.calls
                now = 69.0
                await asyncio.sleep(0.01)
                assert starts == [0.0]
                assert broker.calls > syncs
                assert broker.invalidations == [set()]
                now = 70.0
                await asyncio.wait_for(recovered.wait(), timeout=1)
                assert starts == [0.0, 70.0]
                assert {"event": "paper_feed_recovered", "errors": 1} in journal.events()
                assert "secret upstream details" not in repr(emitted)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(test())


def test_sync_errors_emit_safe_api_details_and_recovery_before_resuming_scans():
    async def test():
        emitted = []
        scanned = asyncio.Event()

        class FlakyBroker(Broker):
            def sync(self):
                snapshot = super().sync()
                if self.calls == 1:
                    raise AlpacaAPIError("credential echoed by upstream", status=503)
                return snapshot

        broker = FlakyBroker()

        async def fake_scan(*args, **kwargs):
            scanned.set()
            await asyncio.Event().wait()

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", fake_scan):
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    source_factory=SourceContext,
                    poll_seconds=0.001,
                    emit=emitted.append,
                )
            )
            try:
                await asyncio.wait_for(scanned.wait(), timeout=1)
                error = next(event for event in emitted if event["event"] == "alpaca_sync_error")
                assert error["http_status"] == 503
                assert error["reason"] == "api_error"
                assert error["consecutive"] == 1
                assert error in journal.events()
                assert {"event": "alpaca_sync_recovered", "errors": 1} in emitted
                assert "credential echoed by upstream" not in repr(emitted)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(test())


def test_repeated_sync_failures_stop_at_the_configured_limit():
    async def test():
        emitted = []

        class BrokenBroker(Broker):
            def sync(self):
                super().sync()
                raise RuntimeError("unsafe exception body")

            def invalidate_pending(self, active_ids):
                super().invalidate_pending(active_ids)
                raise AlpacaAPIError("unsafe shutdown exception body", status=503)

        broker = BrokenBroker()
        with Journal(":memory:") as journal, patch("sweepflow.paper.scan") as scanner:
            with pytest.raises(RuntimeError, match="unsafe exception body"):
                await run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    poll_seconds=0.001,
                    max_errors=3,
                    emit=emitted.append,
                )
            scanner.assert_not_called()
            errors = [event for event in emitted if event["event"] == "alpaca_sync_error"]
            assert broker.calls == 3
            assert [error["consecutive"] for error in errors] == [1, 2, 3]
            assert "unsafe exception body" not in repr(errors)
            assert broker.invalidations == [set()]
            shutdown = next(
                event for event in emitted if event["event"] == "paper_shutdown_sync_error"
            )
            assert shutdown["http_status"] == 503
            assert shutdown in journal.events()
            assert "unsafe shutdown exception body" not in repr(emitted)

    asyncio.run(test())


def test_submission_failure_cools_down_and_does_not_extend_signal_freshness():
    async def test():
        now = 0.0
        signal = signal_now()
        wall_time = signal.created_at + timedelta(seconds=1)
        failed = asyncio.Event()
        recovered = asyncio.Event()
        emitted = []

        class FlakyBroker(Broker):
            attempts = 0

            def submit(self, signal):
                self.attempts += 1
                if self.attempts == 1:
                    raise AlpacaAPIError("unsafe submission details", status=503)
                return super().submit(signal)

        broker = FlakyBroker()

        async def fake_scan(*args, **kwargs):
            assert kwargs["max_signal_age"] == timedelta(seconds=90)
            return ScanResult(
                "ok", 1, 1, (), eligible_signals=(signal,), active_signal_ids=(signal.id,)
            )

        def emit(event):
            emitted.append(event)
            if event.get("event") == "paper_execution_error":
                failed.set()
            elif event.get("event") == "paper_scan":
                recovered.set()

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", fake_scan):
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    source_factory=SourceContext,
                    poll_seconds=0.001,
                    scan_seconds=30,
                    monotonic=lambda: now,
                    clock=lambda: wall_time,
                    emit=emit,
                )
            )
            try:
                await asyncio.wait_for(failed.wait(), timeout=1)
                await asyncio.sleep(0.01)
                assert broker.attempts == 1
                now = 30.0
                wall_time = signal.created_at + timedelta(seconds=91)
                await asyncio.wait_for(recovered.wait(), timeout=1)
                assert broker.attempts == 1
                assert broker.submissions == []
                error = next(
                    event for event in emitted if event["event"] == "paper_execution_error"
                )
                assert error["http_status"] == 503
                assert error["consecutive"] == 1
                assert {"event": "paper_execution_recovered", "errors": 1} in journal.events()
                assert "unsafe submission details" not in repr(emitted)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(test())


def test_broker_retry_deferrals_wait_without_spending_sync_error_budget():
    async def test():
        emitted = []
        scanned = asyncio.Event()

        class RateLimitedBroker(Broker):
            def sync(self):
                snapshot = super().sync()
                if self.calls == 1:
                    raise AlpacaAPIError(
                        "unsafe upstream response",
                        status=429,
                        method="GET",
                        endpoint="/v2/orders",
                        reason="rate_limited",
                        attempts=1,
                        retry_after_seconds=60,
                    )
                if self.calls <= 4:
                    raise AlpacaAPIError(
                        "waiting for cooldown",
                        status=429,
                        method="GET",
                        endpoint="/v2/account",
                        reason="retry_deferred",
                        attempts=0,
                        retry_after_seconds=60 - self.calls,
                    )
                return snapshot

        broker = RateLimitedBroker()

        async def fake_scan(*args, **kwargs):
            scanned.set()
            await asyncio.Event().wait()

        with Journal(":memory:") as journal, patch("sweepflow.paper.scan", fake_scan):
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    AppConfig(),
                    journal,
                    source_factory=SourceContext,
                    max_errors=2,
                    poll_seconds=0.001,
                    emit=emitted.append,
                )
            )
            try:
                await asyncio.wait_for(scanned.wait(), timeout=1)
                errors = [event for event in emitted if event["event"] == "alpaca_sync_error"]
                waiting = [
                    event for event in emitted if event["event"] == "paper_broker_retry_deferred"
                ]
                assert len(errors) == 1 and errors[0]["consecutive"] == 1
                assert len(waiting) == 1
                assert waiting[0]["attempts"] == 0
                assert waiting[0]["retry_after_seconds"] == 58
                assert waiting[0]["consecutive"] == 1
                assert {"event": "alpaca_sync_recovered", "errors": 1} in emitted
                assert "unsafe upstream response" not in repr(emitted)
                assert not task.done()
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(test())
