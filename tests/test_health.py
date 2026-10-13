import asyncio
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from sweepflow.cli import main
from sweepflow.health import check_feed
from sweepflow.models import Bar

NOW = datetime(2026, 10, 12, 12, tzinfo=UTC)
EXPECTED_END = datetime(2026, 10, 9, 20, tzinfo=UTC)


def candle(end=EXPECTED_END):
    return Bar("SPY", end - timedelta(minutes=5), "100", "102", "99", "101", volume=1000)


class FakeSource:
    def __init__(self, bars=None):
        self.bars = [candle()] if bars is None else bars
        self.calls = []
        self.closed = False
        self.capabilities = SimpleNamespace(equity_historicals=True)

    async def __aenter__(self):
        self.calls.append("connect")
        return self

    async def __aexit__(self, *_):
        await self.close()

    async def close(self):
        self.closed = True

    async def discover(self):
        self.calls.append("discover")
        return self.capabilities

    async def get_bars(self, symbols, start, end, *, now):
        self.calls.append((symbols, start, end, now))
        return {"SPY": self.bars}


@pytest.mark.parametrize(
    ("now", "expected_end"),
    [
        (NOW, EXPECTED_END),
        (datetime(2026, 10, 11, 12, tzinfo=UTC), EXPECTED_END),
        (datetime(2025, 7, 4, 12, tzinfo=UTC), datetime(2025, 7, 3, 17, tzinfo=UTC)),
        (datetime(2025, 3, 10, 12, tzinfo=UTC), datetime(2025, 3, 7, 21, tzinfo=UTC)),
    ],
)
def test_probe_uses_previous_session_close_without_claiming_live_freshness(now, expected_end):
    source = FakeSource([candle(expected_end)])
    result = asyncio.run(check_feed(source_factory=lambda: source, clock=lambda: now))
    assert result["status"] == "ready"
    assert result["expected_bar_start"] == expected_end - timedelta(minutes=5)
    assert result["expected_bar_end"] == expected_end
    assert result["checked_at"] == now
    assert result["coverage_scope"] == "previous_session_close"
    assert result["real_time_verified"] is False
    assert result["received_bars"] == result["valid_bars"] == 1
    assert source.calls == [
        "connect",
        "discover",
        (("SPY",), expected_end - timedelta(minutes=5), expected_end, now),
    ]
    assert source.closed


@dataclass(frozen=True)
class InterpolatedBar(Bar):
    interpolated: bool = True


@pytest.mark.parametrize(
    "bars",
    [
        [],
        [candle(EXPECTED_END - timedelta(minutes=5))],
        [replace(candle(), duration=timedelta(minutes=1))],
        [replace(candle(), symbol="AAPL")],
        [replace(candle(), volume=0)],
        [candle(), candle()],
        [{"symbol": "SPY", "interpolated": True}],
        [
            InterpolatedBar(
                "SPY", EXPECTED_END - timedelta(minutes=5), "100", "102", "99", "101", volume=1000
            )
        ],
    ],
)
def test_probe_rejects_missing_stale_invalid_and_interpolated_candles(bars):
    source = FakeSource(bars)
    result = asyncio.run(check_feed(source_factory=lambda: source, clock=lambda: NOW))
    assert result["status"] == "incomplete"
    assert result["reason"] == "missing_completed_probe_bar"
    assert source.closed


def test_probe_requires_historical_capability_before_reading():
    source = FakeSource()
    source.capabilities.equity_historicals = False
    result = asyncio.run(check_feed(source_factory=lambda: source, clock=lambda: NOW))
    assert result["status"] == "unavailable"
    assert result["reason"] == "historicals_unavailable"
    assert source.calls == ["connect", "discover"]
    assert source.closed


@pytest.mark.parametrize("stage", ["connect", "discover", "history"])
def test_probe_checks_actual_access_and_sanitizes_authentication_errors(stage):
    class AuthenticationRequired(RuntimeError):
        pass

    class AuthSource(FakeSource):
        async def __aenter__(self):
            if stage == "connect":
                raise AuthenticationRequired("unsafe credential detail")
            return await super().__aenter__()

        async def discover(self):
            if stage == "discover":
                raise AuthenticationRequired("unsafe credential detail")
            return await super().discover()

        async def get_bars(self, *args, **kwargs):
            raise AuthenticationRequired("unsafe credential detail")

    source = AuthSource()
    result = asyncio.run(check_feed(source_factory=lambda: source, clock=lambda: NOW))
    assert result["status"] == "unavailable"
    assert result["reason"] == "authentication_required"
    assert "Stored credentials alone" in result["action"]
    assert "unsafe credential detail" not in repr(result)
    assert source.closed


@pytest.mark.parametrize("stage", ["connect", "history"])
def test_probe_timeout_cleans_up_and_returns_safe_result(monkeypatch, stage):
    class SlowSource(FakeSource):
        async def __aenter__(self):
            if stage == "connect":
                await asyncio.Event().wait()
            return await super().__aenter__()

        async def get_bars(self, *args, **kwargs):
            await asyncio.Event().wait()

    source = SlowSource()
    monkeypatch.setattr("sweepflow.health._PROBE_TIMEOUT_SECONDS", 0)
    result = asyncio.run(check_feed(source_factory=lambda: source, clock=lambda: NOW))
    assert result["status"] == "unavailable"
    assert result["reason"] == "feed_timeout"
    assert result["error_type"] == "TimeoutError"
    assert source.closed


def test_probe_propagates_task_cancellation_after_closing_source():
    async def check():
        waiting = asyncio.Event()

        class SlowSource(FakeSource):
            async def get_bars(self, *args, **kwargs):
                waiting.set()
                await asyncio.Event().wait()

        source = SlowSource()
        task = asyncio.create_task(check_feed(source_factory=lambda: source, clock=lambda: NOW))
        await asyncio.wait_for(waiting.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert source.closed

    asyncio.run(check())


@pytest.mark.parametrize("stage", ["connect", "history", "successful_read"])
def test_probe_bounds_hanging_cleanup_after_request_timeout_or_success(monkeypatch, stage):
    class HangingSource(FakeSource):
        async def __aenter__(self):
            if stage == "connect":
                await asyncio.Event().wait()
            return await super().__aenter__()

        async def get_bars(self, *args, **kwargs):
            if stage == "history":
                await asyncio.Event().wait()
            return await super().get_bars(*args, **kwargs)

        async def close(self):
            self.closed = True
            await asyncio.Event().wait()

    async def check():
        source = HangingSource()
        result = await asyncio.wait_for(
            check_feed(source_factory=lambda: source, clock=lambda: NOW), timeout=0.5
        )
        assert result["status"] == "unavailable"
        assert result["reason"] == "feed_timeout"
        assert source.closed

    monkeypatch.setattr("sweepflow.health._PROBE_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr("sweepflow.health._CLEANUP_TIMEOUT_SECONDS", 0.01)
    asyncio.run(check())


def test_cleanup_timeout_preserves_authentication_failure(monkeypatch):
    class AuthenticationRequired(RuntimeError):
        pass

    class HangingSource(FakeSource):
        async def get_bars(self, *args, **kwargs):
            raise AuthenticationRequired("do not log")

        async def close(self):
            self.closed = True
            await asyncio.Event().wait()

    monkeypatch.setattr("sweepflow.health._CLEANUP_TIMEOUT_SECONDS", 0.01)
    source = HangingSource()
    result = asyncio.run(check_feed(source_factory=lambda: source, clock=lambda: NOW))
    assert source.closed
    assert result["reason"] == "authentication_required"
    assert "do not log" not in repr(result)


def test_cleanup_failure_after_success_blocks_ready_and_sanitizes_error():
    class FailingCloseSource(FakeSource):
        async def close(self):
            self.closed = True
            raise RuntimeError("unsafe transport response")

    source = FailingCloseSource()
    result = asyncio.run(check_feed(source_factory=lambda: source, clock=lambda: NOW))
    assert source.closed
    assert result["status"] == "unavailable"
    assert result["reason"] == "feed_unavailable"
    assert result["error_type"] == "RuntimeError"
    assert "unsafe transport response" not in repr(result)


@pytest.mark.parametrize("authentication_failure", [False, True])
def test_request_deadline_ends_before_independent_cleanup(monkeypatch, authentication_failure):
    class AuthenticationRequired(RuntimeError):
        pass

    async def check():
        cleanup_started = asyncio.Event()
        finish_cleanup = asyncio.Event()
        budgets = []
        timeout = asyncio.timeout

        def track_timeout(seconds):
            budget = timeout(seconds)
            budgets.append(budget)
            return budget

        class LateSource(FakeSource):
            async def get_bars(self, *args, **kwargs):
                # Expiration is queued for the next event-loop turn. A request
                # completing now must exit its deadline before cleanup yields.
                budgets[0].reschedule(asyncio.get_running_loop().time())
                if authentication_failure:
                    raise AuthenticationRequired("unsafe credential detail")
                return await super().get_bars(*args, **kwargs)

            async def close(self):
                cleanup_started.set()
                await finish_cleanup.wait()
                self.closed = True

        source = LateSource()
        monkeypatch.setattr("sweepflow.health.asyncio.timeout", track_timeout)
        task = asyncio.create_task(check_feed(source_factory=lambda: source, clock=lambda: NOW))
        try:
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            await asyncio.sleep(0)
            assert not budgets[0].expired()
            assert not task.done()
            assert not source.closed
            finish_cleanup.set()
            result = await asyncio.wait_for(task, timeout=1)
            assert source.closed
            assert result["status"] == ("unavailable" if authentication_failure else "ready")
            assert result["reason"] == (
                "authentication_required"
                if authentication_failure
                else "historical_probe_succeeded"
            )
            if authentication_failure:
                assert result["error_type"] == "AuthenticationRequired"
            assert "unsafe credential detail" not in repr(result)
        finally:
            finish_cleanup.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


def test_cleanup_timeout_preserves_caller_cancellation(monkeypatch):
    async def check():
        waiting = asyncio.Event()

        class HangingSource(FakeSource):
            async def get_bars(self, *args, **kwargs):
                waiting.set()
                await asyncio.Event().wait()

            async def close(self):
                self.closed = True
                await asyncio.Event().wait()

        source = HangingSource()
        task = asyncio.create_task(check_feed(source_factory=lambda: source, clock=lambda: NOW))
        await asyncio.wait_for(waiting.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=0.5)
        assert source.closed

    monkeypatch.setattr("sweepflow.health._CLEANUP_TIMEOUT_SECONDS", 0.01)
    asyncio.run(check())


def test_feed_check_exit_status_is_based_on_probe_not_stored_credentials(monkeypatch, capsys):
    from sweepflow import health

    async def unavailable():
        return {"status": "unavailable", "reason": "authentication_required"}

    monkeypatch.setattr(health, "check_feed", unavailable)
    assert main(["feed-check"]) == 1
    assert json.loads(capsys.readouterr().out)["reason"] == "authentication_required"

    async def ready():
        return {"status": "ready", "real_time_verified": False}

    monkeypatch.setattr(health, "check_feed", ready)
    assert main(["feed-check"]) == 0
    assert json.loads(capsys.readouterr().out)["real_time_verified"] is False
