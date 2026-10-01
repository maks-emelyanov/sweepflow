from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

pytest.importorskip("robinhood_mcp_wrapper", reason="Optional Robinhood wrapper is not installed")

from mcp_types import CallToolResult, TextContent
from robinhood_mcp_wrapper import RobinhoodMCPClient
from robinhood_mcp_wrapper.errors import UpstreamUnavailableError

from sweepflow.models import Bar
from sweepflow.robinhood import (
    RobinhoodDataError,
    RobinhoodDataSource,
    UnsupportedExecutionError,
    _HistoricalClient,
    require_live_execution,
)

NOW = datetime(2026, 9, 28, 14, 17, tzinfo=UTC)


def candle(minute=0, **overrides):
    return {
        "begins_at": f"2026-09-28T14:{minute:02d}:00Z",
        "open_price": "100.10",
        "high_price": "101.20",
        "low_price": "99.90",
        "close_price": "100.50",
        "volume": 12345,
        "session": "reg",
        "interpolated": False,
        **overrides,
    }


def history(symbol="SPY", bars=None, **overrides):
    return {
        "symbol": symbol,
        "interval": "5minute",
        "bounds": "regular",
        "bars": [candle()] if bars is None else bars,
        **overrides,
    }


class FakeClient:
    def __init__(self, responses=None, names=None):
        self.responses = list(responses or [])
        self.names = names or [
            "get_equity_historicals",
            "get_accounts",
            "get_equity_positions",
            "get_equity_orders",
            "get_portfolio",
            "place_equity_order",
        ]
        self.calls = []
        self.closed = False

    async def connect(self):
        pass

    async def close(self):
        self.closed = True

    async def list_all_tools(self, *, refresh=False):
        return [{"name": name, "inputSchema": {"type": "object"}} for name in self.names]

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        if self.responses:
            return self.responses.pop(0)
        return result(
            {
                "results": [
                    history(symbol, [], interval=arguments["interval"])
                    for symbol in arguments["symbols"]
                ]
            }
        )


def result(data):
    return CallToolResult(structured_content={"data": data, "guide": ""}, content=[])


def fetch(client):
    return asyncio.run(
        RobinhoodDataSource(client).get_bars(["SPY"], NOW.replace(minute=0), NOW, now=NOW)
    )


def test_native_mcp_models_keep_prices_and_filter_forming_or_interpolated_bars():
    client = FakeClient(
        [
            result(
                {
                    "results": [
                        history(
                            bars=[
                                candle(15),
                                candle(0),
                                candle(5, interpolated=True, volume=0),
                                candle(10),
                            ]
                        )
                    ]
                }
            )
        ]
    )
    bars = fetch(client)["SPY"]
    assert [bar.start.minute for bar in bars] == [0, 10]
    assert str(bars[0].open) == "100.10"
    assert bars[0].volume == 12345
    assert client.calls[0] == (
        "get_equity_historicals",
        {
            "symbols": ["SPY"],
            "interval": "5minute",
            "bounds": "regular",
            "adjustment_type": "split",
            "start_time": "2026-09-28T14:00:00Z",
            "end_time": "2026-09-28T14:17:00Z",
        },
    )


def test_text_only_mcp_result_supported():
    client = FakeClient(
        [
            CallToolResult(
                content=[
                    TextContent(type="text", text=json.dumps({"data": {"results": [history()]}}))
                ]
            )
        ]
    )
    assert len(fetch(client)["SPY"]) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"results": []},
        {"results": [history(interval="day")]},
        {"results": [history(bounds="extended")]},
        {"results": [history(symbol="AAPL")]},
        {"results": [history(bars=[candle(session="pre")])]},
        {"results": [history(bars=[candle(volume="100")])]},
        {"results": [history(bars=[candle(interpolated="true")])]},
        {"results": [history(bars=[candle(high_price="NaN")])]},
        {"results": [history(bars=[candle(1)])]},
        {"results": [history(bars=[candle(), candle(close_price="100.60")])]},
        {"results": [history()], "not_found": ["SPY"]},
    ],
)
def test_invalid_candles_and_missing_symbols_fail_closed(payload):
    with pytest.raises(RobinhoodDataError):
        fetch(FakeClient([result(payload)]))


def test_explicit_upstream_error_is_not_mistaken_for_empty_history():
    client = FakeClient([CallToolResult(is_error=True, content=[])])
    with pytest.raises(RobinhoodDataError, match="isError"):
        fetch(client)
    assert len(client.calls) == 1


def test_batches_at_ten_symbols_and_aligned_week_chunks():
    client = FakeClient()
    names = ["SPY", "VOO", "IVV", "AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "TSLA", "JPM"]
    rows = asyncio.run(
        RobinhoodDataSource(client).get_bars(names, NOW - timedelta(days=8), NOW, now=NOW)
    )
    assert set(rows) == set(names)
    assert [len(args["symbols"]) for _, args in client.calls] == [10, 1, 10, 1]
    first_end = datetime.fromisoformat(client.calls[0][1]["end_time"])
    assert first_end.minute % 5 == 0
    assert client.calls[2][1]["start_time"] == client.calls[0][1]["end_time"]


def test_positions_paginate_and_orders_treat_pending_cancel_as_open():
    client = FakeClient(
        [
            result({"positions": [{"symbol": "SPY"}], "next": "next-page"}),
            result({"positions": [{"symbol": "VOO"}], "next": ""}),
            result(
                {
                    "orders": [
                        {"state": "filled"},
                        {"state": "pending_cancelled"},
                        {"state": "new"},
                    ],
                    "next": "",
                }
            ),
        ]
    )

    async def read():
        async with RobinhoodDataSource(client) as source:
            return await source.get_positions("test-account"), await source.get_orders(
                "test-account"
            )

    positions, orders = asyncio.run(read())
    assert len(positions) == len(orders) == 2
    assert client.calls[1][1]["cursor"] == "next-page"
    assert client.closed


def test_repeated_pagination_cursor_errors():
    client = FakeClient([result({"orders": [], "next": "repeat"})] * 2)
    with pytest.raises(RobinhoodDataError, match="cursor"):
        asyncio.run(RobinhoodDataSource(client).get_orders("test-account"))


def test_discovery_is_honest_and_mutating_tools_cannot_be_called():
    client = FakeClient()
    source = RobinhoodDataSource(client)
    capabilities = asyncio.run(source.discover())
    assert capabilities.equity_historicals
    assert not capabilities.atomic_brackets
    assert not capabilities.short_equities
    assert not capabilities.live_execution
    with pytest.raises(UnsupportedExecutionError):
        asyncio.run(source._read("place_equity_order", {}))
    with pytest.raises(UnsupportedExecutionError):
        require_live_execution()
    assert client.calls == []


def test_missing_history_tool_and_naive_time_are_rejected():
    with pytest.raises(RobinhoodDataError, match="required tool"):
        fetch(FakeClient(names=["get_accounts"]))
    with pytest.raises(ValueError, match="timezone"):
        asyncio.run(
            RobinhoodDataSource(FakeClient()).get_bars(
                ["SPY"], NOW.replace(tzinfo=None), NOW, now=NOW
            )
        )


def test_missing_account_records_are_not_silently_treated_as_empty():
    client = FakeClient([result({"next": ""})])
    with pytest.raises(RobinhoodDataError, match="Missing positions"):
        asyncio.run(RobinhoodDataSource(client).get_positions("test-account"))


def test_one_minute_history_requests_real_minute_bars_and_filters_forming_bar():
    client = FakeClient(
        [
            result(
                {
                    "results": [
                        history(
                            interval="minute", bars=[candle(0), candle(1), candle(16), candle(17)]
                        )
                    ]
                }
            )
        ]
    )
    rows = asyncio.run(
        RobinhoodDataSource(client).get_bars(
            ["SPY"], NOW.replace(minute=0), NOW, now=NOW, bar_minutes=1
        )
    )
    assert [bar.start.minute for bar in rows["SPY"]] == [0, 1, 16]
    assert all(bar.duration == timedelta(minutes=1) for bar in rows["SPY"])
    assert client.calls[0][1]["interval"] == "minute"


def test_one_minute_requests_chunk_to_at_most_one_day():
    client = FakeClient()
    asyncio.run(
        RobinhoodDataSource(client).get_bars(
            ["SPY"], NOW - timedelta(days=2, seconds=20), NOW, now=NOW, bar_minutes=1
        )
    )
    assert len(client.calls) == 3
    for _, args in client.calls:
        start = datetime.fromisoformat(args["start_time"])
        end = datetime.fromisoformat(args["end_time"])
        assert end - start <= timedelta(days=1)
        assert args["interval"] == "minute"
    assert client.calls[1][1]["start_time"] == client.calls[0][1]["end_time"]


def test_one_minute_request_rejects_coarser_response():
    client = FakeClient([result({"results": [history()]})])
    with pytest.raises(RobinhoodDataError, match="1-minute"):
        asyncio.run(
            RobinhoodDataSource(client).get_bars(
                ["SPY"], NOW - timedelta(minutes=20), NOW, now=NOW, bar_minutes=1
            )
        )


@pytest.mark.parametrize("bar_minutes", [0, 2, 15, -1, True, 1.0, "1"])
def test_unsupported_bar_duration_fails_before_upstream_call(bar_minutes):
    client = FakeClient()
    with pytest.raises(ValueError, match="bar_minutes"):
        asyncio.run(
            RobinhoodDataSource(client).get_bars(
                ["SPY"], NOW - timedelta(minutes=20), NOW, now=NOW, bar_minutes=bar_minutes
            )
        )
    assert client.calls == []


@pytest.fixture
def isolated_repair_cache(monkeypatch):
    monkeypatch.setattr(RobinhoodDataSource, "_repair_retry_at", {})


def repaired(source, *, now=NOW, known=(), repaired_keys=()):
    return asyncio.run(
        source.get_bars_with_repair(
            ["SPY"], NOW.replace(minute=0), NOW, now=now, known=known, repaired=repaired_keys
        )
    )["SPY"]


def test_missing_five_minute_bar_is_repaired_from_complete_genuine_minutes(
    isolated_repair_cache,
):
    minute_rows = [
        candle(
            minute,
            open_price="100.20",
            high_price=str(102 + minute),
            low_price=str(99 - minute),
            close_price=str(100 + minute),
            volume=minute,
        )
        for minute in range(5, 10)
    ]
    client = FakeClient(
        [
            result({"results": [history(bars=[candle(0), candle(5, interpolated=True)])]}),
            result({"results": [history(interval="minute", bars=minute_rows)]}),
        ]
    )
    # Exclude the later 14:10 slot from this request so its absence is unrelated.
    source = RobinhoodDataSource(client)
    rows = asyncio.run(
        source.get_bars_with_repair(["SPY"], NOW.replace(minute=0), NOW.replace(minute=10), now=NOW)
    )["SPY"]
    assert [bar.start.minute for bar in rows] == [0, 5]
    assert rows[0].open == Decimal("100.10")
    assert rows[1] == Bar(
        "SPY",
        NOW.replace(minute=5),
        Decimal("100.20"),
        Decimal("111"),
        Decimal("90"),
        Decimal("109"),
        volume=35,
        duration=timedelta(minutes=5),
    )
    assert source.last_repaired_bars == 1
    assert source.last_repaired_keys == {("SPY", NOW.replace(minute=5))}
    assert client.calls[1][1]["interval"] == "minute"
    assert client.calls[1][1]["start_time"] == "2026-09-28T14:05:00Z"
    assert client.calls[1][1]["end_time"] == "2026-09-28T14:10:00Z"


@pytest.mark.parametrize("missing", range(5, 10))
def test_repair_requires_every_genuine_minute(missing, isolated_repair_cache):
    client = FakeClient(
        [
            result({"results": [history(bars=[candle(0), candle(10)])]}),
            result(
                {
                    "results": [
                        history(
                            interval="minute",
                            bars=[candle(minute) for minute in range(5, 10) if minute != missing],
                        )
                    ]
                }
            ),
        ]
    )
    source = RobinhoodDataSource(client)
    assert [bar.start.minute for bar in repaired(source)] == [0, 10]
    assert source.last_repaired_bars == 0


@pytest.mark.parametrize(
    "minute_history",
    [
        history(
            interval="minute", bars=[candle(minute, interpolated=True) for minute in range(5, 10)]
        ),
        history(interval="5minute", bars=[candle(5)]),
        history(interval="minute", bounds="extended", bars=[candle(5)]),
        history(interval="minute", bars=[candle(5, session="pre")]),
        history(interval="minute", bars=[candle(5, begins_at="2026-09-28T14:05:00")]),
        history(interval="minute", bars=[candle(5), candle(5, close_price="100.60")]),
    ],
)
def test_invalid_or_interpolated_minute_repair_preserves_original_data(
    minute_history, isolated_repair_cache
):
    source = RobinhoodDataSource(
        FakeClient(
            [
                result({"results": [history(bars=[candle(0), candle(10)])]}),
                result({"results": [minute_history]}),
            ]
        )
    )
    assert [bar.start.minute for bar in repaired(source)] == [0, 10]
    assert source.last_repaired_bars == 0


def test_known_complete_slot_and_forming_slots_do_not_request_repair(isolated_repair_cache):
    known = Bar("SPY", NOW.replace(minute=5), Decimal(100), Decimal(101), Decimal(99), Decimal(100))
    client = FakeClient([result({"results": [history(bars=[candle(0), candle(10)])]})])
    source = RobinhoodDataSource(client)
    assert [bar.start.minute for bar in repaired(source, known=[known])] == [0, 10]
    assert len(client.calls) == 1
    assert source.last_repaired_bars == 0


@pytest.mark.parametrize("reconnect", [False, True])
def test_persisted_minute_repairs_refresh_after_corrected_minutes(reconnect, isolated_repair_cache):
    primary = result({"results": [history(bars=[candle(0), candle(10)])]})
    initial_minutes = result(
        {"results": [history(interval="minute", bars=[candle(i) for i in range(5, 10)])]}
    )
    corrected_minutes = result(
        {
            "results": [
                history(
                    interval="minute",
                    bars=[
                        candle(i, close_price="100.90" if i == 9 else "100.50")
                        for i in range(5, 10)
                    ],
                )
            ]
        }
    )
    client = FakeClient([primary, initial_minutes])
    source = RobinhoodDataSource(client)
    initial = repaired(source)
    persisted_keys = set(source.last_repaired_keys)
    assert initial[1].close == Decimal("100.50")
    assert persisted_keys == {("SPY", NOW.replace(minute=5))}
    if reconnect:
        client = FakeClient([primary, corrected_minutes])
        source = RobinhoodDataSource(client)
    else:
        client.responses.extend([primary, corrected_minutes])
    updated = repaired(source, known=initial, repaired_keys=persisted_keys)
    assert [bar.start.minute for bar in updated] == [0, 5, 10]
    assert updated[1].close == Decimal("100.90")
    assert source.last_repaired_bars == 1
    assert source.last_repaired_keys == persisted_keys
    assert client.calls[-1][1]["interval"] == "minute"


def test_native_five_minute_bar_supersedes_prior_minute_repair(isolated_repair_cache):
    primary = result({"results": [history(bars=[candle(0), candle(10)])]})
    client = FakeClient(
        [
            primary,
            result(
                {"results": [history(interval="minute", bars=[candle(i) for i in range(5, 10)])]}
            ),
        ]
    )
    source = RobinhoodDataSource(client)
    initial = repaired(source)
    persisted_keys = set(source.last_repaired_keys)
    client.responses.append(
        result(
            {"results": [history(bars=[candle(0), candle(5, close_price="100.80"), candle(10)])]}
        )
    )
    updated = repaired(source, known=initial, repaired_keys=persisted_keys)
    assert updated[1].close == Decimal("100.80")
    assert len(client.calls) == 3
    assert source.last_repaired_bars == 0
    assert source.last_repaired_keys == set()


def test_unavailable_old_minutes_back_off_across_source_reconnects(isolated_repair_cache):
    primary = result({"results": [history(bars=[candle(0), candle(10)])]})
    first = FakeClient([primary, result({"results": [history(interval="minute", bars=[])]})])
    assert [bar.start.minute for bar in repaired(RobinhoodDataSource(first))] == [0, 10]
    second = FakeClient([primary])
    assert [bar.start.minute for bar in repaired(RobinhoodDataSource(second))] == [0, 10]
    assert len(first.calls) == 2
    assert len(second.calls) == 1
    later = FakeClient(
        [
            primary,
            result(
                {"results": [history(interval="minute", bars=[candle(i) for i in range(5, 10)])]}
            ),
        ]
    )
    assert [
        bar.start.minute
        for bar in repaired(RobinhoodDataSource(later), now=NOW + timedelta(minutes=16))
    ] == [0, 5, 10]
    assert len(later.calls) == 2


def test_repair_deadline_cancels_slow_read_and_preserves_original_data(
    monkeypatch, isolated_repair_cache
):
    class SlowMinutes(FakeClient):
        minute_cancelled = False

        async def call_tool(self, name, arguments):
            if arguments["interval"] == "minute":
                self.calls.append((name, arguments))
                try:
                    await asyncio.Event().wait()
                finally:
                    self.minute_cancelled = True
            return await super().call_tool(name, arguments)

    monkeypatch.setattr(RobinhoodDataSource, "_REPAIR_SECONDS", 0.01)
    client = SlowMinutes([result({"results": [history(bars=[candle(0), candle(10)])]})])
    source = RobinhoodDataSource(client)
    assert [bar.start.minute for bar in repaired(source)] == [0, 10]
    assert client.minute_cancelled
    assert source.last_repaired_bars == 0


def test_repair_ignores_non_session_dates_and_out_of_session_minutes(isolated_repair_cache):
    # Sep 26, 2026 is Saturday. Sep 28's regular session starts at 13:30 UTC.
    client = FakeClient()
    source = RobinhoodDataSource(client)
    rows = asyncio.run(
        source.get_bars_with_repair(
            ["SPY"],
            datetime(2026, 9, 26, 14, tzinfo=UTC),
            datetime(2026, 9, 28, 13, 34, tzinfo=UTC),
            now=NOW,
        )
    )
    assert rows == {"SPY": []}
    assert all(args["interval"] == "5minute" for _, args in client.calls)


def test_primary_read_remains_fail_closed_during_repair(isolated_repair_cache):
    client = FakeClient([result({"results": [history(interval="minute")]})])
    with pytest.raises(RobinhoodDataError, match="5-minute"):
        repaired(RobinhoodDataSource(client))
    assert len(client.calls) == 1


def test_history_batches_are_concurrent_and_bounded():
    class ConcurrentClient(FakeClient):
        active = 0
        maximum = 0

        async def call_tool(self, name, arguments):
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            try:
                await asyncio.sleep(0)
                return await super().call_tool(name, arguments)
            finally:
                self.active -= 1

    client = ConcurrentClient()
    names = [f"SYM{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(51)]
    rows = asyncio.run(
        RobinhoodDataSource(client).get_bars(names, NOW - timedelta(minutes=5), NOW, now=NOW)
    )
    assert set(rows) == set(names)
    assert client.maximum == 4
    assert [len(args["symbols"]) for _, args in client.calls] == [10, 10, 10, 10, 10, 1]
    assert client.active == 0


def test_failed_concurrent_history_group_waits_for_remaining_requests():
    class FailingClient(FakeClient):
        completed = []

        async def call_tool(self, name, arguments):
            await asyncio.sleep(0)
            if arguments["symbols"][0] == "SYMAA":
                raise RobinhoodDataError("failed historical batch")
            await asyncio.sleep(0)
            self.completed.append(arguments["symbols"][0])
            return await super().call_tool(name, arguments)

    client = FailingClient()
    with pytest.raises(RobinhoodDataError, match="failed historical batch"):
        asyncio.run(
            RobinhoodDataSource(client).get_bars(
                [f"SYMA{chr(65 + i)}" for i in range(21)],
                NOW - timedelta(minutes=5),
                NOW,
                now=NOW,
            )
        )
    assert client.completed == ["SYMAK", "SYMAU"]


def test_concurrent_wrapper_failure_defers_transport_close_to_owner_task():
    async def exercise():
        class OwnerStack:
            owner = None
            closed = False

            async def __aenter__(self):
                self.owner = asyncio.current_task()
                return self

            async def __aexit__(self, *_):
                assert asyncio.current_task() is self.owner
                self.closed = True

        client = _HistoricalClient()
        source = RobinhoodDataSource(client)
        stack = OwnerStack()
        stop = asyncio.Event()
        ready = asyncio.Event()

        async def own_connection():
            async with stack:
                ready.set()
                await stop.wait()

        owner_task = asyncio.create_task(own_connection())
        client._connection_task = owner_task
        client._connection_stop = stop
        client._client = object()
        await ready.wait()
        assert stack.owner is owner_task

        workers_ready = asyncio.Event()
        request_count = 0

        async def unavailable(_):
            nonlocal request_count
            request_count += 1
            if request_count == 2:
                workers_ready.set()
            await workers_ready.wait()
            raise OSError("upstream disconnected")

        failures = await asyncio.wait_for(
            asyncio.gather(
                client._invoke(unavailable), client._invoke(unavailable), return_exceptions=True
            ),
            timeout=5,
        )
        assert all(isinstance(failure, UpstreamUnavailableError) for failure in failures)
        assert not stack.closed
        assert not stop.is_set()
        assert not owner_task.done()

        async def unexpected_read(_):
            pytest.fail("invalidated connection accepted another read")

        with pytest.raises(UpstreamUnavailableError, match="requires reconnect"):
            await client._invoke(unexpected_read)
        await source.close()
        assert stack.closed
        assert stop.is_set()
        assert owner_task.done()
        assert client._connection_task is None
        assert client._connection_stop is None
        assert not client.connected

    asyncio.run(exercise())


def test_explicit_wrapper_keeps_serial_reads_in_its_owning_task():
    class OwnerClient(RobinhoodMCPClient):
        def __init__(self):
            self.calls = []
            self.owner = None

        async def list_all_tools(self, *, refresh=False):
            self.owner = asyncio.current_task()
            return [{"name": "get_equity_historicals", "inputSchema": {"type": "object"}}]

        async def call_tool(self, name, arguments):
            assert asyncio.current_task() is self.owner
            self.calls.append((name, arguments))
            return result({"results": [history(symbol, []) for symbol in arguments["symbols"]]})

    client = OwnerClient()
    rows = asyncio.run(
        RobinhoodDataSource(client).get_bars(
            [f"SYMA{chr(65 + i)}" for i in range(21)],
            NOW - timedelta(minutes=5),
            NOW,
            now=NOW,
        )
    )
    assert len(rows) == 21
    assert len(client.calls) == 3
