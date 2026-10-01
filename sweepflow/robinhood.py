"""Read-only market data through the installed :mod:`robinhood_mcp` client.

The wire names and shapes were verified with the server's tool discovery.  This
adapter deliberately has no order submission method: Robinhood does not expose
the atomic entry/stop/target bracket required by this strategy.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from robinhood_mcp import RobinhoodMCPClient
from robinhood_mcp.errors import UpstreamUnavailableError
from robinhood_mcp.serialization import to_jsonable

from sweepflow.data import FiveMinuteAggregator
from sweepflow.integrations import UnsupportedExecutionError as UnsupportedExecutionError
from sweepflow.integrations import require_live_execution as require_live_execution
from sweepflow.models import Bar
from sweepflow.sessions import NEW_YORK, SessionCalendar
from sweepflow.universe import normalize_symbol


class RobinhoodDataError(ValueError):
    """The upstream result cannot safely be used as strategy input."""


@dataclass(frozen=True)
class Capabilities:
    tool_names: tuple[str, ...]
    equity_historicals: bool
    accounts: bool
    positions: bool
    orders: bool
    atomic_brackets: bool = False
    short_equities: bool = False
    live_execution: bool = False
    reason: str = (
        "This integration supports read-only data and paper execution. Robinhood's "
        "equity tool exposes individual orders, not atomic entry/stop/target brackets; "
        "Agentic accounts support long equities, not short-equity entries."
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _payload(result: Any) -> dict[str, Any]:
    """Accept native mcp-types results, preserving explicit upstream failures."""
    wire = to_jsonable(result)
    if not isinstance(wire, dict):
        raise RobinhoodDataError("Robinhood returned a non-object MCP result")
    if wire.get("isError", wire.get("is_error", False)):
        raise RobinhoodDataError("Robinhood tool returned isError=true")
    structured = wire.get("structuredContent", wire.get("structured_content"))
    if structured is None:
        texts = [
            block.get("text", "")
            for block in wire.get("content", [])
            if isinstance(block, dict) and block.get("type") == "text"
        ]
        if len(texts) != 1:
            raise RobinhoodDataError("Robinhood result has no unambiguous JSON payload")
        try:
            structured = json.loads(texts[0])
        except (TypeError, json.JSONDecodeError) as exc:
            raise RobinhoodDataError("Robinhood returned non-JSON text") from exc
    if not isinstance(structured, dict) or not isinstance(structured.get("data"), dict):
        raise RobinhoodDataError("Robinhood result is missing its data object")
    return structured["data"]


class _HistoricalClient(RobinhoodMCPClient):
    """Keep transport teardown in the task that owns its context manager.

    The installed wrapper closes AnyIO transport scopes immediately on request
    failure. Concurrent read workers must defer that close to the source owner;
    marking the session unusable still prevents subsequent reads from reusing it.
    """

    def __init__(self) -> None:
        super().__init__()
        self._invalidated = False

    async def connect(self) -> None:
        if self._invalidated:
            raise UpstreamUnavailableError("Historical connection requires reconnect")
        await super().connect()

    async def _invalidate_connection(self) -> None:
        self._invalidated = True

    async def close(self) -> None:
        await super().close()
        self._invalidated = False


class RobinhoodDataSource:
    """Small typed adapter; all callable upstream tools are explicitly read-only."""

    _READ_TOOLS = frozenset(
        {
            "get_equity_historicals",
            "get_accounts",
            "get_portfolio",
            "get_equity_positions",
            "get_equity_orders",
        }
    )
    _BATCH_CONCURRENCY = 4
    _REPAIR_SECONDS = 5
    # Sources reconnect between scans. Avoid repeatedly downloading an entire
    # minute session for a gap whose genuine minutes are still unavailable.
    _repair_retry_at: dict[tuple[str, datetime], datetime] = {}

    def __init__(self, client: RobinhoodMCPClient | None = None) -> None:
        self.client = client if client is not None else _HistoricalClient()
        self._tools: dict[str, Any] | None = None
        self.last_repaired_bars = 0
        self.last_repaired_keys: set[tuple[str, datetime]] = set()
        # An injected wrapper retains its original immediate-close behavior.
        # Keep its requests in the caller's task rather than concurrent workers.
        self._batch_concurrency = (
            1
            if isinstance(self.client, RobinhoodMCPClient)
            and not isinstance(self.client, _HistoricalClient)
            else self._BATCH_CONCURRENCY
        )

    async def __aenter__(self) -> RobinhoodDataSource:
        await self.client.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self.client.close()

    async def discover(self, *, refresh: bool = False) -> Capabilities:
        if self._tools is None or refresh:
            tools = await self.client.list_all_tools(refresh=refresh)
            self._tools = {}
            for tool in tools:
                wire = to_jsonable(tool)
                if not isinstance(wire, dict) or not isinstance(wire.get("name"), str):
                    raise RobinhoodDataError("Invalid tool discovery result")
                self._tools[wire["name"]] = wire
        names = tuple(sorted(self._tools))
        return Capabilities(
            tool_names=names,
            equity_historicals="get_equity_historicals" in names,
            accounts="get_accounts" in names,
            positions="get_equity_positions" in names,
            orders="get_equity_orders" in names,
        )

    async def _read(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if name not in self._READ_TOOLS:
            raise UnsupportedExecutionError(f"Not an allowed read-only tool: {name}")
        await self.discover()
        if self._tools is None or name not in self._tools:
            raise RobinhoodDataError(f"Robinhood did not expose required tool {name}")
        # The library validates arguments against the current discovered JSON schema
        # and invokes exactly once; ambiguous failures are never retried here.
        return _payload(await self.client.call_tool(name, arguments))

    async def get_bars(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
        *,
        now: datetime | None = None,
        bar_minutes: int = 5,
    ) -> dict[str, list[Bar]]:
        """Fetch completed, genuine one- or five-minute regular-session candles.

        Requests use split-adjusted prices and batches of at most ten symbols.
        One-minute requests span at most one day; five-minute requests span at
        most seven days. Missing/invalid symbols fail the request rather than
        silently narrowing the universe. Interpolated and forming bars are omitted.
        """
        if (
            isinstance(bar_minutes, bool)
            or not isinstance(bar_minutes, int)
            or bar_minutes not in (1, 5)
        ):
            raise ValueError("bar_minutes must be 1 or 5")
        interval = "minute" if bar_minutes == 1 else "5minute"
        max_window = timedelta(days=1 if bar_minutes == 1 else 7)
        start, end = _utc(start), _utc(end)
        cutoff = min(end, _utc(now or datetime.now(UTC)))
        if start >= end:
            raise ValueError("start must precede end")
        if isinstance(symbols, str):
            raise TypeError("symbols must be a sequence, for example ['SPY']")
        names = tuple(dict.fromkeys(normalize_symbol(symbol) for symbol in symbols))
        if not names:
            raise ValueError("at least one symbol is required")
        bars: dict[str, dict[datetime, Bar]] = {symbol: {} for symbol in names}
        window_start = start
        while window_start < cutoff:
            window_end = min(window_start + max_window, cutoff)
            if window_end < cutoff:
                window_end = window_end.replace(
                    minute=(window_end.minute // bar_minutes) * bar_minutes, second=0, microsecond=0
                )
            # Discover once before starting concurrent requests, and await each
            # group completely before accepting any of its candle payloads.
            await self.discover()
            batches = [names[offset : offset + 10] for offset in range(0, len(names), 10)]
            for offset in range(0, len(batches), self._batch_concurrency):
                group = batches[offset : offset + self._batch_concurrency]
                if self._batch_concurrency == 1:
                    # Preserve task ownership for an explicitly supplied wrapper.
                    batch = group[0]
                    data = await self._read(
                        "get_equity_historicals",
                        {
                            "symbols": list(batch),
                            "start_time": _timestamp(window_start),
                            "end_time": _timestamp(window_end),
                            "interval": interval,
                            "bounds": "regular",
                            "adjustment_type": "split",
                        },
                    )
                    self._collect_bars(data, batch, window_start, window_end, bars, bar_minutes)
                    continue
                results = await asyncio.gather(
                    *(
                        self._read(
                            "get_equity_historicals",
                            {
                                "symbols": list(batch),
                                "start_time": _timestamp(window_start),
                                "end_time": _timestamp(window_end),
                                "interval": interval,
                                "bounds": "regular",
                                "adjustment_type": "split",
                            },
                        )
                        for batch in group
                    ),
                    return_exceptions=True,
                )
                for data in results:
                    if isinstance(data, BaseException):
                        raise data
                for batch, data in zip(group, results, strict=True):
                    self._collect_bars(data, batch, window_start, window_end, bars, bar_minutes)
            window_start = window_end
        return {
            symbol: sorted(items.values(), key=lambda bar: bar.start)
            for symbol, items in bars.items()
        }

    async def get_bars_with_repair(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
        *,
        now: datetime | None = None,
        known: Sequence[Bar] = (),
        repaired: Sequence[tuple[str, datetime]] = (),
    ) -> dict[str, list[Bar]]:
        """Repair absent five-minute slots from five genuine completed minutes.

        The normal five-minute read and its validation remain authoritative.
        Only absent or previously repaired slots trigger bounded, read-only
        minute requests. Unavailable, interpolated, or incomplete minutes leave the
        original gap intact; minute repairs never overwrite a native five-minute
        bar. Previously repaired slots are refreshed while native data remains
        absent, so later minute corrections can update their persisted OHLC.
        """
        self.last_repaired_bars = 0
        self.last_repaired_keys = set()
        requested_at = _utc(now or datetime.now(UTC))
        start, end = _utc(start), _utc(end)
        rows = await self.get_bars(symbols, start, end, now=requested_at)
        calendar = SessionCalendar()
        collected = {symbol: {bar.start: bar for bar in bars} for symbol, bars in rows.items()}
        present = {(bar.symbol, bar.start) for bars in rows.values() for bar in bars}
        repaired_keys = {(normalize_symbol(symbol), _utc(at)) for symbol, at in repaired}
        present.update(
            (bar.symbol, bar.start)
            for bar in known
            if bar.duration == timedelta(minutes=5)
            and calendar.is_regular_bar(bar)
            and (bar.symbol, bar.start) not in repaired_keys
        )
        # Prune old entries so the shared retry cache stays bounded across days.
        for key, retry_at in tuple(self._repair_retry_at.items()):
            if retry_at < requested_at - timedelta(days=1):
                del self._repair_retry_at[key]
        cutoff = min(end, requested_at)
        gaps_by_day: dict[datetime, dict[str, list[datetime]]] = {}
        day = start.astimezone(NEW_YORK).date()
        last_day = cutoff.astimezone(NEW_YORK).date()
        while day <= last_day:
            session = calendar.session(day)
            if session is not None:
                cursor = session.open
                while cursor + timedelta(minutes=5) <= min(session.close, cutoff):
                    if cursor >= start:
                        for symbol in rows:
                            key = (symbol, cursor)
                            retry_at = self._repair_retry_at.get(key)
                            if key not in present and (
                                retry_at is None or retry_at <= requested_at
                            ):
                                gaps_by_day.setdefault(session.open, {}).setdefault(
                                    symbol, []
                                ).append(cursor)
                    cursor += timedelta(minutes=5)
            day += timedelta(days=1)

        async def repair(batch: tuple[str, ...], missing: dict[str, list[datetime]]) -> None:
            window_start = min(at for symbol in batch for at in missing[symbol])
            window_end = max(at for symbol in batch for at in missing[symbol]) + timedelta(
                minutes=5
            )
            for symbol in batch:
                for at in missing[symbol]:
                    # Failed or canceled optional reads also back off. Recent
                    # publication gaps remain eligible for a prompt retry.
                    wait = timedelta(
                        seconds=30 if requested_at - at < timedelta(minutes=10) else 300
                    )
                    self._repair_retry_at[(symbol, at)] = requested_at + wait
            try:
                minutes = await self.get_bars(
                    batch, window_start, window_end, now=requested_at, bar_minutes=1
                )
            except Exception:
                # Optional recovery cannot turn a valid primary read into an
                # error or make an unavailable candle eligible for execution.
                return
            for symbol in batch:
                aggregator = FiveMinuteAggregator(calendar)
                reconstructed: dict[datetime, Bar] = {}
                for minute in minutes[symbol]:
                    bar = aggregator.on_bar(minute)
                    if bar is not None:
                        reconstructed[bar.start] = bar
                for at in missing[symbol]:
                    bar = reconstructed.get(at)
                    if bar is not None:
                        collected[symbol][at] = bar
                        self.last_repaired_bars += 1
                        self.last_repaired_keys.add((symbol, at))
                        self._repair_retry_at.pop((symbol, at), None)
                    else:
                        # Recent bars may still be publishing; old sparse gaps
                        # warrant a less frequent attempt across reconnects.
                        wait = timedelta(
                            seconds=30 if requested_at - at < timedelta(minutes=10) else 900
                        )
                        self._repair_retry_at[(symbol, at)] = requested_at + wait

        # Prefer this session's missing inputs over older history. Batch each
        # day's affected symbols into one bounded span rather than one request
        # per slot. Cancellation propagates; timeout retains completed repairs.
        try:
            async with asyncio.timeout(self._REPAIR_SECONDS):
                for _, missing in sorted(gaps_by_day.items(), reverse=True):
                    names = tuple(missing)
                    batches = [names[offset : offset + 10] for offset in range(0, len(names), 10)]
                    for offset in range(0, len(batches), self._batch_concurrency):
                        group = batches[offset : offset + self._batch_concurrency]
                        if self._batch_concurrency == 1:
                            await repair(group[0], missing)
                        else:
                            await asyncio.gather(*(repair(batch, missing) for batch in group))
        except TimeoutError:
            pass
        return {
            symbol: sorted(bars.values(), key=lambda bar: bar.start)
            for symbol, bars in collected.items()
        }

    @staticmethod
    def _collect_bars(
        data: dict[str, Any],
        batch: tuple[str, ...],
        start: datetime,
        end: datetime,
        collected: dict[str, dict[datetime, Bar]],
        bar_minutes: int,
    ) -> None:
        if data.get("not_found"):
            raise RobinhoodDataError(f"Symbols not found: {data['not_found']}")
        results = data.get("results")
        if not isinstance(results, list):
            raise RobinhoodDataError("Historical result is missing its results array")
        seen: set[str] = set()
        for result in results:
            if not isinstance(result, dict):
                raise RobinhoodDataError("Invalid historical symbol result")
            symbol = result.get("symbol")
            if symbol not in batch or symbol in seen:
                raise RobinhoodDataError("Unexpected or repeated historical symbol")
            seen.add(symbol)
            interval = "minute" if bar_minutes == 1 else "5minute"
            if result.get("interval") != interval or result.get("bounds") != "regular":
                raise RobinhoodDataError(f"Expected {bar_minutes}-minute regular-session candles")
            rows = result.get("bars")
            if rows is None:
                rows = []
            if not isinstance(rows, list):
                raise RobinhoodDataError("Historical bars must be an array")
            for row in rows:
                if not isinstance(row, dict):
                    raise RobinhoodDataError("Null or malformed candle")
                interpolated = row.get("interpolated")
                if interpolated is not None and not isinstance(interpolated, bool):
                    raise RobinhoodDataError("Malformed interpolated candle flag")
                if interpolated is True:
                    continue
                if row.get("session") not in (None, "", "reg"):
                    raise RobinhoodDataError("Non-regular candle in regular-session response")
                try:
                    timestamp = _utc(datetime.fromisoformat(row["begins_at"]))
                    if timestamp.minute % bar_minutes or timestamp.second or timestamp.microsecond:
                        raise ValueError(
                            f"candle is not aligned to a {bar_minutes}-minute boundary"
                        )
                    volume = row["volume"]
                    if isinstance(volume, bool) or not isinstance(volume, int):
                        raise ValueError("volume must be an integer")
                    bar = Bar(
                        symbol=symbol,
                        start=timestamp,
                        open=Decimal(row["open_price"]),
                        high=Decimal(row["high_price"]),
                        low=Decimal(row["low_price"]),
                        close=Decimal(row["close_price"]),
                        volume=volume,
                        duration=timedelta(minutes=bar_minutes),
                    )
                except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
                    raise RobinhoodDataError(f"Malformed {symbol} candle: {exc}") from exc
                if bar.start < start or bar.end > end:
                    continue
                previous = collected[symbol].get(bar.start)
                if previous is not None and previous != bar:
                    raise RobinhoodDataError(f"Conflicting duplicate {symbol} candle")
                collected[symbol][bar.start] = bar
        if seen != set(batch):
            raise RobinhoodDataError(f"Missing historical results: {sorted(set(batch) - seen)}")

    async def get_accounts(self) -> list[dict[str, Any]]:
        data = await self._read("get_accounts", {})
        return self._records(data, "accounts")

    async def get_portfolio(self, account_number: str) -> dict[str, Any]:
        return await self._read("get_portfolio", self._account_args(account_number))

    async def get_positions(self, account_number: str) -> list[dict[str, Any]]:
        return await self._pages("get_equity_positions", account_number, "positions")

    async def get_orders(
        self, account_number: str, *, open_only: bool = True
    ) -> list[dict[str, Any]]:
        orders = await self._pages("get_equity_orders", account_number, "orders")
        if not open_only:
            return orders
        # Unknown states are conservatively treated as open; pending cancellations
        # are open until the broker reports a terminal result.
        terminal = {
            "filled",
            "cancelled",
            "canceled",
            "rejected",
            "failed",
            "voided",
            "partially_filled_rest_cancelled",
            "partially_filled_rest_canceled",
        }
        return [order for order in orders if order.get("state") not in terminal]

    @staticmethod
    def _account_args(account_number: str) -> dict[str, str]:
        if not isinstance(account_number, str) or not account_number.strip():
            raise ValueError("an explicit account_number is required")
        return {"account_number": account_number}

    @staticmethod
    def _records(data: dict[str, Any], field: str) -> list[dict[str, Any]]:
        if field not in data:
            raise RobinhoodDataError(f"Missing {field} array")
        records = data.get(field)
        if records is None:
            return []
        if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
            raise RobinhoodDataError(f"Invalid {field} array")
        return records

    async def _pages(self, name: str, account: str, field: str) -> list[dict[str, Any]]:
        args = self._account_args(account)
        rows: list[dict[str, Any]] = []
        cursors: set[str] = set()
        while True:
            data = await self._read(name, args)
            rows.extend(self._records(data, field))
            cursor = data.get("next")
            if cursor in (None, ""):
                return rows
            if not isinstance(cursor, str) or cursor in cursors:
                raise RobinhoodDataError("Invalid or repeated account-data pagination cursor")
            cursors.add(cursor)
            args = {**args, "cursor": cursor}
