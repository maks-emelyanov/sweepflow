"""Read-only checks of actual historical-feed access, independent of credentials on disk."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sweepflow.integrations import create_robinhood_data_source
from sweepflow.models import Bar, aware
from sweepflow.paper import feed_error_fields
from sweepflow.sessions import NEW_YORK, SessionCalendar

_PROBE_TIMEOUT_SECONDS = 30
_CLEANUP_TIMEOUT_SECONDS = 5


async def check_feed(
    *,
    source_factory: Callable = create_robinhood_data_source,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, object]:
    """Verify one genuine SPY bar from the previous regular session's close.

    A stored OAuth credential is not proof that a connection or a historical
    request works. This probe exercises both, without journal or order access.
    Historical coverage does not establish current-session timeliness or full
    universe coverage. The adapter discards interpolated upstream candles.
    Requests have a 30-second budget, followed by at most five seconds of cleanup.
    """
    checked_at = clock()
    aware(checked_at, "checked_at")
    calendar = SessionCalendar()
    day = checked_at.astimezone(NEW_YORK).date() - timedelta(days=1)
    session = calendar.session(day)
    while session is None:
        day -= timedelta(days=1)
        session = calendar.session(day)
    end = session.close
    start = end - timedelta(minutes=5)
    result: dict[str, object] = {
        "status": "unavailable",
        "checked_at": checked_at,
        "symbol": "SPY",
        "coverage_scope": "previous_session_close",
        "real_time_verified": False,
        "session": session.label,
        "expected_bar_start": start,
        "expected_bar_end": end,
        "received_bars": 0,
        "valid_bars": 0,
    }
    try:
        context = None
        entered = False
        try:
            async with asyncio.timeout(_PROBE_TIMEOUT_SECONDS):
                context = source_factory()
                source = await context.__aenter__()
                entered = True
                capabilities = await source.discover()
                if getattr(capabilities, "equity_historicals", False) is not True:
                    return result | {
                        "reason": "historicals_unavailable",
                        "action": "Restore equity historical data access and repeat the check.",
                    }
                response = await source.get_bars(("SPY",), start, end, now=checked_at)
        finally:
            failure = sys.exc_info()
            try:
                # End the request budget before cleanup so a late request
                # failure or success cannot be replaced by its expired timer.
                async with asyncio.timeout(_CLEANUP_TIMEOUT_SECONDS):
                    if entered:
                        await context.__aexit__(*failure)
                    elif (close := getattr(context, "close", None)) is not None:
                        await close()
            except Exception:
                # Preserve the original request/authentication failure or
                # caller cancellation if cleanup fails or reaches its limit.
                if failure[0] is None:
                    raise
    except Exception as exc:
        fields = feed_error_fields(exc)
        if fields["reason"] == "authentication_required":
            fields["action"] = (
                "Reauthenticate Robinhood with `uv run --no-sync robinhood-mcp-wrapper "
                "auth login --force`, then repeat the feed health check. Stored credentials alone "
                "do not verify feed access."
            )
        else:
            fields["action"] = (
                "Check Robinhood connectivity and service availability, then repeat the check."
            )
        if isinstance(exc, TimeoutError):
            fields["reason"] = "feed_timeout"
        return result | fields | {"error_type": type(exc).__name__}

    bars = response.get("SPY", []) if isinstance(response, dict) else []
    if not isinstance(bars, (list, tuple)):
        bars = []
    valid = [
        bar
        for bar in bars
        if isinstance(bar, Bar)
        and bar.symbol == "SPY"
        and bar.start == start
        and bar.end == end
        and bar.end <= checked_at
        and bar.volume > 0
        and getattr(bar, "interpolated", False) is False
    ]
    result.update(received_bars=len(bars), valid_bars=len(valid))
    if len(valid) != 1:
        return result | {
            "status": "incomplete",
            "reason": "missing_completed_probe_bar",
            "action": (
                "Verify genuine five-minute SPY coverage at the expected previous-session "
                "close and repeat the check."
            ),
        }
    return result | {
        "status": "ready",
        "reason": "historical_probe_succeeded",
        "action": "Historical access is verified; current freshness still needs a live scan.",
    }
