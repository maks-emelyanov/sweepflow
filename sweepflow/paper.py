"""Alpaca paper runtime: broker reconciliation runs while Robinhood scans await data."""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import os
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sweepflow.alpaca import AlpacaAPIError
from sweepflow.config import AppConfig
from sweepflow.integrations import create_robinhood_data_source
from sweepflow.monitor import bind_monitor, prepare_session, scan
from sweepflow.sessions import NEW_YORK, SessionCalendar
from sweepflow.storage import Journal, dumps


def feed_error_fields(exc: Exception) -> dict[str, str]:
    """Classify feed failures without optional imports or upstream error text."""
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        error = pending.pop()
        if id(error) in seen:
            continue
        seen.add(id(error))
        if any(cls.__name__ == "AuthenticationRequired" for cls in type(error).__mro__):
            return {
                "reason": "authentication_required",
                "action": (
                    "Reauthenticate Robinhood with `uv run --no-sync "
                    "robinhood-mcp-wrapper auth login --force`."
                ),
            }
        if isinstance(error, BaseExceptionGroup):
            pending.extend(error.exceptions)
        if error.__cause__ is not None:
            pending.append(error.__cause__)
        elif error.__context__ is not None and not error.__suppress_context__:
            pending.append(error.__context__)
    return {
        "reason": "feed_unavailable",
        "action": "Check the Robinhood data connection and service availability.",
    }


@contextmanager
def paper_account_lock(account_id: str) -> Iterator[None]:
    """One runner per OS user/account, even when different journals are supplied."""
    digest = hashlib.sha256(account_id.encode()).hexdigest()[:24]
    path = Path("/tmp") / f"sweepflow-alpaca-{os.getuid()}-{digest}.lock"
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another Alpaca paper runner is using this account") from None
        yield
    finally:
        os.close(descriptor)


async def run_paper(
    broker,
    symbols: Sequence[str],
    config: AppConfig,
    journal: Journal,
    *,
    once: bool = False,
    poll_seconds: float = 5,
    scan_seconds: float = 30,
    max_errors: int = 3,
    max_signal_age: float = 90,
    source_factory: Callable = create_robinhood_data_source,
    emit: Callable = lambda result: print(dumps(result), flush=True),
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] | None = None,
) -> None:
    """Poll account/orders independently of scans; emit only current strategy entries.

    The coroutine owns the journal and broker on one thread. Robinhood requests
    yield, and reconstruction yields every 200 candles. Feed outages cancel
    unfilled entries while bracket exits and periodic account sync remain active.
    """
    bind_monitor(journal, config, symbols, "alpaca-paper")
    calendar = SessionCalendar()
    task = None
    task_kind = None
    prepared_day = None
    next_prepare = 0.0
    preparation_errors = 0
    phase = None
    finished_normally = False
    next_scan = 0.0
    feed_errors = 0
    sync_errors = 0
    sync_retry_deferred = False
    execution_errors = 0
    loop = asyncio.get_running_loop()
    monotonic = monotonic or loop.time
    last_feed_success = monotonic()
    entries_canceled_for_feed = False
    next_control_retry = 0.0
    feed_retry_at = 0.0
    auth_errors = 0
    outage_started = None
    outage_started_at = None
    outage_errors = 0
    outage_reason = None

    def error_fields(exc: Exception) -> dict[str, object]:
        # Upstream bodies and arbitrary exception text can echo credentials.
        fields: dict[str, object] = {"error_type": type(exc).__name__}
        if isinstance(exc, AlpacaAPIError):
            fields.update(exc.safe_fields())
        return fields

    def control_error(exc: Exception) -> None:
        nonlocal execution_errors, next_scan, next_control_retry
        execution_errors += 1
        next_scan = max(next_scan, monotonic() + max(scan_seconds, poll_seconds))
        next_control_retry = monotonic() + poll_seconds
        report(
            "paper_execution_error",
            **error_fields(exc),
            consecutive=execution_errors,
            entries_paused=True,
        )

    def report(event: str, **fields: object) -> None:
        payload = {"event": event, **fields}
        journal.record(payload)
        emit(payload)

    def feed_failure(exc: Exception, stage: str) -> None:
        nonlocal feed_retry_at, auth_errors, outage_started, outage_started_at
        nonlocal outage_errors, outage_reason
        at = monotonic()
        if outage_started is None:
            outage_started = at
            outage_started_at = clock()
        details = feed_error_fields(exc)
        outage_reason = details["reason"]
        outage_errors += 1
        delay = max(scan_seconds, poll_seconds)
        if outage_reason == "authentication_required":
            auth_errors += 1
            # Repeated authentication attempts cannot repair expired consent.
            # Back off across both preparation and scans, including the open.
            delay = min(900.0, max(delay, 60.0 * 2 ** min(auth_errors - 1, 4)))
        else:
            auth_errors = 0
        feed_retry_at = at + delay
        report(
            "paper_feed_health",
            status="unavailable",
            stage=stage,
            **(error_fields(exc) | details),
            since=outage_started_at,
            outage_seconds=max(0.0, at - outage_started),
            consecutive=outage_errors,
            retry_after_seconds=delay,
            next_retry_at=clock() + timedelta(seconds=delay),
            entries_paused=True,
            broker_reconciliation_active=True,
        )

    def feed_recovered(stage: str) -> None:
        nonlocal feed_retry_at, auth_errors, outage_started, outage_started_at
        nonlocal outage_errors, outage_reason
        if outage_started is not None:
            report(
                "paper_feed_health",
                status="recovered",
                stage=stage,
                previous_reason=outage_reason,
                since=outage_started_at,
                outage_seconds=max(0.0, monotonic() - outage_started),
                errors=outage_errors,
                action="Feed requests succeeded; normal data validation and entry checks resume.",
                broker_reconciliation_active=True,
            )
        feed_retry_at = 0.0
        auth_errors = 0
        outage_started = None
        outage_started_at = None
        outage_errors = 0
        outage_reason = None

    async def read_history():
        # A stuck data connection must not occupy the preparation slot forever.
        async with asyncio.timeout(300), source_factory() as source:
            return await prepare_session(source, symbols, config, journal, clock=clock)

    async def read_signals():
        async with source_factory() as source:
            return await scan(
                source,
                symbols,
                config,
                journal,
                mode="alpaca-paper",
                max_signal_age=timedelta(seconds=max_signal_age),
                clock=clock,
            )

    try:
        while True:
            try:
                snapshot = broker.sync()
                if sync_errors or sync_retry_deferred:
                    report("alpaca_sync_recovered", errors=sync_errors)
                sync_errors = 0
                sync_retry_deferred = False
                emit({"event": "alpaca_sync", **snapshot})
            except Exception as exc:
                details = error_fields(exc)
                deferred = (
                    isinstance(exc, AlpacaAPIError) and details.get("reason") == "retry_deferred"
                )
                if deferred:
                    # A broker cooldown performs no request. Keep waiting without
                    # spending the consecutive-error budget or flooding the log.
                    if not sync_retry_deferred:
                        report(
                            "paper_broker_retry_deferred",
                            **details,
                            consecutive=sync_errors,
                            entries_paused=True,
                        )
                    sync_retry_deferred = True
                else:
                    sync_errors += 1
                    sync_retry_deferred = False
                    report("alpaca_sync_error", **details, consecutive=sync_errors)
                if (not deferred and sync_errors >= max_errors) or once:
                    raise
                await asyncio.sleep(poll_seconds)
                continue

            now = clock()
            session = calendar.session(now.astimezone(NEW_YORK).date())
            preopen = session is not None and now < session.open
            if snapshot.get("market_open"):
                current_phase = "market_open"
            elif preopen and not once:
                current_phase = "waiting_for_open" if prepared_day == session.label else "preparing"
            else:
                current_phase = "market_closed"
            if current_phase != phase:
                phase = current_phase
                if phase == "market_open":
                    # Overnight waiting is not a live-feed outage.
                    last_feed_success = monotonic()
                report(
                    "paper_phase",
                    phase=phase,
                    at=now,
                    market_open_at=session.open if session else None,
                    market_close_at=session.close if session else None,
                )

            if task is not None and task_kind == "prepare" and task.done():
                completed, task = task, None
                next_prepare = monotonic() + max(scan_seconds, poll_seconds)
                try:
                    preparation = completed.result()
                except Exception as exc:
                    preparation_errors += 1
                    feed_failure(exc, "preparation")
                    report(
                        "paper_preparation_error",
                        **error_fields(exc),
                        consecutive=preparation_errors,
                    )
                else:
                    if preparation.status in {"ready", "incomplete"}:
                        feed_recovered("preparation")
                    if preparation.status == "incomplete":
                        report(
                            "paper_feed_health",
                            status="degraded",
                            stage="preparation",
                            reason="incomplete_history",
                            missing_intraday_symbols=preparation.missing_symbols,
                            missing_daily_symbols=preparation.missing_daily_symbols,
                            action=(
                                "Retry previous-session history. Missing daily levels block "
                                "entries for affected symbols; intraday gaps are diagnostic."
                            ),
                            broker_reconciliation_active=True,
                        )
                    if preparation_errors:
                        report("paper_preparation_recovered", errors=preparation_errors)
                    preparation_errors = 0
                    if preparation.status == "ready" and session is not None:
                        prepared_day = session.label
                    report("paper_preparation", result=preparation)

            if task is not None and task.done():
                completed, task = task, None
                try:
                    result = completed.result()
                except Exception as exc:
                    feed_errors += 1
                    feed_failure(exc, "scan")
                    # A fast failed scan must not create an immediate retry loop.
                    next_scan = monotonic() + max(scan_seconds, poll_seconds)
                    # An unavailable strategy feed cannot justify resting entries.
                    try:
                        broker.invalidate_pending(set())
                        entries_canceled_for_feed = True
                    except Exception as management_error:
                        control_error(management_error)
                    report(
                        "paper_feed_error",
                        **error_fields(exc),
                        consecutive=feed_errors,
                        entries_paused=True,
                    )
                    if once:
                        details = feed_error_fields(exc)
                        raise RuntimeError(
                            f"Market-data feed unavailable ({details['reason']}). "
                            f"{details['action']}"
                        ) from None
                else:
                    if not snapshot.get("market_open") or result.status == "market_closed":
                        report("paper_scan_skipped", reason="market_closed")
                        if once:
                            finished_normally = True
                            return
                        continue
                    feed_recovered("scan")
                    if feed_errors:
                        report("paper_feed_recovered", errors=feed_errors)
                    feed_errors = 0
                    last_feed_success = monotonic()
                    entries_canceled_for_feed = False
                    try:
                        broker.invalidate_pending(set(result.active_signal_ids))
                        submissions = []
                        for signal in result.eligible_signals:
                            # Broker also checks wall time after its own network synchronization.
                            now = clock()
                            if (
                                now < signal.created_at
                                or now >= signal.expires_at
                                or (now - signal.created_at).total_seconds() > max_signal_age
                            ):
                                continue
                            submissions.append(broker.submit(signal))
                            await asyncio.sleep(0)
                    except Exception as exc:
                        control_error(exc)
                        if once:
                            raise
                    else:
                        if execution_errors:
                            report("paper_execution_recovered", errors=execution_errors)
                        execution_errors = 0
                        emit({"event": "paper_scan", "scan": result, "submissions": submissions})
                if once:
                    emit({"event": "alpaca_sync", **broker.sync()})
                    finished_normally = True
                    return

            if (
                snapshot.get("market_open")
                and not entries_canceled_for_feed
                and monotonic() >= next_control_retry
                and (
                    feed_errors
                    or outage_started is not None
                    or monotonic() - last_feed_success > 300 + max_signal_age
                )
            ):
                try:
                    broker.invalidate_pending(set())
                    entries_canceled_for_feed = True
                except Exception as exc:
                    control_error(exc)

            if (
                task is None
                and snapshot.get("market_open")
                and monotonic() >= max(next_scan, feed_retry_at)
            ):
                task_kind = "scan"
                # Cadence measures scan starts. A slow full-universe read already
                # consumed the interval and needs no additional post-scan delay.
                next_scan = monotonic() + scan_seconds
                task = asyncio.create_task(read_signals())
            elif (
                task is None
                and not once
                and not snapshot.get("market_open")
                and preopen
                and prepared_day != session.label
                and monotonic() >= max(next_prepare, feed_retry_at)
            ):
                task_kind = "prepare"
                report("paper_preparation_started", session=session.label)
                task = asyncio.create_task(read_history())
            elif once and task is None and not snapshot.get("market_open"):
                finished_normally = True
                return

            if task is None:
                await asyncio.sleep(poll_seconds)
            else:
                # Unlike wait_for, this does not cancel a slow full-universe scan.
                await asyncio.wait({task}, timeout=poll_seconds)
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        # Stop new exposure on orderly shutdown; preserve exits on filled positions.
        # If Alpaca is unreachable, durable state and server-side brackets survive.
        try:
            if not finished_normally:
                broker.invalidate_pending(set())
        except Exception as exc:
            report("paper_shutdown_sync_error", **error_fields(exc))
