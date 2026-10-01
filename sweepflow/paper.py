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
                    report(
                        "paper_preparation_error",
                        **error_fields(exc),
                        consecutive=preparation_errors,
                    )
                else:
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
                        raise
                else:
                    if not snapshot.get("market_open"):
                        report("paper_scan_skipped", reason="market_closed")
                        if once:
                            finished_normally = True
                            return
                        continue
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
                and (feed_errors or monotonic() - last_feed_success > 300 + max_signal_age)
            ):
                try:
                    broker.invalidate_pending(set())
                    entries_canceled_for_feed = True
                except Exception as exc:
                    control_error(exc)

            if task is None and snapshot.get("market_open") and monotonic() >= next_scan:
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
                and monotonic() >= next_prepare
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
