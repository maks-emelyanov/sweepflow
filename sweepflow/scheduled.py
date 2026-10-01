"""Run the full paper workflow during exchange-calendar session windows."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

from .models import aware
from .sessions import NEW_YORK, Session, SessionCalendar

START_LEAD = timedelta(minutes=30)
STOP_LAG = timedelta(minutes=10)
GRACE_SECONDS = 120


def utc_now() -> datetime:
    return datetime.now(UTC)


def log(event: str, **fields: object) -> None:
    print(json.dumps({"event": event, **fields}), flush=True)


def active_session(now: datetime, calendar: SessionCalendar) -> Session | None:
    """Include preopen startup and postclose reconciliation, end-exclusive."""
    aware(now, "now")
    session = calendar.session(now.astimezone(NEW_YORK).date())
    if session and session.open - START_LEAD <= now < session.close + STOP_LAG:
        return session
    return None


def _signal_group(process: subprocess.Popen, signum: int) -> None:
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        pass


def stop_child(process: subprocess.Popen) -> None:
    """Let asyncio finish reconciliation cleanup, then reap all descendants."""
    try:
        _signal_group(process, signal.SIGINT)
        try:
            process.wait(timeout=GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            log("scheduler_shutdown_timeout", timeout_seconds=GRACE_SECONDS)
            _signal_group(process, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _signal_group(process, signal.SIGKILL)
                process.wait(timeout=5)
    finally:
        # A child may exit before its MCP descendants. Do not leave them behind.
        _signal_group(process, signal.SIGKILL)


def supervise(command: list[str], deadline: datetime) -> int:
    """Forward service shutdown and return failure for unexpected child exits."""
    stopping = Event()
    previous_handlers = {}
    process = None

    def request_stop(signum: int, frame: object) -> None:
        stopping.set()

    try:
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[signum] = signal.signal(signum, request_stop)
        # Recheck after handler setup: never start after the session deadline.
        if stopping.is_set() or utc_now() >= deadline:
            return 0
        process = subprocess.Popen(
            command,
            start_new_session=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        log("scheduler_started", pid=process.pid, stop_at=deadline.isoformat())
        while not stopping.is_set() and utc_now() < deadline:
            try:
                returncode = process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                continue
            if stopping.is_set() or utc_now() >= deadline:
                return 0
            log("scheduler_child_exited", returncode=returncode)
            return returncode if returncode > 0 else 1
        log("scheduler_stopping", reason="signal" if stopping.is_set() else "session_end")
        return 0
    finally:
        try:
            if process is not None:
                stop_child(process)
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Check the calendar without APIs")
    parser.add_argument("--config", type=Path, default=Path("config/strategy.toml"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--db", type=Path, default=Path("data/alpaca-paper.sqlite"))
    parser.add_argument("--allow-shorts", action="store_true")
    args = parser.parse_args(argv)
    session = active_session(utc_now(), SessionCalendar())
    if args.check:
        return 0 if session else 1
    if session is None:
        log("scheduler_idle", reason="outside_session_window")
        return 0
    command = [
        sys.executable,
        "-u",
        "-m",
        "sweepflow",
        "--config",
        str(args.config),
        "paper",
        "--env-file",
        str(args.env_file),
        "--db",
        str(args.db),
    ]
    if args.allow_shorts:
        command.append("--allow-shorts")
    return supervise(command, session.close + STOP_LAG)


if __name__ == "__main__":
    raise SystemExit(main())
