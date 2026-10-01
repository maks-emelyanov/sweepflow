import signal
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest

from sweepflow import scheduled
from sweepflow.sessions import SessionCalendar


@pytest.fixture(scope="module")
def calendar():
    return SessionCalendar()


@pytest.mark.parametrize(
    "at,active",
    [
        ("2025-06-03T12:59:59+00:00", False),
        ("2025-06-03T13:00:00+00:00", True),
        ("2025-06-03T20:09:59+00:00", True),
        ("2025-06-03T20:10:00+00:00", False),
        ("2025-07-04T15:00:00+00:00", False),  # Independence Day
        ("2025-07-05T15:00:00+00:00", False),  # Saturday
        ("2025-11-28T18:09:59+00:00", True),  # 13:00 early close
        ("2025-11-28T18:10:00+00:00", False),
        ("2025-03-07T14:00:00+00:00", True),  # Before DST
        ("2025-03-10T13:00:00+00:00", True),  # After DST
        ("2025-03-10T12:59:59+00:00", False),
    ],
)
def test_exchange_window(calendar, at, active):
    assert bool(scheduled.active_session(datetime.fromisoformat(at), calendar)) == active


def test_requires_aware_timestamp(calendar):
    with pytest.raises(ValueError, match="timezone"):
        scheduled.active_session(datetime(2025, 6, 3), calendar)


def test_check_and_inactive_modes_never_start_child(monkeypatch, calendar, capsys):
    monkeypatch.setattr(scheduled, "SessionCalendar", lambda: calendar)
    monkeypatch.setattr(
        scheduled, "supervise", lambda *args: pytest.fail("unexpected child launch")
    )
    monkeypatch.setattr(scheduled, "utc_now", lambda: datetime(2025, 7, 4, 15, tzinfo=UTC))
    assert scheduled.main(["--check"]) == 1
    assert scheduled.main([]) == 0
    assert "scheduler_idle" in capsys.readouterr().out
    monkeypatch.setattr(scheduled, "utc_now", lambda: datetime(2025, 6, 3, 15, tzinfo=UTC))
    assert scheduled.main(["--check"]) == 0


def test_active_mode_runs_full_paper_cli(monkeypatch, calendar):
    monkeypatch.setattr(scheduled, "SessionCalendar", lambda: calendar)
    monkeypatch.setattr(scheduled, "utc_now", lambda: datetime(2025, 11, 28, 15, tzinfo=UTC))
    calls = []

    def supervise(command, deadline):
        calls.append((command, deadline))
        return 7

    monkeypatch.setattr(scheduled, "supervise", supervise)
    assert (
        scheduled.main(
            ["--config", "a.toml", "--env-file", "b.env", "--db", "c.sqlite", "--allow-shorts"]
        )
        == 7
    )
    assert calls == [
        (
            [
                sys.executable,
                "-u",
                "-m",
                "sweepflow",
                "--config",
                "a.toml",
                "paper",
                "--env-file",
                "b.env",
                "--db",
                "c.sqlite",
                "--allow-shorts",
            ],
            datetime(2025, 11, 28, 18, 10, tzinfo=UTC),
        )
    ]


class FakeProcess:
    pid = 12345

    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)
        self.waits = []

    def wait(self, timeout):
        self.waits.append(timeout)
        outcome = next(self.outcomes)
        if callable(outcome):
            return outcome()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture
def supervisor(monkeypatch):
    now = datetime(2025, 6, 3, 15, tzinfo=UTC)
    monkeypatch.setattr(scheduled, "utc_now", lambda: now)
    handlers = {signal.SIGTERM: signal.SIG_DFL, signal.SIGINT: signal.SIG_DFL}
    original = handlers.copy()

    def install(signum, handler):
        previous = handlers[signum]
        handlers[signum] = handler
        return previous

    monkeypatch.setattr(scheduled.signal, "signal", install)
    group_signals = []
    monkeypatch.setattr(scheduled.os, "killpg", lambda pid, sig: group_signals.append((pid, sig)))
    launches = []

    def launch(process):
        def popen(command, **kwargs):
            launches.append((command, kwargs))
            return process

        monkeypatch.setattr(scheduled.subprocess, "Popen", popen)

    return now, handlers, original, group_signals, launches, launch


@pytest.mark.parametrize("exitcode,expected", [(0, 1), (-2, 1), (7, 7)])
def test_early_exit_requests_restart_and_cleans_group(supervisor, exitcode, expected):
    now, handlers, original, group_signals, launches, launch = supervisor
    process = FakeProcess([exitcode, exitcode])
    launch(process)
    assert scheduled.supervise(["child"], now + timedelta(hours=1)) == expected
    assert launches[0][1]["start_new_session"] is True
    assert launches[0][1]["env"]["PYTHONUNBUFFERED"] == "1"
    assert group_signals == [(12345, signal.SIGINT), (12345, signal.SIGKILL)]
    assert handlers == original


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_service_signal_gracefully_stops_child(supervisor, signum):
    now, handlers, original, group_signals, _, launch = supervisor

    def shutdown():
        handlers[signum](signum, None)
        raise subprocess.TimeoutExpired("child", 1)

    process = FakeProcess([shutdown, 0])
    launch(process)
    assert scheduled.supervise(["child"], now + timedelta(hours=1)) == 0
    assert process.waits == [1, 120]
    assert group_signals == [(12345, signal.SIGINT), (12345, signal.SIGKILL)]
    assert handlers == original


def test_expired_window_never_launches(supervisor):
    now, handlers, original, group_signals, launches, launch = supervisor
    launch(FakeProcess([]))
    assert scheduled.supervise(["child"], now) == 0
    assert launches == group_signals == []
    assert handlers == original


def test_session_end_stops_existing_child(monkeypatch, supervisor):
    now, _, _, group_signals, _, launch = supervisor
    deadline = now + timedelta(seconds=1)
    ticks = iter([now, deadline])
    monkeypatch.setattr(scheduled, "utc_now", lambda: next(ticks))
    process = FakeProcess([0])
    launch(process)
    assert scheduled.supervise(["child"], deadline) == 0
    assert process.waits == [120]
    assert group_signals[0] == (12345, signal.SIGINT)


def test_stubborn_child_escalates_and_reaps(supervisor):
    _, _, _, group_signals, _, _ = supervisor
    process = FakeProcess(
        [subprocess.TimeoutExpired("child", 120), subprocess.TimeoutExpired("child", 5), -9]
    )
    scheduled.stop_child(process)
    assert process.waits == [120, 5, 5]
    assert [sig for _, sig in group_signals] == [
        signal.SIGINT,
        signal.SIGTERM,
        signal.SIGKILL,
        signal.SIGKILL,
    ]


def test_exited_process_group_is_safe(monkeypatch):
    def vanished(*args):
        raise ProcessLookupError

    monkeypatch.setattr(scheduled.os, "killpg", vanished)
    scheduled.stop_child(FakeProcess([0]))


def test_real_child_gets_graceful_interrupt(tmp_path):
    checkpoint = tmp_path / "shutdown.txt"
    child = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "try:\n"
        "    while True: time.sleep(0.05)\n"
        "except KeyboardInterrupt:\n"
        "    Path(sys.argv[1]).write_text('cleanup completed')\n"
    )
    assert (
        scheduled.supervise(
            [sys.executable, "-c", child, str(checkpoint)],
            datetime.now(UTC) + timedelta(seconds=1),
        )
        == 0
    )
    assert checkpoint.read_text() == "cleanup completed"
