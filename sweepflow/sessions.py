"""Exchange scheduled regular sessions and configurable setup windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars

from .config import StrategyConfig
from .models import Bar, aware

NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Session:
    label: date
    open: datetime
    close: datetime


class SessionCalendar:
    def __init__(self) -> None:
        self.calendar = exchange_calendars.get_calendar(
            "XNYS", start="1990-01-01", end="2050-12-31"
        )
        self._sessions: dict[date, Session | None] = {}

    def session(self, day: date) -> Session | None:
        if day in self._sessions:
            return self._sessions[day]
        label = day.isoformat()
        if not self.calendar.is_session(label):
            self._sessions[day] = None
            return None
        session = Session(
            day,
            self.calendar.session_open(label).to_pydatetime(),
            self.calendar.session_close(label).to_pydatetime(),
        )
        self._sessions[day] = session
        return session

    def previous_session(self, day: date) -> Session:
        label = self.calendar.previous_session(day.isoformat()).date()
        result = self.session(label)
        assert result is not None
        return result

    def session_for(self, timestamp: datetime) -> Session | None:
        aware(timestamp, "timestamp")
        session = self.session(timestamp.astimezone(NEW_YORK).date())
        return (
            session if session is not None and session.open <= timestamp < session.close else None
        )

    def is_regular_bar(self, bar: Bar) -> bool:
        session = self.session_for(bar.start)
        return bool(
            session
            and bar.end <= session.close
            and (bar.start - session.open) % bar.duration == timedelta(0)
        )

    def setup_window(
        self, timestamp: datetime, config: StrategyConfig
    ) -> tuple[datetime, datetime] | None:
        session = self.session_for(timestamp)
        if session is None:
            return None
        windows = (
            (
                session.open + timedelta(minutes=config.opening_start_minutes),
                min(session.open + timedelta(minutes=config.opening_end_minutes), session.close),
            ),
            (
                max(session.close - timedelta(minutes=config.closing_start_minutes), session.open),
                session.close - timedelta(minutes=config.closing_end_minutes),
            ),
        )
        for start, end in windows:
            if start <= timestamp < end:
                return start, end
        return None
