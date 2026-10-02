"""SQLite decision journal, unique signal IDs, and persisted shadow-mode input bars."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path

from sweepflow.data import bar_from_dict, bar_to_dict
from sweepflow.models import Bar, Signal
from sweepflow.sessions import SessionCalendar


def json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def dumps(value) -> str:
    return json.dumps(value, default=json_default, sort_keys=True, allow_nan=False)


def fingerprint(value) -> str:
    return hashlib.sha256(dumps(value).encode()).hexdigest()


class Journal:
    def __init__(self, path: str | Path) -> None:
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(path))
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS bars (
                symbol TEXT NOT NULL, start TEXT NOT NULL, payload TEXT NOT NULL,
                PRIMARY KEY(symbol, start)
            );
            CREATE TABLE IF NOT EXISTS daily_bars (
                symbol TEXT NOT NULL, start TEXT NOT NULL, payload TEXT NOT NULL,
                PRIMARY KEY(symbol, start)
            );
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY, recorded_at TEXT NOT NULL,
                kind TEXT NOT NULL, payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY, created_at TEXT NOT NULL, payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS alpaca_state (
                account_id TEXT PRIMARY KEY, payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS quarantined (
                symbol TEXT NOT NULL, session TEXT NOT NULL, PRIMARY KEY(symbol, session)
            );
            CREATE TABLE IF NOT EXISTS bar_repairs (
                symbol TEXT NOT NULL, start TEXT NOT NULL, PRIMARY KEY(symbol, start)
            );
        """)
        self._bar_cache: dict[tuple[str, datetime], Bar] | None = None
        self._daily_bar_cache: dict[tuple[str, datetime], Bar] | None = None
        self._bar_data_version: int | None = None
        self._daily_calendar: SessionCalendar | None = None
        self._scan_cache = None
        self.bar_generation = 0

    def __enter__(self) -> Journal:
        return self

    def __exit__(self, *_):
        self.close()

    def close(self) -> None:
        self.connection.close()
        self._bar_cache = None
        self._daily_bar_cache = None
        self._scan_cache = None

    def get_alpaca_state(self, account_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT payload FROM alpaca_state WHERE account_id=?", (account_id,)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def bind_alpaca_account(self, account_id: str) -> None:
        if not account_id:
            raise ValueError("Alpaca account ID is required")
        row = self.connection.execute(
            "SELECT value FROM metadata WHERE key='alpaca_account'"
        ).fetchone()
        if row and row[0] != account_id:
            raise ValueError("Journal belongs to a different Alpaca paper account")
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO metadata VALUES ('alpaca_account', ?)", (account_id,)
            )

    def save_alpaca_state(self, account_id: str, state: dict) -> None:
        self.bind_alpaca_account(account_id)
        with self.connection:
            self.connection.execute(
                "INSERT INTO alpaca_state VALUES (?,?) ON CONFLICT(account_id) "
                "DO UPDATE SET payload=excluded.payload",
                (account_id, dumps(state)),
            )

    def bind(self, config: object) -> None:
        """Prevent accidental reuse of shadow checkpoints under different rules/universes."""
        value = fingerprint(config)
        row = self.connection.execute("SELECT value FROM metadata WHERE key='config'").fetchone()
        if row and row[0] != value:
            raise ValueError("Journal belongs to a different configuration; choose a new --db")
        with self.connection:
            self.connection.execute("INSERT OR IGNORE INTO metadata VALUES ('config', ?)", (value,))

    def record(self, event: dict) -> None:
        kind = str(event.get("event", event.get("status", event.get("kind", "decision"))))
        with self.connection:
            self.connection.execute(
                "INSERT INTO decisions(recorded_at,kind,payload) VALUES(?,?,?)",
                (datetime.now(UTC).isoformat(), kind, dumps(event)),
            )

    def save_signal(self, signal: Signal) -> bool:
        with self.connection:
            cursor = self.connection.execute(
                "INSERT OR IGNORE INTO signals VALUES(?,?,?)",
                (signal.id, signal.created_at.isoformat(), dumps(signal)),
            )
        return cursor.rowcount == 1

    def store_bars(
        self,
        bars: list[Bar],
        session: date,
        *,
        repaired_keys: set[tuple[str, datetime]] | None = None,
    ) -> set[str]:
        """Apply corrected candles and atomically audit their old/new values.

        Revisions trigger reconstruction by the monitor, not a session-long ban.
        Legacy quarantine rows remain available as historical audit records only.
        """
        cached = self._cached_bars()
        repaired_keys = repaired_keys or set()
        previous_repairs = self.repaired_bars_since()
        repair_changes = False
        revised = set()
        updates: dict[tuple[str, datetime], Bar] = {}
        with self.connection:
            for bar in bars:
                start = bar.start.astimezone(UTC)
                key = (bar.symbol, start)
                previous = updates.get(key, cached.get(key))
                if (key in repaired_keys) != (key in previous_repairs):
                    repair_changes = True
                    if key in repaired_keys:
                        self.connection.execute(
                            "INSERT OR IGNORE INTO bar_repairs VALUES (?,?)",
                            (bar.symbol, start.isoformat()),
                        )
                        previous_repairs.add(key)
                    else:
                        self.connection.execute(
                            "DELETE FROM bar_repairs WHERE symbol=? AND start=?",
                            (bar.symbol, start.isoformat()),
                        )
                        previous_repairs.discard(key)
                if previous == bar:
                    continue
                timestamp = start.isoformat()
                payload = dumps(bar_to_dict(bar))
                if previous is not None:
                    revised.add(bar.symbol)
                    self._record_bar_revision(previous, bar, session)
                self.connection.execute(
                    "INSERT INTO bars VALUES(?,?,?) ON CONFLICT(symbol,start) "
                    "DO UPDATE SET payload=excluded.payload",
                    (bar.symbol, timestamp, payload),
                )
                updates[key] = bar
        # Publish inputs only after candles and revision audits commit together.
        if updates or repair_changes:
            cached.update(updates)
            self.bar_generation += 1
        return revised

    def store_daily_bars(self, bars: list[Bar], session: date) -> set[str]:
        """Persist full regular-session daily candles separately from intraday inputs."""
        cached = self._cached_daily_bars()
        if self._daily_calendar is None:
            self._daily_calendar = SessionCalendar()
        revised = set()
        updates: dict[tuple[str, datetime], Bar] = {}
        with self.connection:
            for bar in bars:
                regular = self._daily_calendar.session_for(bar.start)
                if regular is None or bar.start != regular.open or bar.end != regular.close:
                    raise ValueError("Daily bars must cover a full regular exchange session")
                start = bar.start.astimezone(UTC)
                key = (bar.symbol, start)
                previous = updates.get(key, cached.get(key))
                if previous == bar:
                    continue
                if previous is not None:
                    revised.add(bar.symbol)
                    self._record_bar_revision(previous, bar, session, timeframe="day")
                self.connection.execute(
                    "INSERT INTO daily_bars VALUES(?,?,?) ON CONFLICT(symbol,start) "
                    "DO UPDATE SET payload=excluded.payload",
                    (bar.symbol, start.isoformat(), dumps(bar_to_dict(bar))),
                )
                updates[key] = bar
        if updates:
            cached.update(updates)
            self.bar_generation += 1
        return revised

    def _record_bar_revision(
        self, previous: Bar, bar: Bar, session: date, *, timeframe: str | None = None
    ) -> None:
        event = {
            "event": "data_revision",
            "symbol": bar.symbol,
            "session": session,
            "start": bar.start.astimezone(UTC).isoformat(),
            "changed_fields": [
                "duration_seconds" if field == "duration" else field
                for field in ("open", "high", "low", "close", "volume", "duration")
                if getattr(previous, field) != getattr(bar, field)
            ],
            "before": bar_to_dict(previous),
            "after": bar_to_dict(bar),
        }
        if timeframe is not None:
            event["timeframe"] = timeframe
        self.connection.execute(
            "INSERT INTO decisions(recorded_at,kind,payload) VALUES(?,?,?)",
            (datetime.now(UTC).isoformat(), "data_revision", dumps(event)),
        )

    def repaired_bars_since(self, start: datetime | None = None) -> set[tuple[str, datetime]]:
        if start is None:
            rows = self.connection.execute("SELECT symbol,start FROM bar_repairs")
        else:
            rows = self.connection.execute(
                "SELECT symbol,start FROM bar_repairs WHERE start>=?",
                (start.astimezone(UTC).isoformat(),),
            )
        return {(symbol, datetime.fromisoformat(at)) for symbol, at in rows}

    def quarantined(self, session: date) -> set[str]:
        """Read legacy quarantine history; these rows no longer restrict monitoring."""
        return {
            row[0]
            for row in self.connection.execute(
                "SELECT symbol FROM quarantined WHERE session=?", (session.isoformat(),)
            )
        }

    def _cached_bars(self) -> dict[tuple[str, datetime], Bar]:
        version = self.connection.execute("PRAGMA data_version").fetchone()[0]
        if self._bar_cache is None or version != self._bar_data_version:
            self._bar_cache = {
                (bar.symbol, bar.start.astimezone(UTC)): bar
                for (payload,) in self.connection.execute("SELECT payload FROM bars")
                for bar in (bar_from_dict(json.loads(payload)),)
            }
            self._daily_bar_cache = {
                (bar.symbol, bar.start.astimezone(UTC)): bar
                for (payload,) in self.connection.execute("SELECT payload FROM daily_bars")
                for bar in (bar_from_dict(json.loads(payload)),)
            }
            self._bar_data_version = version
            self.bar_generation += 1
        return self._bar_cache

    def _cached_daily_bars(self) -> dict[tuple[str, datetime], Bar]:
        self._cached_bars()
        assert self._daily_bar_cache is not None
        return self._daily_bar_cache

    def bars_since(self, start: datetime | None = None) -> list[Bar]:
        cutoff = start.astimezone(UTC) if start is not None else None
        return sorted(
            (bar for (_, at), bar in self._cached_bars().items() if cutoff is None or at >= cutoff),
            key=lambda bar: (bar.start, bar.symbol),
        )

    def daily_bars_since(self, start: datetime | None = None) -> list[Bar]:
        cutoff = start.astimezone(UTC) if start is not None else None
        return sorted(
            (
                bar
                for (_, at), bar in self._cached_daily_bars().items()
                if cutoff is None or at >= cutoff
            ),
            key=lambda bar: (bar.start, bar.symbol),
        )

    def events(self) -> list[dict]:
        return [
            json.loads(row[0])
            for row in self.connection.execute("SELECT payload FROM decisions ORDER BY id")
        ]
