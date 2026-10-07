"""Manual farm journal. No provider calls, inferred prices, or net-profit claims.

All files live beside the caller's injected SQLite path; construction does not
touch disk. Connections close per operation (no background worker or WAL).
Sale totals replace the session's confirmed proceeds, never its drop estimate.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Callable

from pydantic import (
    BaseModel, ConfigDict, Field, StrictBool, StrictInt, StringConstraints,
    field_validator, model_validator,
)


Label = Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=200)]
Platinum = Annotated[Decimal, Field(ge=0, le=1000000, max_digits=9, decimal_places=2, allow_inf_nan=False)]


class FarmConflict(ValueError):
    """Another session is already active."""


class SessionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    route: Label | None = None
    target: Label | None = None
    started_at: datetime | None = None

    @field_validator("started_at", mode="before")
    @classmethod
    def timestamp(cls, value):
        if not isinstance(value, (str, datetime)):
            raise ValueError("Timestamp must be an ISO 8601 string with a timezone")
        try:
            parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        except ValueError as exc:
            raise ValueError("Timestamp must be ISO 8601 with a timezone") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("Timestamp must include a timezone")
        return parsed.astimezone(timezone.utc)


def _money(value):
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError("Platinum must be a finite JSON number, 0 to 1000000, with at most two decimals")
    return Decimal(str(value))


class SessionUpdate(SessionCreate):
    ended_at: datetime | None = None
    finish: StrictBool = False
    confirmed_sale_platinum: Platinum = Decimal(0)

    @field_validator("ended_at", mode="before")
    @classmethod
    def end_timestamp(cls, value):
        return None if value is None else cls.timestamp(value)

    @field_validator("confirmed_sale_platinum", mode="before")
    @classmethod
    def money(cls, value):
        return _money(value)

    @model_validator(mode="after")
    def not_empty(self):
        if not self.model_fields_set:
            raise ValueError("Supply at least one field to update")
        return self


class DropCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    item: Label
    quantity: StrictInt = Field(ge=1, le=1000000)
    estimated_unit_platinum: Platinum | None = None

    @field_validator("estimated_unit_platinum", mode="before")
    @classmethod
    def money(cls, value):
        return None if value is None else _money(value)


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _cents(value: Decimal) -> int:
    return int(value * 100)


def _elapsed(row, now):
    start = datetime.fromisoformat(row["started_at"])
    end = datetime.fromisoformat(row["ended_at"]) if row["ended_at"] else now
    return max(0, (end - start).total_seconds())


def _metrics(seconds, sale_cents, estimated_cents, priced_count, quantity, unvalued):
    return {
        "elapsed_seconds": seconds,
        "drop_quantity": quantity,
        "unvalued_quantity": unvalued,
        "valuation_complete": priced_count > 0 and unvalued == 0,
        "estimated_drop_platinum": estimated_cents / 100 if priced_count else None,
        "confirmed_sale_platinum": sale_cents / 100,
        "observed_sale_platinum_per_hour": round(sale_cents * 36 / seconds, 4) if seconds > 0 else None,
    }


class FarmJournalStore:
    def __init__(self, db_path: Path | str, *, clock: Callable[[], datetime] | None = None):
        self.db_path = Path(db_path)
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @contextmanager
    def _connection(self, *, write=False):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            # Additive initialization is also safe across separate router instances.
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS warframe_farm_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    route TEXT, target TEXT,
                    started_at TEXT NOT NULL, ended_at TEXT,
                    confirmed_sale_cents INTEGER NOT NULL DEFAULT 0 CHECK(confirmed_sale_cents >= 0)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS warframe_farm_one_active
                    ON warframe_farm_sessions ((1)) WHERE ended_at IS NULL;
                CREATE TABLE IF NOT EXISTS warframe_farm_drops (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL REFERENCES warframe_farm_sessions(id),
                    item TEXT NOT NULL,
                    quantity INTEGER NOT NULL CHECK(quantity BETWEEN 1 AND 1000000),
                    estimated_unit_cents INTEGER CHECK(estimated_unit_cents BETWEEN 0 AND 100000000),
                    recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS warframe_farm_drops_session ON warframe_farm_drops(session_id);
            """)
            conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            with conn:
                yield conn
        finally:
            conn.close()

    def _now(self):
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Clock must provide a timezone-aware datetime")
        return now.astimezone(timezone.utc)

    @staticmethod
    def _id(session_id):
        if type(session_id) is not int or not 1 <= session_id <= 9223372036854775807:
            raise ValueError("Session ID must be a positive 64-bit integer")

    @staticmethod
    def _time_bounds(started, ended, now):
        if started > now or (ended is not None and ended > now):
            raise ValueError("Session timestamps cannot be in the future")
        if ended is not None and ended < started:
            raise ValueError("End timestamp cannot precede start timestamp")

    @staticmethod
    def _rows(conn):
        return conn.execute("""
            SELECT s.*, COUNT(d.estimated_unit_cents) AS priced_count,
                COALESCE(SUM(d.quantity), 0) AS quantity,
                COALESCE(SUM(CASE WHEN d.estimated_unit_cents IS NULL THEN d.quantity ELSE 0 END), 0) AS unvalued,
                COALESCE(SUM(d.quantity * d.estimated_unit_cents), 0) AS estimated_cents
            FROM warframe_farm_sessions s LEFT JOIN warframe_farm_drops d ON d.session_id=s.id
            GROUP BY s.id ORDER BY s.id DESC
        """).fetchall()

    @staticmethod
    def _session(conn, row, now):
        seconds = _elapsed(row, now)
        result = {key: row[key] for key in ("id", "route", "target", "started_at", "ended_at")}
        result["status"] = "finished" if row["ended_at"] is not None else "active"
        result.update(_metrics(seconds, row["confirmed_sale_cents"], row["estimated_cents"],
                               row["priced_count"], row["quantity"], row["unvalued"]))
        result["drops"] = [{
            "id": drop["id"], "item": drop["item"], "quantity": drop["quantity"],
            "estimated_unit_platinum": drop["estimated_unit_cents"] / 100 if drop["estimated_unit_cents"] is not None else None,
            "recorded_at": drop["recorded_at"],
        } for drop in conn.execute("SELECT * FROM warframe_farm_drops WHERE session_id=? ORDER BY id", (row["id"],))]
        return result

    def _get(self, conn, session_id, now):
        row = next((row for row in self._rows(conn) if row["id"] == session_id), None)
        if row is None:
            raise KeyError(session_id)
        return self._session(conn, row, now)

    def create_session(self, payload: dict) -> dict:
        data = SessionCreate.model_validate(payload)
        with self._connection(write=True) as conn:
            now = self._now()
            started = data.started_at or now
            self._time_bounds(started, None, now)
            if conn.execute("SELECT 1 FROM warframe_farm_sessions WHERE ended_at IS NULL").fetchone():
                raise FarmConflict("Finish the active session before starting another")
            cursor = conn.execute("INSERT INTO warframe_farm_sessions(route,target,started_at) VALUES (?,?,?)",
                                  (data.route, data.target, _stamp(started)))
            return self._get(conn, cursor.lastrowid, now)

    def update_session(self, session_id: int, payload: dict) -> dict:
        """Partial update; confirmed_sale_platinum is a replacement TOTAL, not an increment.

        finish=true stops at server UTC now, idempotently. Explicit ended_at also
        finishes. Finished sessions can be corrected but never reopened.
        """
        self._id(session_id)
        data = SessionUpdate.model_validate(payload)
        fields = data.model_fields_set
        with self._connection(write=True) as conn:
            row = conn.execute("SELECT * FROM warframe_farm_sessions WHERE id=?", (session_id,)).fetchone()
            if row is None:
                raise KeyError(session_id)
            now = self._now()
            started = data.started_at if "started_at" in fields else datetime.fromisoformat(row["started_at"])
            ended = datetime.fromisoformat(row["ended_at"]) if row["ended_at"] else None
            if "ended_at" in fields:
                if data.ended_at is None and ended is not None:
                    raise ValueError("Finished sessions cannot be reopened")
                ended = data.ended_at
            if data.finish and ended is None:
                ended = now
            self._time_bounds(started, ended, now)
            conn.execute("""UPDATE warframe_farm_sessions SET route=?,target=?,started_at=?,ended_at=?,
                         confirmed_sale_cents=? WHERE id=?""", (
                data.route if "route" in fields else row["route"],
                data.target if "target" in fields else row["target"],
                _stamp(started), _stamp(ended) if ended is not None else None,
                _cents(data.confirmed_sale_platinum) if "confirmed_sale_platinum" in fields else row["confirmed_sale_cents"],
                session_id,
            ))
            return self._get(conn, session_id, now)

    def add_drop(self, session_id: int, payload: dict) -> dict:
        """Append a manual drop, including late entries on finished sessions."""
        self._id(session_id)
        data = DropCreate.model_validate(payload)
        with self._connection(write=True) as conn:
            if conn.execute("SELECT 1 FROM warframe_farm_sessions WHERE id=?", (session_id,)).fetchone() is None:
                raise KeyError(session_id)
            now = self._now()
            conn.execute("""INSERT INTO warframe_farm_drops(session_id,item,quantity,estimated_unit_cents,recorded_at)
                         VALUES (?,?,?,?,?)""", (session_id, data.item, data.quantity,
                         _cents(data.estimated_unit_platinum) if data.estimated_unit_platinum is not None else None, _stamp(now)))
            return self._get(conn, session_id, now)

    def list_sessions(self, *, limit: int = 50, offset: int = 0) -> dict:
        """Newest-created first; summary covers ALL sessions, not just the page.

        elapsed_seconds includes active elapsed time. Observed sale rate is total
        confirmed proceeds / total measured hours, null for zero elapsed time.
        Estimates cover only explicitly priced drops and are never added to sales.
        """
        if type(limit) is not int or not 1 <= limit <= 200 or type(offset) is not int or not 0 <= offset <= 1000000:
            raise ValueError("limit must be 1..200; offset must be 0..1000000")
        with self._connection() as conn:
            now = self._now()
            rows = self._rows(conn)
            seconds = sum(_elapsed(row, now) for row in rows)
            summary = _metrics(seconds, sum(row["confirmed_sale_cents"] for row in rows),
                               sum(row["estimated_cents"] for row in rows), sum(row["priced_count"] for row in rows),
                               sum(row["quantity"] for row in rows), sum(row["unvalued"] for row in rows))
            summary.update(session_count=len(rows), finished_count=sum(row["ended_at"] is not None for row in rows))
            active = next((row for row in rows if row["ended_at"] is None), None)
            return {
                "sessions": [self._session(conn, row, now) for row in rows[offset:offset + limit]],
                "active_session": self._session(conn, active, now) if active is not None else None,
                "summary": summary, "as_of": _stamp(now), "limit": limit, "offset": offset,
                "has_more": offset + limit < len(rows),
            }
