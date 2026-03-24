from __future__ import annotations

import json
import re
import sqlite3
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _search_text(entry: dict[str, Any]) -> str:
    return _normalize_text(" ".join([
        str(entry.get("session_key") or ""),
        str(entry.get("season") or ""),
        str(entry.get("year") or ""),
        str(entry.get("round") or ""),
        str(entry.get("meeting_name") or ""),
        str(entry.get("session_name") or ""),
        str(entry.get("circuit") or ""),
        str(entry.get("location") or ""),
        str(entry.get("country_name") or ""),
    ]))


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_f1_history_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS f1_sessions (
                session_key TEXT PRIMARY KEY,
                season TEXT,
                year INTEGER,
                round INTEGER,
                meeting_name TEXT,
                session_name TEXT,
                circuit TEXT,
                location TEXT,
                country_name TEXT,
                date_start TEXT,
                date_end TEXT,
                data_as_of TEXT,
                generated_at TEXT,
                source TEXT,
                live INTEGER DEFAULT 0,
                drivers_count INTEGER DEFAULT 0,
                search_text TEXT,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_f1_sessions_year_round ON f1_sessions(year, round);
            CREATE INDEX IF NOT EXISTS idx_f1_sessions_data_as_of ON f1_sessions(data_as_of DESC);
            CREATE INDEX IF NOT EXISTS idx_f1_sessions_search ON f1_sessions(search_text);
            """
        )


def upsert_f1_history_session(db_path: Path, entry: dict[str, Any], payload: dict[str, Any]) -> None:
    if not isinstance(entry, dict) or not isinstance(payload, dict):
        return
    session_key = str(entry.get("session_key") or payload.get("session_key") or "").strip()
    if not session_key:
        return
    ensure_f1_history_db(db_path)
    payload_json = json.dumps(payload, ensure_ascii=False)
    search_text = _search_text(entry)
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO f1_sessions (
                session_key, season, year, round, meeting_name, session_name, circuit, location,
                country_name, date_start, date_end, data_as_of, generated_at, source, live,
                drivers_count, search_text, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_key) DO UPDATE SET
                season=excluded.season,
                year=excluded.year,
                round=excluded.round,
                meeting_name=excluded.meeting_name,
                session_name=excluded.session_name,
                circuit=excluded.circuit,
                location=excluded.location,
                country_name=excluded.country_name,
                date_start=excluded.date_start,
                date_end=excluded.date_end,
                data_as_of=excluded.data_as_of,
                generated_at=excluded.generated_at,
                source=excluded.source,
                live=excluded.live,
                drivers_count=excluded.drivers_count,
                search_text=excluded.search_text,
                payload_json=excluded.payload_json
            """,
            (
                session_key,
                str(entry.get("season") or ""),
                entry.get("year"),
                entry.get("round"),
                str(entry.get("meeting_name") or ""),
                str(entry.get("session_name") or ""),
                str(entry.get("circuit") or ""),
                str(entry.get("location") or ""),
                str(entry.get("country_name") or ""),
                str(entry.get("date_start") or ""),
                str(entry.get("date_end") or ""),
                str(entry.get("data_as_of") or payload.get("data_as_of") or ""),
                str(entry.get("generated_at") or payload.get("generated_at") or ""),
                str(entry.get("source") or payload.get("source") or ""),
                1 if bool(entry.get("live") or payload.get("live")) else 0,
                int(entry.get("drivers_count") or payload.get("drivers_count") or 0),
                search_text,
                payload_json,
            ),
        )


def get_f1_history_session(db_path: Path, session_key: str) -> Optional[dict[str, Any]]:
    key = str(session_key or "").strip()
    if not key or not db_path.exists():
        return None
    with _connect(db_path) as conn:
        row = conn.execute("SELECT payload_json FROM f1_sessions WHERE session_key = ?", (key,)).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(row["payload_json"])
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _summarize_row(row: sqlite3.Row) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    try:
        parsed = json.loads(row["payload_json"])
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        payload = {}
    leaderboard = [item for item in list(payload.get("leaderboard") or [])[:5] if isinstance(item, dict)]
    return {
        "session_key": row["session_key"],
        "season": row["season"] or None,
        "year": row["year"],
        "round": row["round"],
        "meeting_name": row["meeting_name"],
        "session_name": row["session_name"],
        "circuit": row["circuit"],
        "location": row["location"],
        "country_name": row["country_name"],
        "date_start": row["date_start"] or None,
        "data_as_of": row["data_as_of"] or None,
        "source": row["source"] or "history_db",
        "live": bool(row["live"]),
        "drivers_count": int(row["drivers_count"] or 0),
        "leaderboard": deepcopy(leaderboard),
    }


def search_f1_history_sessions(db_path: Path, q: str = "", *, limit: int = 8) -> list[dict[str, Any]]:
    ensure_f1_history_db(db_path)
    query = _normalize_text(q)
    tokens = [token for token in query.split(" ") if token]
    sql = "SELECT * FROM f1_sessions"
    params: list[Any] = []
    if tokens:
        sql += " WHERE " + " AND ".join("search_text LIKE ?" for _ in tokens)
        params.extend(f"%{token}%" for token in tokens)
    sql += " ORDER BY COALESCE(data_as_of, generated_at) DESC LIMIT ?"
    params.append(max(8, min(int(limit or 8) * 6, 96)))
    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()

    if not query:
        return [_summarize_row(row) for row in rows[: max(1, min(int(limit or 8), 24))]]

    ranked: list[tuple[int, dict[str, Any]]] = []
    for row in rows:
        haystack = _normalize_text(row["search_text"] or "")
        if not haystack:
            continue
        score = 0
        if query in haystack:
            score += 70
        for token in tokens:
            if token in haystack:
                score += 14
            if token == "race" and "race" in haystack:
                score += 10
            if token.startswith("quali") and "qual" in haystack:
                score += 10
            if token.startswith("sprint") and "sprint" in haystack:
                score += 10
        if score <= 0:
            continue
        ranked.append((score, _summarize_row(row)))
    ranked.sort(key=lambda item: (item[0], str(item[1].get("data_as_of") or "")), reverse=True)
    return [item[1] for item in ranked[: max(1, min(int(limit or 8), 24))]]


def f1_history_summary(db_path: Path, *, limit: int = 6) -> dict[str, Any]:
    ensure_f1_history_db(db_path)
    with _connect(db_path) as conn:
        total_sessions = int(conn.execute("SELECT COUNT(*) FROM f1_sessions").fetchone()[0] or 0)
        total_seasons = int(conn.execute("SELECT COUNT(DISTINCT year) FROM f1_sessions WHERE year IS NOT NULL").fetchone()[0] or 0)
        latest_row = conn.execute(
            "SELECT data_as_of, generated_at FROM f1_sessions ORDER BY COALESCE(data_as_of, generated_at) DESC LIMIT 1"
        ).fetchone()
        recent_rows = conn.execute(
            "SELECT * FROM f1_sessions ORDER BY COALESCE(data_as_of, generated_at) DESC LIMIT ?",
            (max(1, min(int(limit or 6), 12)),),
        ).fetchall()
    latest_data_as_of = None
    if latest_row:
        latest_data_as_of = latest_row["data_as_of"] or latest_row["generated_at"]
    return {
        "stored_sessions": total_sessions,
        "stored_seasons": total_seasons,
        "latest_data_as_of": latest_data_as_of,
        "recent_sessions": [_summarize_row(row) for row in recent_rows],
    }
