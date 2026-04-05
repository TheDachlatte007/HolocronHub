from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Optional


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_market_history_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS market_history (
                symbol TEXT NOT NULL,
                range_key TEXT NOT NULL,
                interval_key TEXT NOT NULL,
                provider TEXT,
                warning TEXT,
                fetched_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (symbol, range_key, interval_key)
            );
            CREATE INDEX IF NOT EXISTS idx_market_history_fetched_at
            ON market_history(fetched_at DESC);
            """
        )


def upsert_market_history(
    db_path: Path,
    *,
    symbol: str,
    range_key: str,
    interval_key: str,
    provider: str,
    series: list[dict[str, Any]],
    warning: Optional[str] = None,
    fetched_at: str,
) -> None:
    sym = str(symbol or "").strip().upper()
    if not sym or not isinstance(series, list) or not series:
        return
    ensure_market_history_db(db_path)
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO market_history (
                symbol, range_key, interval_key, provider, warning, fetched_at, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, range_key, interval_key) DO UPDATE SET
                provider=excluded.provider,
                warning=excluded.warning,
                fetched_at=excluded.fetched_at,
                payload_json=excluded.payload_json
            """,
            (
                sym,
                str(range_key or "1mo").strip().lower(),
                str(interval_key or "1d").strip().lower(),
                str(provider or "").strip(),
                str(warning or "").strip() or None,
                str(fetched_at or "").strip(),
                json.dumps(series, ensure_ascii=False),
            ),
        )


def get_market_history(
    db_path: Path,
    *,
    symbol: str,
    range_key: str,
    interval_key: str,
) -> Optional[dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    ensure_market_history_db(db_path)
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT provider, warning, fetched_at, payload_json
            FROM market_history
            WHERE symbol = ? AND range_key = ? AND interval_key = ?
            """,
            (
                sym,
                str(range_key or "1mo").strip().lower(),
                str(interval_key or "1d").strip().lower(),
            ),
        ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(row["payload_json"] or "[]")
    except Exception:
        return None
    if not isinstance(payload, list) or not payload:
        return None
    return {
        "provider": row["provider"] or "",
        "warning": row["warning"] or None,
        "fetched_at": row["fetched_at"] or None,
        "series": payload,
    }
