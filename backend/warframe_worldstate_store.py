from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_warframe_worldstate_db(db_path: Path) -> None:
    conn = _connect(db_path)
    try:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS warframe_worldstate_snapshots (
                platform TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                source TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (platform, captured_at)
            );
            CREATE INDEX IF NOT EXISTS idx_warframe_worldstate_recent
            ON warframe_worldstate_snapshots(platform, captured_at DESC);
            """
        )
    finally:
        conn.close()


def upsert_warframe_worldstate(
    db_path: Path,
    *,
    platform: str,
    captured_at: str,
    source: str,
    payload: dict[str, Any],
    retention_days: int = 14,
) -> None:
    if not isinstance(payload, dict):
        return
    platform_key = str(platform or "pc").strip().lower()
    captured_key = str(captured_at or "").strip()
    if not captured_key:
        return
    ensure_warframe_worldstate_db(db_path)
    conn = _connect(db_path)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO warframe_worldstate_snapshots (
                    platform, captured_at, source, payload_json
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(platform, captured_at) DO UPDATE SET
                    source=excluded.source,
                    payload_json=excluded.payload_json
                """,
                (
                    platform_key,
                    captured_key,
                    str(source or "warframestat"),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
            try:
                cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(retention_days)))
                conn.execute(
                    "DELETE FROM warframe_worldstate_snapshots WHERE platform = ? AND captured_at < ?",
                    (platform_key, cutoff.isoformat()),
                )
            except Exception:
                # Keep the fresh snapshot even if an old timestamp cannot be parsed.
                pass
    finally:
        conn.close()


def get_latest_warframe_worldstate(
    db_path: Path,
    *,
    platform: str,
    max_age_seconds: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    platform_key = str(platform or "pc").strip().lower()
    if not db_path.exists():
        return None
    ensure_warframe_worldstate_db(db_path)
    conn = _connect(db_path)
    try:
        with conn:
            row = conn.execute(
                """
                SELECT captured_at, source, payload_json
                FROM warframe_worldstate_snapshots
                WHERE platform = ?
                ORDER BY captured_at DESC
                LIMIT 1
                """,
                (platform_key,),
            ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    try:
        payload = json.loads(row["payload_json"] or "{}")
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    captured_at = str(row["captured_at"] or "")
    age_seconds: Optional[int] = None
    try:
        captured_dt = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
        if captured_dt.tzinfo is None:
            captured_dt = captured_dt.replace(tzinfo=timezone.utc)
        age_seconds = max(0, int((datetime.now(timezone.utc) - captured_dt).total_seconds()))
    except Exception:
        pass
    if max_age_seconds is not None and age_seconds is not None and age_seconds > max_age_seconds:
        return None
    return {
        "payload": payload,
        "captured_at": captured_at,
        "source": str(row["source"] or "warframestat"),
        "age_seconds": age_seconds,
    }
