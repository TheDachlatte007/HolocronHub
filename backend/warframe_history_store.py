from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_warframe_history_db(db_path: Path) -> None:
    conn = _connect(db_path)
    try:
        with conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS warframe_market_snapshots (
                    platform TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    item_name TEXT,
                    captured_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (platform, slug, captured_at)
                );
                CREATE INDEX IF NOT EXISTS idx_warframe_snapshots_lookup
                ON warframe_market_snapshots(platform, slug, captured_at DESC);
                """
            )
    finally:
        conn.close()


def upsert_warframe_snapshot(
    db_path: Path,
    *,
    platform: str,
    slug: str,
    item_name: str,
    snapshot: dict[str, Any],
) -> None:
    platform_key = str(platform or "pc").strip().lower()
    slug_key = str(slug or "").strip()
    captured_at = str(snapshot.get("captured_at") or "").strip()
    if not slug_key or not captured_at:
        return
    ensure_warframe_history_db(db_path)
    conn = _connect(db_path)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO warframe_market_snapshots (
                    platform, slug, item_name, captured_at, payload_json
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(platform, slug, captured_at) DO NOTHING
                """,
                (
                    platform_key,
                    slug_key,
                    str(item_name or slug_key.replace("_", " ")).strip(),
                    captured_at,
                    json.dumps(snapshot, ensure_ascii=False),
                ),
            )
    finally:
        conn.close()


def get_warframe_snapshots(db_path: Path, *, platform: str, slug: str, limit: int = 2880) -> list[dict[str, Any]]:
    platform_key = str(platform or "pc").strip().lower()
    slug_key = str(slug or "").strip()
    if not slug_key:
        return []
    ensure_warframe_history_db(db_path)
    conn = _connect(db_path)
    try:
        with conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM warframe_market_snapshots
                WHERE platform = ? AND slug = ?
                ORDER BY captured_at DESC
                LIMIT ?
                """,
                (platform_key, slug_key, max(1, min(int(limit), 10000))),
            ).fetchall()
    finally:
        conn.close()
    snapshots: list[dict[str, Any]] = []
    for row in reversed(rows):
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except Exception:
            continue
        if isinstance(payload, dict):
            snapshots.append(payload)
    return snapshots
