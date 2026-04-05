from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tldr_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tldr_issues (
                issue_id TEXT PRIMARY KEY,
                gmail_id TEXT NOT NULL,
                gmail_thread_id TEXT,
                message_id TEXT,
                newsletter_slug TEXT,
                newsletter_name TEXT,
                subject TEXT,
                sender TEXT,
                received_at TEXT,
                snippet TEXT,
                html_content TEXT,
                reader_html TEXT,
                text_content TEXT,
                sections_json TEXT,
                items_json TEXT,
                unread INTEGER NOT NULL DEFAULT 0,
                starred INTEGER NOT NULL DEFAULT 0,
                local_read INTEGER NOT NULL DEFAULT 0,
                imported_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tldr_issues_received
            ON tldr_issues(received_at DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tldr_issues_newsletter
            ON tldr_issues(newsletter_slug, received_at DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tldr_issues_unread
            ON tldr_issues(local_read, unread, received_at DESC)
            """
        )


def upsert_tldr_issue(db_path: Path, issue: dict[str, Any]) -> None:
    payload = dict(issue or {})
    payload.setdefault("sections_json", "[]")
    payload.setdefault("items_json", "[]")
    payload.setdefault("unread", 0)
    payload.setdefault("starred", 0)
    payload.setdefault("local_read", 0)
    payload.setdefault("imported_at", payload.get("updated_at") or payload.get("received_at") or "")
    payload.setdefault("updated_at", payload.get("received_at") or payload.get("imported_at") or "")
    with _connect(db_path) as conn:
        existing = conn.execute(
            "SELECT local_read FROM tldr_issues WHERE issue_id = ?",
            (payload.get("issue_id"),),
        ).fetchone()
        local_read = int(existing["local_read"]) if existing else int(payload.get("local_read") or 0)
        conn.execute(
            """
            INSERT INTO tldr_issues (
                issue_id, gmail_id, gmail_thread_id, message_id, newsletter_slug, newsletter_name,
                subject, sender, received_at, snippet, html_content, reader_html, text_content,
                sections_json, items_json, unread, starred, local_read, imported_at, updated_at
            ) VALUES (
                :issue_id, :gmail_id, :gmail_thread_id, :message_id, :newsletter_slug, :newsletter_name,
                :subject, :sender, :received_at, :snippet, :html_content, :reader_html, :text_content,
                :sections_json, :items_json, :unread, :starred, :local_read, :imported_at, :updated_at
            )
            ON CONFLICT(issue_id) DO UPDATE SET
                gmail_id = excluded.gmail_id,
                gmail_thread_id = excluded.gmail_thread_id,
                message_id = excluded.message_id,
                newsletter_slug = excluded.newsletter_slug,
                newsletter_name = excluded.newsletter_name,
                subject = excluded.subject,
                sender = excluded.sender,
                received_at = excluded.received_at,
                snippet = excluded.snippet,
                html_content = excluded.html_content,
                reader_html = excluded.reader_html,
                text_content = excluded.text_content,
                sections_json = excluded.sections_json,
                items_json = excluded.items_json,
                unread = excluded.unread,
                starred = excluded.starred,
                local_read = :preserved_local_read,
                imported_at = excluded.imported_at,
                updated_at = excluded.updated_at
            """,
            {
                **payload,
                "preserved_local_read": local_read,
                "local_read": local_read,
            },
        )


def _row_to_issue(row: sqlite3.Row) -> dict[str, Any]:
    issue = dict(row)
    issue["unread"] = bool(issue.get("unread"))
    issue["starred"] = bool(issue.get("starred"))
    issue["local_read"] = bool(issue.get("local_read"))
    for key in ("sections_json", "items_json"):
        raw = issue.pop(key, "[]")
        try:
            issue[key.replace("_json", "")] = json.loads(raw or "[]")
        except Exception:
            issue[key.replace("_json", "")] = []
    return issue


def list_tldr_issues(
    db_path: Path,
    *,
    newsletter: str | None = None,
    q: str | None = None,
    unread_only: bool = False,
    limit: int = 50,
) -> list[dict[str, Any]]:
    ensure_tldr_db(db_path)
    sql = """
        SELECT *
        FROM tldr_issues
        WHERE 1 = 1
    """
    params: list[Any] = []
    if newsletter:
        sql += " AND newsletter_slug = ?"
        params.append(str(newsletter).strip().lower())
    if q:
        needle = f"%{str(q).strip().lower()}%"
        sql += " AND (LOWER(subject) LIKE ? OR LOWER(snippet) LIKE ? OR LOWER(newsletter_name) LIKE ?)"
        params.extend([needle, needle, needle])
    if unread_only:
        sql += " AND (local_read = 0 AND unread = 1)"
    sql += " ORDER BY received_at DESC LIMIT ?"
    params.append(max(1, min(int(limit or 50), 500)))
    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_issue(row) for row in rows]


def get_tldr_issue(db_path: Path, issue_id: str) -> dict[str, Any] | None:
    ensure_tldr_db(db_path)
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM tldr_issues WHERE issue_id = ?",
            (str(issue_id or "").strip(),),
        ).fetchone()
    if not row:
        return None
    return _row_to_issue(row)


def mark_tldr_issue_read(db_path: Path, issue_id: str, *, read: bool = True) -> bool:
    ensure_tldr_db(db_path)
    with _connect(db_path) as conn:
        cur = conn.execute(
            "UPDATE tldr_issues SET local_read = ? WHERE issue_id = ?",
            (1 if read else 0, str(issue_id or "").strip()),
        )
        return cur.rowcount > 0


def tldr_summary(db_path: Path) -> dict[str, Any]:
    ensure_tldr_db(db_path)
    with _connect(db_path) as conn:
        total = int(conn.execute("SELECT COUNT(*) FROM tldr_issues").fetchone()[0])
        unread = int(
            conn.execute("SELECT COUNT(*) FROM tldr_issues WHERE unread = 1 AND local_read = 0").fetchone()[0]
        )
        latest = conn.execute(
            "SELECT received_at, newsletter_name, subject FROM tldr_issues ORDER BY received_at DESC LIMIT 1"
        ).fetchone()
        newsletters = conn.execute(
            """
            SELECT newsletter_slug, newsletter_name, COUNT(*) AS count,
                   SUM(CASE WHEN unread = 1 AND local_read = 0 THEN 1 ELSE 0 END) AS unread_count
            FROM tldr_issues
            GROUP BY newsletter_slug, newsletter_name
            ORDER BY MAX(received_at) DESC
            """
        ).fetchall()
    return {
        "issue_count": total,
        "unread_count": unread,
        "newsletter_count": len(newsletters),
        "last_issue_at": latest["received_at"] if latest else None,
        "last_issue_newsletter": latest["newsletter_name"] if latest else None,
        "latest_issue": dict(latest) if latest else None,
        "newsletters": [dict(row) for row in newsletters],
    }
