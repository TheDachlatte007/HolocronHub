from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


_CARD_FIELDS = (
    "id",
    "deck",
    "category",
    "skill",
    "prompt",
    "answer",
    "example",
    "explanation",
    "source",
    "tags",
)


def _connect(db_path: Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


@contextmanager
def _connection(db_path: Path):
    conn = _connect(db_path)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def _utc_now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc_now(value).isoformat()


def init_learning_db(db_path: Path) -> None:
    with _connection(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS learning_cards (
                id TEXT PRIMARY KEY,
                deck TEXT NOT NULL,
                category TEXT NOT NULL,
                skill TEXT NOT NULL,
                prompt TEXT NOT NULL,
                answer TEXT NOT NULL,
                example TEXT NOT NULL,
                explanation TEXT NOT NULL,
                source TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                content_updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS learning_progress (
                card_id TEXT PRIMARY KEY REFERENCES learning_cards(id) ON DELETE CASCADE,
                state TEXT NOT NULL DEFAULT 'new',
                due_at TEXT,
                interval_days INTEGER NOT NULL DEFAULT 0,
                ease REAL NOT NULL DEFAULT 2.5,
                review_count INTEGER NOT NULL DEFAULT 0,
                lapse_count INTEGER NOT NULL DEFAULT 0,
                last_review_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS learning_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_id TEXT NOT NULL REFERENCES learning_cards(id) ON DELETE CASCADE,
                rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 4),
                reviewed_at TEXT NOT NULL,
                previous_due_at TEXT,
                next_due_at TEXT NOT NULL,
                previous_interval_days INTEGER NOT NULL,
                next_interval_days INTEGER NOT NULL,
                previous_ease REAL NOT NULL,
                next_ease REAL NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_learning_cards_category ON learning_cards(category, id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_learning_progress_due ON learning_progress(due_at, review_count)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_learning_reviews_time ON learning_reviews(reviewed_at, card_id)"
        )


def _validated_seed(seed_path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(Path(seed_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Unable to read learning seed: {exc}") from exc
    if not isinstance(payload, list) or not payload:
        raise ValueError("Learning seed must be a non-empty JSON array")

    cards: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(payload):
        if not isinstance(raw, dict):
            raise ValueError(f"Learning seed row {index} must be an object")
        missing = [field for field in _CARD_FIELDS if field not in raw]
        if missing:
            raise ValueError(f"Learning seed row {index} is missing: {', '.join(missing)}")
        card = {field: raw[field] for field in _CARD_FIELDS}
        for field in _CARD_FIELDS[:-1]:
            if not isinstance(card[field], str) or not card[field].strip():
                raise ValueError(f"Learning seed row {index} has invalid {field}")
            card[field] = card[field].strip()
        if not isinstance(card["tags"], list) or not all(
            isinstance(tag, str) and tag.strip() for tag in card["tags"]
        ):
            raise ValueError(f"Learning seed row {index} has invalid tags")
        card["tags"] = list(dict.fromkeys(tag.strip() for tag in card["tags"]))
        if card["id"] in seen:
            raise ValueError(f"Duplicate learning card id: {card['id']}")
        seen.add(card["id"])
        cards.append(card)
    return cards


def sync_seed_cards(db_path: Path, seed_path: Path) -> int:
    cards = _validated_seed(seed_path)
    init_learning_db(db_path)
    updated_at = _iso(datetime.now(timezone.utc))
    with _connection(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        for card in cards:
            conn.execute(
                """
                INSERT INTO learning_cards (
                    id, deck, category, skill, prompt, answer, example,
                    explanation, source, tags_json, content_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    deck = excluded.deck,
                    category = excluded.category,
                    skill = excluded.skill,
                    prompt = excluded.prompt,
                    answer = excluded.answer,
                    example = excluded.example,
                    explanation = excluded.explanation,
                    source = excluded.source,
                    tags_json = excluded.tags_json,
                    content_updated_at = excluded.content_updated_at
                """,
                (
                    card["id"],
                    card["deck"],
                    card["category"],
                    card["skill"],
                    card["prompt"],
                    card["answer"],
                    card["example"],
                    card["explanation"],
                    card["source"],
                    json.dumps(card["tags"], ensure_ascii=False),
                    updated_at,
                ),
            )
            conn.execute(
                "INSERT OR IGNORE INTO learning_progress(card_id) VALUES (?)",
                (card["id"],),
            )
    return len(cards)


def _decode_card(row: sqlite3.Row, *, now: datetime) -> dict[str, Any]:
    card = dict(row)
    raw_tags = card.pop("tags_json", "[]")
    try:
        card["tags"] = json.loads(raw_tags or "[]")
    except json.JSONDecodeError:
        card["tags"] = []
    if int(card.get("review_count") or 0) == 0:
        status = "new"
    elif card.get("due_at") and card["due_at"] <= _iso(now):
        status = "due"
    elif int(card.get("interval_days") or 0) >= 21:
        status = "mastered"
    else:
        status = "scheduled"
    card["status"] = status
    return card


def _card_select() -> str:
    return """
        SELECT c.id, c.deck, c.category, c.skill, c.prompt, c.answer,
               c.example, c.explanation, c.source, c.tags_json,
               p.state, p.due_at, p.interval_days, p.ease,
               p.review_count, p.lapse_count, p.last_review_at
        FROM learning_cards c
        JOIN learning_progress p ON p.card_id = c.id
    """


def learning_session(
    db_path: Path,
    *,
    limit: int = 20,
    category: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    if isinstance(limit, bool) or not 1 <= int(limit) <= 100:
        raise ValueError("limit must be between 1 and 100")
    init_learning_db(db_path)
    current = _utc_now(now)
    current_iso = _iso(current)
    sql = _card_select() + " WHERE (p.review_count = 0 OR p.due_at <= ?)"
    params: list[Any] = [current_iso]
    if category:
        sql += " AND c.category = ?"
        params.append(str(category).strip())
    sql += """
        ORDER BY
            CASE WHEN p.review_count > 0 AND p.due_at <= ? THEN 0 ELSE 1 END,
            CASE WHEN p.review_count > 0 THEN p.due_at ELSE c.id END,
            c.id
        LIMIT ?
    """
    params.extend([current_iso, int(limit)])
    with _connection(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_decode_card(row, now=current) for row in rows]


def list_learning_cards(
    db_path: Path,
    *,
    query: str | None = None,
    category: str | None = None,
    status: str | None = None,
    limit: int = 500,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    if isinstance(limit, bool) or not 1 <= int(limit) <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    normalized_status = str(status or "").strip().lower()
    if normalized_status and normalized_status not in {"new", "due", "scheduled", "mastered"}:
        raise ValueError("status must be new, due, scheduled, or mastered")
    init_learning_db(db_path)
    current = _utc_now(now)
    current_iso = _iso(current)
    sql = _card_select() + " WHERE 1 = 1"
    params: list[Any] = []
    if query and str(query).strip():
        needle = f"%{str(query).strip().lower()}%"
        sql += """
            AND (
                LOWER(c.prompt) LIKE ? OR LOWER(c.answer) LIKE ? OR
                LOWER(c.example) LIKE ? OR LOWER(c.explanation) LIKE ?
            )
        """
        params.extend([needle, needle, needle, needle])
    if category:
        sql += " AND c.category = ?"
        params.append(str(category).strip())
    if normalized_status == "new":
        sql += " AND p.review_count = 0"
    elif normalized_status == "due":
        sql += " AND p.review_count > 0 AND p.due_at IS NOT NULL AND p.due_at <= ?"
        params.append(current_iso)
    elif normalized_status == "mastered":
        sql += """
            AND p.review_count > 0
            AND (p.due_at IS NULL OR p.due_at > ?)
            AND p.interval_days >= 21
        """
        params.append(current_iso)
    elif normalized_status == "scheduled":
        sql += """
            AND p.review_count > 0
            AND (p.due_at IS NULL OR p.due_at > ?)
            AND p.interval_days < 21
        """
        params.append(current_iso)
    sql += " ORDER BY c.category, c.id LIMIT ?"
    params.append(int(limit))
    with _connection(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_decode_card(row, now=current) for row in rows]


def _next_schedule(
    *,
    rating: int,
    interval_days: int,
    ease: float,
    is_new: bool,
    now: datetime,
) -> tuple[str, datetime, int, float, bool]:
    if rating == 1:
        return "learning", now + timedelta(minutes=10), 0, max(1.3, ease - 0.20), True
    if rating == 2:
        interval = max(1, round(max(interval_days, 1) * 1.2))
        return "review", now + timedelta(days=interval), interval, max(1.3, ease - 0.15), False
    if rating == 3:
        interval = 3 if is_new else max(1, round(interval_days * ease))
        return "review", now + timedelta(days=interval), interval, ease, False
    interval = 7 if is_new else max(2, round(interval_days * ease * 1.3))
    return "review", now + timedelta(days=interval), interval, ease + 0.15, False


def record_learning_review(
    db_path: Path,
    card_id: str,
    rating: int,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    if isinstance(rating, bool) or not isinstance(rating, int) or rating not in {1, 2, 3, 4}:
        raise ValueError("rating must be an integer from 1 to 4")
    identifier = str(card_id or "").strip()
    if not identifier:
        raise KeyError("Unknown learning card")
    init_learning_db(db_path)
    current = _utc_now(now)
    reviewed_at = _iso(current)
    with _connection(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM learning_progress WHERE card_id = ?",
            (identifier,),
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown learning card: {identifier}")
        previous_interval = int(row["interval_days"] or 0)
        previous_ease = float(row["ease"] or 2.5)
        state, next_due, next_interval, next_ease, lapsed = _next_schedule(
            rating=rating,
            interval_days=previous_interval,
            ease=previous_ease,
            is_new=int(row["review_count"] or 0) == 0,
            now=current,
        )
        due_at = _iso(next_due)
        conn.execute(
            """
            UPDATE learning_progress
            SET state = ?, due_at = ?, interval_days = ?, ease = ?,
                review_count = review_count + 1,
                lapse_count = lapse_count + ?, last_review_at = ?
            WHERE card_id = ?
            """,
            (state, due_at, next_interval, next_ease, 1 if lapsed else 0, reviewed_at, identifier),
        )
        conn.execute(
            """
            INSERT INTO learning_reviews (
                card_id, rating, reviewed_at, previous_due_at, next_due_at,
                previous_interval_days, next_interval_days, previous_ease, next_ease
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identifier,
                rating,
                reviewed_at,
                row["due_at"],
                due_at,
                previous_interval,
                next_interval,
                previous_ease,
                next_ease,
            ),
        )
        updated = conn.execute(
            "SELECT * FROM learning_progress WHERE card_id = ?",
            (identifier,),
        ).fetchone()
    result = dict(updated)
    result["rating"] = rating
    return result


def _streak_days(review_dates: Iterable[str], *, today: date) -> int:
    parsed: set[date] = set()
    for value in review_dates:
        try:
            parsed.add(date.fromisoformat(str(value)))
        except (TypeError, ValueError):
            continue
    if today in parsed:
        cursor = today
    elif today - timedelta(days=1) in parsed:
        cursor = today - timedelta(days=1)
    else:
        return 0
    streak = 0
    while cursor in parsed:
        streak += 1
        cursor -= timedelta(days=1)
    return streak


def learning_summary(db_path: Path, *, now: datetime | None = None) -> dict[str, Any]:
    init_learning_db(db_path)
    current = _utc_now(now)
    current_iso = _iso(current)
    today_iso = current.date().isoformat()
    with _connection(db_path) as conn:
        totals = conn.execute(
            """
            SELECT
                COUNT(*) AS total_cards,
                SUM(CASE WHEN p.review_count = 0 THEN 1 ELSE 0 END) AS new_cards,
                SUM(CASE WHEN p.review_count > 0 AND p.due_at <= ? THEN 1 ELSE 0 END) AS due_cards,
                SUM(CASE WHEN p.interval_days >= 21 THEN 1 ELSE 0 END) AS mastered_cards
            FROM learning_cards c
            JOIN learning_progress p ON p.card_id = c.id
            """,
            (current_iso,),
        ).fetchone()
        reviews_today = int(
            conn.execute(
                "SELECT COUNT(*) FROM learning_reviews WHERE substr(reviewed_at, 1, 10) = ?",
                (today_iso,),
            ).fetchone()[0]
        )
        review_dates = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT substr(reviewed_at, 1, 10) FROM learning_reviews ORDER BY 1 DESC"
            ).fetchall()
        ]
        categories = [
            dict(row)
            for row in conn.execute(
                """
                SELECT c.category,
                       COUNT(*) AS total_cards,
                       SUM(CASE WHEN p.review_count > 0 THEN 1 ELSE 0 END) AS reviewed_cards,
                       SUM(CASE WHEN p.review_count > 0 AND p.due_at <= ? THEN 1 ELSE 0 END) AS due_cards
                FROM learning_cards c
                JOIN learning_progress p ON p.card_id = c.id
                GROUP BY c.category
                ORDER BY MIN(c.id)
                """,
                (current_iso,),
            ).fetchall()
        ]
    return {
        "total_cards": int(totals["total_cards"] or 0),
        "new_cards": int(totals["new_cards"] or 0),
        "due_cards": int(totals["due_cards"] or 0),
        "mastered_cards": int(totals["mastered_cards"] or 0),
        "reviews_today": reviews_today,
        "streak_days": _streak_days(review_dates, today=current.date()),
        "categories": categories,
    }
