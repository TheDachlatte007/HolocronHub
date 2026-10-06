import json
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path


@contextmanager
def open_test_db(path):
    conn = sqlite3.connect(path)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


class LearningStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "learning.db"
        self.seed = self.root / "learning.seed.json"
        self.cards = [
            self._card("card-a", "Measurement & Data", "What is a baseline?"),
            self._card("card-b", "Engineering Practice", "What does verify mean?"),
            self._card("card-c", "Measurement & Data", "What is an outlier?"),
        ]
        self._write_seed(self.cards)

    @staticmethod
    def _card(card_id, category, prompt):
        return {
            "id": card_id,
            "deck": "Engineering English",
            "category": category,
            "skill": "recognition",
            "prompt": prompt,
            "answer": f"Answer for {card_id}.",
            "example": f"Example for {card_id}.",
            "explanation": f"Explanation for {card_id}.",
            "source": "test",
            "tags": ["lang::en", "test"],
        }

    def _write_seed(self, cards):
        self.seed.write_text(json.dumps(cards), encoding="utf-8")

    def _store(self):
        try:
            from backend import learning_store
        except ImportError as exc:
            self.fail(f"backend.learning_store is missing: {exc}")

        return learning_store

    def test_initialization_is_additive_and_enables_wal(self):
        store = self._store()

        store.init_learning_db(self.db)
        store.init_learning_db(self.db)

        with open_test_db(self.db) as conn:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
            journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        self.assertTrue({"learning_cards", "learning_progress", "learning_reviews"}.issubset(tables))
        self.assertEqual("wal", journal_mode.lower())

    def test_seed_updates_content_without_resetting_progress_or_history(self):
        store = self._store()
        now = datetime(2026, 10, 7, 9, 0, tzinfo=timezone.utc)
        self.assertEqual(3, store.sync_seed_cards(self.db, self.seed))
        reviewed = store.record_learning_review(self.db, "card-a", 3, now=now)
        revised = [dict(card) for card in self.cards]
        revised[0]["prompt"] = "Define a measurement baseline."
        self._write_seed(revised)

        self.assertEqual(3, store.sync_seed_cards(self.db, self.seed))
        cards = store.list_learning_cards(self.db, query="measurement baseline", now=now)

        self.assertEqual("Define a measurement baseline.", cards[0]["prompt"])
        self.assertEqual(reviewed["due_at"], cards[0]["due_at"])
        self.assertEqual(1, cards[0]["review_count"])
        with open_test_db(self.db) as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM learning_reviews").fetchone()[0])

    def test_new_card_ratings_apply_exact_schedule(self):
        store = self._store()
        store.sync_seed_cards(self.db, self.seed)
        now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
        expected = {
            1: (now + timedelta(minutes=10), 0, 2.30, 1),
            2: (now + timedelta(days=1), 1, 2.35, 0),
            3: (now + timedelta(days=3), 3, 2.50, 0),
            4: (now + timedelta(days=7), 7, 2.65, 0),
        }

        for index, rating in enumerate((1, 2, 3, 4), start=1):
            card_id = f"rating-{rating}"
            extra = self._card(card_id, "Engineering Practice", f"Rating {rating}")
            self._write_seed(self.cards + [extra])
            store.sync_seed_cards(self.db, self.seed)
            result = store.record_learning_review(self.db, card_id, rating, now=now)
            due, interval, ease, lapses = expected[rating]
            self.assertEqual(due.isoformat(), result["due_at"])
            self.assertEqual(interval, result["interval_days"])
            self.assertAlmostEqual(ease, result["ease"], places=2)
            self.assertEqual(lapses, result["lapse_count"])
            self.assertEqual(1, result["review_count"])

    def test_review_ratings_scale_existing_interval(self):
        store = self._store()
        store.sync_seed_cards(self.db, self.seed)
        now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
        cases = {
            2: (5, 2.35),
            3: (10, 2.50),
            4: (13, 2.65),
        }

        for rating, (expected_interval, expected_ease) in cases.items():
            card_id = f"card-{chr(95 + rating)}"
            with open_test_db(self.db) as conn:
                conn.execute(
                    """
                    UPDATE learning_progress
                    SET state = 'review', interval_days = 4, ease = 2.5,
                        review_count = 2, due_at = ?
                    WHERE card_id = ?
                    """,
                    ((now - timedelta(days=1)).isoformat(), card_id),
                )
            result = store.record_learning_review(self.db, card_id, rating, now=now)
            self.assertEqual(expected_interval, result["interval_days"])
            self.assertEqual((now + timedelta(days=expected_interval)).isoformat(), result["due_at"])
            self.assertAlmostEqual(expected_ease, result["ease"], places=2)

    def test_review_after_again_uses_reviewed_minimums_and_ease_floor(self):
        store = self._store()
        store.sync_seed_cards(self.db, self.seed)
        now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)

        store.record_learning_review(self.db, "card-a", 1, now=now)
        good = store.record_learning_review(
            self.db, "card-a", 3, now=now + timedelta(minutes=10)
        )
        store.record_learning_review(self.db, "card-b", 1, now=now)
        easy = store.record_learning_review(
            self.db, "card-b", 4, now=now + timedelta(minutes=10)
        )
        for index in range(10):
            floor = store.record_learning_review(
                self.db, "card-c", 1, now=now + timedelta(minutes=index)
            )

        self.assertEqual(1, good["interval_days"])
        self.assertEqual((now + timedelta(days=1, minutes=10)).isoformat(), good["due_at"])
        self.assertEqual(2, easy["interval_days"])
        self.assertEqual((now + timedelta(days=2, minutes=10)).isoformat(), easy["due_at"])
        self.assertAlmostEqual(1.3, floor["ease"], places=2)
        self.assertEqual(10, floor["lapse_count"])

    def test_invalid_rating_and_unknown_card_leave_history_unchanged(self):
        store = self._store()
        store.sync_seed_cards(self.db, self.seed)

        with self.assertRaises(ValueError):
            store.record_learning_review(self.db, "card-a", 5)
        with self.assertRaises(KeyError):
            store.record_learning_review(self.db, "missing", 3)

        with open_test_db(self.db) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM learning_reviews").fetchone()[0])

    def test_session_returns_overdue_before_new_and_respects_category_and_limit(self):
        store = self._store()
        self._write_seed(
            self.cards
            + [self._card("card-d", "Measurement & Data", "What is repeatability?")]
        )
        store.sync_seed_cards(self.db, self.seed)
        now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
        store.record_learning_review(self.db, "card-a", 3, now=now - timedelta(days=5))
        store.record_learning_review(self.db, "card-c", 4, now=now)

        session = store.learning_session(
            self.db,
            limit=2,
            category="Measurement & Data",
            now=now,
        )

        self.assertEqual(["card-a", "card-d"], [card["id"] for card in session])
        self.assertEqual(["due", "new"], [card["status"] for card in session])
        self.assertEqual(2, len(session))
        self.assertNotIn("card-c", [card["id"] for card in session])

    def test_card_status_filter_is_applied_before_limit(self):
        store = self._store()
        store.sync_seed_cards(self.db, self.seed)
        now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
        store.record_learning_review(self.db, "card-c", 3, now=now - timedelta(days=5))

        due = store.list_learning_cards(self.db, status="due", limit=1, now=now)

        self.assertEqual(["card-c"], [card["id"] for card in due])

    def test_reviews_persist_append_only_and_summary_reports_today_and_streak(self):
        store = self._store()
        store.sync_seed_cards(self.db, self.seed)
        day_one = datetime(2026, 10, 5, 8, 0, tzinfo=timezone.utc)
        day_two = datetime(2026, 10, 6, 8, 0, tzinfo=timezone.utc)
        today = datetime(2026, 10, 7, 8, 0, tzinfo=timezone.utc)
        store.record_learning_review(self.db, "card-a", 3, now=day_one)
        store.record_learning_review(self.db, "card-a", 2, now=day_two)
        store.record_learning_review(self.db, "card-b", 4, now=today)

        summary = store.learning_summary(self.db, now=today)
        reopened = store.list_learning_cards(self.db, now=today)

        self.assertEqual(1, summary["reviews_today"])
        self.assertEqual(3, summary["streak_days"])
        self.assertEqual(3, summary["total_cards"])
        self.assertEqual(2, next(card for card in reopened if card["id"] == "card-a")["review_count"])
        with open_test_db(self.db) as conn:
            rows = conn.execute(
                "SELECT card_id, rating FROM learning_reviews ORDER BY reviewed_at, id"
            ).fetchall()
        self.assertEqual([("card-a", 3), ("card-a", 2), ("card-b", 4)], rows)


if __name__ == "__main__":
    unittest.main()
