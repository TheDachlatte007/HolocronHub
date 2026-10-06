import io
import json
import sqlite3
import tempfile
import unittest
import zipfile
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.learning_store import record_learning_review, sync_seed_cards


class LearningApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "data" / "learning.db"
        self.seed = Path(__file__).with_name("learning.seed.json")
        self.cards = json.loads(self.seed.read_text(encoding="utf-8"))

    def client(self, db=None, seed=None):
        try:
            from backend.learning_api import create_learning_router
        except ImportError as exc:
            self.fail(f"Learning router is missing: {exc}")
        app = FastAPI()
        app.include_router(create_learning_router(db or self.db, seed or self.seed))
        client = TestClient(app)
        self.addCleanup(client.close)
        return client

    def test_summary_seeds_injected_database_and_returns_category_progress(self):
        response = self.client().get("/api/learning/summary")
        self.assertEqual(response.status_code, 200)
        summary = response.json()
        self.assertEqual(summary["total_cards"], 120)
        self.assertEqual(summary["new_cards"], 120)
        self.assertEqual(summary["due_cards"], 0)
        self.assertEqual(summary["reviews_today"], 0)
        self.assertEqual(summary["streak_days"], 0)
        self.assertEqual(len(summary["categories"]), 4)
        self.assertTrue(all(row["total_cards"] == 30 for row in summary["categories"]))
        self.assertTrue(self.db.is_file())

    def test_session_returns_due_before_new_with_limit_and_category(self):
        sync_seed_cards(self.db, self.seed)
        due_id = self.cards[1]["id"]
        category = self.cards[1]["category"]
        record_learning_review(self.db, due_id, 1, now=datetime.now(timezone.utc) - timedelta(hours=1))
        client = self.client()
        response = client.get("/api/learning/session", params={"limit": 2, "category": category})
        self.assertEqual(response.status_code, 200)
        cards = response.json()
        self.assertEqual(len(cards), 2)
        self.assertEqual(cards[0]["id"], due_id)
        self.assertEqual(cards[0]["status"], "due")
        self.assertEqual(cards[1]["status"], "new")
        self.assertTrue(all(card["category"] == category for card in cards))
        self.assertEqual(len(client.get("/api/learning/session").json()), 20)

    def test_cards_search_category_and_status_filters(self):
        client = self.client()
        first = self.cards[0]
        response = client.get("/api/learning/cards", params={"query": first["prompt"], "category": first["category"], "status": "new"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(first["id"], [row["id"] for row in response.json()])
        self.assertTrue(all(row["category"] == first["category"] and row["status"] == "new" for row in response.json()))
        self.assertEqual(client.get("/api/learning/cards", params={"status": "scheduled"}).json(), [])
        self.assertEqual(len(client.get("/api/learning/cards", params={"category": "", "status": "", "query": ""}).json()), 120)

    def test_review_persists_schedule_history_and_summary_across_routers(self):
        client = self.client()
        card_id = self.cards[0]["id"]
        response = client.post("/api/learning/reviews", json={"card_id": card_id, "rating": 3})
        self.assertEqual(response.status_code, 200)
        progress = response.json()
        self.assertEqual(progress["card_id"], card_id)
        self.assertEqual(progress["interval_days"], 3)
        self.assertEqual(progress["review_count"], 1)
        self.assertEqual(datetime.fromisoformat(progress["due_at"]) - datetime.fromisoformat(progress["last_review_at"]), timedelta(days=3))
        summary = self.client().get("/api/learning/summary").json()
        self.assertEqual(summary["new_cards"], 119)
        self.assertEqual(summary["reviews_today"], 1)
        self.assertEqual(summary["streak_days"], 1)
        scheduled = client.get("/api/learning/cards", params={"status": "scheduled"}).json()
        self.assertEqual([card["id"] for card in scheduled], [card_id])
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(conn.execute("SELECT card_id, rating FROM learning_reviews").fetchall(), [(card_id, 3)])

    def test_all_four_ratings_are_accepted(self):
        client = self.client()
        for rating, interval in [(1, 0), (2, 1), (3, 3), (4, 7)]:
            with self.subTest(rating=rating):
                response = client.post("/api/learning/reviews", json={"card_id": self.cards[rating - 1]["id"], "rating": rating})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["interval_days"], interval)

    def test_session_rejects_invalid_limits(self):
        client = self.client()
        for limit in ["0", "-1", "101", "1.5", "abc"]:
            with self.subTest(limit=limit):
                response = client.get("/api/learning/session", params={"limit": limit})
                self.assertEqual(response.status_code, 422)
                self.assertIn("detail", response.json())

    def test_filters_reject_unknown_categories_and_statuses(self):
        client = self.client()
        for route in ["session", "cards"]:
            with self.subTest(route=route):
                self.assertEqual(client.get(f"/api/learning/{route}", params={"category": "unknown"}).status_code, 422)
        self.assertEqual(client.get("/api/learning/cards", params={"status": "unknown"}).status_code, 422)

    def test_reviews_reject_invalid_ratings_without_writing_history(self):
        client = self.client()
        client.get("/api/learning/summary")
        for rating in [0, 5, -1, True, False, "3", 3.0, None]:
            with self.subTest(rating=rating):
                response = client.post("/api/learning/reviews", json={"card_id": self.cards[0]["id"], "rating": rating})
                self.assertEqual(response.status_code, 422)
        for payload in [{}, {"card_id": "" , "rating": 3}, {"card_id": "   ", "rating": 3}, {"card_id": 123, "rating": 3}]:
            self.assertEqual(client.post("/api/learning/reviews", json=payload).status_code, 422)
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM learning_reviews").fetchone()[0], 0)

    def test_unknown_card_returns_404(self):
        response = self.client().post("/api/learning/reviews", json={"card_id": "missing", "rating": 3})
        self.assertEqual(response.status_code, 404)
        self.assertIn("detail", response.json())

    def test_unavailable_database_returns_503_for_every_endpoint(self):
        blocked = self.root / "blocked"
        blocked.mkdir()
        client = self.client(db=blocked)
        for route in ["summary", "session", "cards"]:
            with self.subTest(route=route):
                self.assertEqual(client.get(f"/api/learning/{route}").status_code, 503)
        response = client.post("/api/learning/reviews", json={"card_id": self.cards[0]["id"], "rating": 3})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "Learning store unavailable")

    def test_invalid_seed_returns_503_and_can_recover(self):
        seed = self.root / "seed.json"
        seed.write_text("invalid JSON", encoding="utf-8")
        client = self.client(seed=seed)
        self.assertEqual(client.get("/api/learning/summary").status_code, 503)
        seed.write_text(json.dumps(self.cards), encoding="utf-8")
        self.assertEqual(client.get("/api/learning/summary").json()["total_cards"], 120)

    def test_main_registers_all_learning_routes(self):
        from backend import main

        paths = {route.path for route in main.app.routes}
        for route in ["summary", "session", "cards", "reviews"]:
            self.assertIn(f"/api/learning/{route}", paths)

    def test_runtime_backup_includes_committed_review_and_progress_from_wal(self):
        from backend import main

        self.assertIn("learning.db", main._BACKUP_DATABASE_FILES)
        sync_seed_cards(self.db, self.seed)
        keeper = sqlite3.connect(self.db)
        try:
            keeper.execute("PRAGMA wal_autocheckpoint = 0")
            self.assertEqual(keeper.execute("SELECT COUNT(*) FROM learning_reviews").fetchone()[0], 0)
            record_learning_review(self.db, self.cards[0]["id"], 4)
            self.assertTrue(Path(str(self.db) + "-wal").is_file())
            with patch.object(main, "BASE_DIR", self.root):
                payload = main._build_runtime_backup()
            with zipfile.ZipFile(io.BytesIO(payload)) as bundle:
                self.assertIn("data/learning.db", bundle.namelist())
                self.assertIn("data/learning.db", json.loads(bundle.read("MANIFEST.json"))["included"])
                bundle.extract("data/learning.db", self.root / "export")
            restored = sqlite3.connect(self.root / "export" / "data" / "learning.db")
            try:
                self.assertEqual(restored.execute("SELECT card_id, rating FROM learning_reviews").fetchall(), [(self.cards[0]["id"], 4)])
                self.assertEqual(restored.execute("SELECT review_count, interval_days FROM learning_progress WHERE card_id = ?", (self.cards[0]["id"],)).fetchone(), (1, 7))
            finally:
                restored.close()
        finally:
            keeper.close()


if __name__ == "__main__":
    unittest.main()
