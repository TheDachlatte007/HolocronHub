import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event

from backend.warframe_cache_store import WarframeCacheStore, WarframeRefreshQueue
from backend.warframe_history_store import ensure_warframe_history_db, get_warframe_snapshots, upsert_warframe_snapshot


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = WarframeCacheStore(self.root / "cache.db")

    def test_snapshot_survives_new_process_store_and_older_seed_cannot_replace_it(self):
        self.store.put("item", {"price": 80}, timestamp=100)
        reopened = WarframeCacheStore(self.store.path)
        reopened.put("item", {"price": 2}, timestamp=50)
        self.assertEqual(reopened.read("item"), {"ts": 100, "value": {"price": 80}})

    def test_more_than_240_history_samples_survive_and_recent_window_is_correct(self):
        path = self.root / "history.db"
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for index in range(245):
            upsert_warframe_snapshot(path, platform="pc", slug="saryn_prime_set", item_name="Saryn Prime Set",
                snapshot={"captured_at": (start + timedelta(minutes=index)).isoformat(), "price": index})
        ensure_warframe_history_db(path)
        self.assertEqual(len(get_warframe_snapshots(path, platform="pc", slug="saryn_prime_set")), 245)
        recent = get_warframe_snapshots(path, platform="pc", slug="saryn_prime_set", limit=3)
        self.assertEqual([row["price"] for row in recent], [242, 243, 244])
        self.assertEqual(get_warframe_snapshots(path, platform="ps4", slug="saryn_prime_set"), [])

    def test_history_samples_are_append_only_for_duplicate_timestamps(self):
        path = self.root / "history.db"
        timestamp = "2026-10-06T12:00:00+00:00"
        upsert_warframe_snapshot(path, platform="pc", slug="saryn_prime_set", item_name="Saryn Prime Set",
            snapshot={"captured_at": timestamp, "price": 80})
        upsert_warframe_snapshot(path, platform="pc", slug="saryn_prime_set", item_name="Edited name",
            snapshot={"captured_at": timestamp, "price": 2})
        saved = get_warframe_snapshots(path, platform="pc", slug="saryn_prime_set")
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["price"], 80)
        self.assertEqual(saved[0]["captured_at"], timestamp)

    def test_refresh_is_nonblocking_deduplicated_and_capacity_bounded(self):
        queue = WarframeRefreshQueue(self.store, workers=1, capacity=1)
        entered, release, finished = Event(), Event(), Event()
        calls = []
        def loader():
            calls.append(1)
            entered.set()
            release.wait(3)
            self.store.put("item", {"price": 81})
            finished.set()
        try:
            self.assertTrue(queue.request("item", loader))
            self.assertTrue(entered.wait(1))
            for _ in range(10):
                self.assertTrue(queue.request("item", loader, force=True))
            self.assertFalse(queue.request("another", loader))
            self.assertIsNone(self.store.read("item"))
            release.set()
            self.assertTrue(finished.wait(2))
            queue._pool.shutdown(wait=True)
            self.assertEqual(calls, [1])
            self.assertEqual(self.store.read("item")["value"]["price"], 81)
        finally:
            release.set()
            queue.close()

    def test_failed_refresh_preserves_saved_data_and_cooldown_survives_restart(self):
        self.store.put("item", {"price": 80}, timestamp=1)
        queue = WarframeRefreshQueue(self.store)
        def failing():
            raise RuntimeError("provider unavailable")
        queue.request("item", failing)
        queue._pool.shutdown(wait=True)
        queue.close()
        reopened = WarframeCacheStore(self.store.path)
        second = WarframeRefreshQueue(reopened)
        try:
            self.assertEqual(reopened.read("item")["value"], {"price": 80})
            self.assertEqual(reopened.last_attempt("item")["error"], "provider unavailable")
            self.assertFalse(second.request("item", failing, force=True))
        finally:
            second.close()

    def test_sqlite_online_backup_includes_committed_snapshots(self):
        self.store.put("item", {"price": 80})
        backup = self.root / "backup.db"
        source = sqlite3.connect(self.store.path)
        target = sqlite3.connect(backup)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
        self.assertEqual(WarframeCacheStore(backup).read("item")["value"], {"price": 80})


if __name__ == "__main__":
    unittest.main()
