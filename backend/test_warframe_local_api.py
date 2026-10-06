import io
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import patch

from backend import main
from backend.warframe_cache_store import WarframeCacheStore, WarframeRefreshQueue


class LocalWarframeApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.data = self.root / "data"
        self.store = WarframeCacheStore(self.data / "warframe_cache.db")
        self.queue = WarframeRefreshQueue(self.store)
        self.patches = [
            patch.object(main, "BASE_DIR", self.root),
            patch.object(main, "WARFRAME_MARKET_HISTORY_DB_FILE", self.data / "warframe_market_history.db"),
            patch.object(main, "WARFRAME_MARKET_HISTORY_FILE", self.data / "warframe_market_history.json"),
            patch.object(main, "WARFRAME_WORLDSTATE_DB_FILE", self.data / "warframe_worldstate.db"),
            patch.object(main, "_warframe_store", self.store),
            patch.object(main, "_warframe_refresh_queue", self.queue),
            patch.object(main, "_warframe_assets", SimpleNamespace(local_url=lambda value: value, warm=lambda values: None)),
            patch.dict(main._API_CACHE, {}, clear=True),
            patch.dict(main._LAST_GOOD, {}, clear=True),
        ]
        for context in self.patches:
            context.start()
        self.key = "warframe:pc:saryn prime set"
        self.payload = {"market": {"canonical_name": "Saryn Prime Set", "slug": "saryn_prime_set",
            "last_avg_price": 80, "thumb": "items/saryn.png",
            "history": [{"datetime": "2026-10-01T00:00:00Z", "avg_price": 80}]},
            "worldstate": {}, "top_sells": [], "platform": "pc", "errors": []}

    def tearDown(self):
        self.queue._pool.shutdown(wait=True)
        self.queue.close()
        for context in reversed(self.patches):
            context.stop()
        self.temp.cleanup()

    def test_fresh_saved_page_after_restart_needs_no_network_or_browser_cache(self):
        self.store.put(self.key, self.payload)
        with patch.object(main, "_http_get_json", side_effect=AssertionError("network on saved response")):
            response = main.warframe_overview("saryn prime set")
        self.assertEqual(response["market"]["last_avg_price"], 80)
        self.assertEqual(len(response["market"]["history"]), 1)
        self.assertTrue(response["cached"])
        self.assertFalse(response["loading"])
        self.assertFalse(response["refreshing"])
        self.assertTrue(response["data_as_of"].endswith("+00:00"))

    def test_old_saved_response_returns_while_provider_is_blocked_and_force_keeps_data(self):
        self.store.put(self.key, self.payload, timestamp=time.time() - 86400)
        entered, release = Event(), Event()
        def slow_provider(*args, **kwargs):
            entered.set()
            release.wait(3)
            return self.payload
        try:
            with patch.object(main, "_build_warframe_overview", side_effect=slow_provider):
                response = main.warframe_overview("saryn prime set", force=True)
                self.assertTrue(entered.wait(1))
                self.assertFalse(release.is_set())
                self.assertEqual(response["market"]["last_avg_price"], 80)
                self.assertTrue(response["refreshing"])
                self.assertTrue(response["stale"])
                self.assertFalse(response["loading"])
                release.set()
                self.queue._pool.shutdown(wait=True)
        finally:
            release.set()

    def test_failed_background_refresh_keeps_saved_snapshot_and_reports_error(self):
        self.store.put(self.key, self.payload, timestamp=time.time() - 86400)
        with patch.object(main, "_build_warframe_overview", side_effect=RuntimeError("offline")):
            main.warframe_overview("saryn prime set")
            self.queue._pool.shutdown(wait=True)
        response = main.warframe_overview("saryn prime set")
        self.assertEqual(response["market"]["last_avg_price"], 80)
        self.assertEqual(response["refresh_error"], "offline")
        self.assertFalse(response["refreshing"])

    def test_brand_new_item_returns_explicit_loading_state_instead_of_waiting(self):
        with patch.object(main, "_build_warframe_overview", side_effect=RuntimeError("offline")):
            response = main.warframe_overview("unknown item")
            self.queue._pool.shutdown(wait=True)
        self.assertTrue(response["loading"])
        self.assertFalse(response["cached"])
        self.assertNotIn("last_avg_price", response["market"])

    def test_history_is_read_from_sqlite_without_legacy_json_index(self):
        snapshot = main._record_warframe_market_snapshot({"slug": "saryn_prime_set", "last_avg_price": 79})
        self.assertEqual(snapshot["last_price"], 79)
        self.assertFalse(main.WARFRAME_MARKET_HISTORY_FILE.exists())
        with patch.object(main, "_load_warframe_market_history_store", side_effect=AssertionError("legacy JSON read")):
            reopened = main._summarize_warframe_market_history("saryn_prime_set")
        self.assertEqual(reopened["series"][0]["price"], 79)

    def test_empty_and_stale_payloads_do_not_replace_saved_prices(self):
        key = "warframe:market_snapshot:pc:saryn_prime_set"
        good = {"payload": self.payload["market"], "errors": []}
        main._cache_set(key, good)
        main._cache_set(key, {"payload": {"last_avg_price": None}, "errors": ["offline"]})
        self.assertEqual(self.store.read(key)["value"], good)
        main._cache_set(key, {"payload": {"last_avg_price": 9, "stale": True}, "errors": []})
        self.assertEqual(self.store.read(key)["value"], good)

    def test_catalog_network_failure_retains_saved_names_and_artwork(self):
        key = "warframe:market:catalog:v2"
        items = [{"name": "Saryn Prime Set", "slug": "saryn_prime_set", "icon": "items/saryn.png"}]
        self.store.put(key, {"items": items}, timestamp=1)
        with patch.object(main, "_http_get_json", return_value=(None, "offline")):
            result, errors = main._fetch_warframe_market_catalog()
        self.assertEqual(result, items)
        self.assertIn("saved_snapshot", errors[0])
        self.assertEqual(self.store.read(key)["ts"], 1)

    def test_export_includes_provider_database_and_completed_artwork_only(self):
        self.store.put(self.key, self.payload)
        asset_dir = self.data / "warframe_assets"
        asset_dir.mkdir()
        (asset_dir / "test.png").write_bytes(b"complete-image-fixture")
        (asset_dir / ".pending-test.tmp").write_bytes(b"partial")
        backup = main._build_runtime_backup()
        with zipfile.ZipFile(io.BytesIO(backup)) as bundle:
            self.assertIn("data/warframe_cache.db", bundle.namelist())
            self.assertIn("data/warframe_assets/test.png", bundle.namelist())
            self.assertNotIn("data/warframe_assets/.pending-test.tmp", bundle.namelist())


if __name__ == "__main__":
    unittest.main()
