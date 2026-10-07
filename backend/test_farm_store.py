"""Journal behavior against isolated, real SQLite files."""
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path


class FarmStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "mounted" / "farm.db"
        self.now = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
        try:
            from backend.warframe_farm_store import FarmJournalStore
        except ImportError as exc:
            self.fail(f"Farm journal store missing: {exc}")
        self.store_class = FarmJournalStore
        self.store = FarmJournalStore(self.db, clock=lambda: self.now)

    def test_lazy_initialization_and_reopen_preserve_sessions_and_drops(self):
        self.assertFalse(self.db.parent.exists())
        session = self.store.create_session({"route": "  Void survival  ", "target": "Argon"})
        self.store.add_drop(session["id"], {"item": "Argon crystal", "quantity": 2})
        reopened = self.store_class(self.db, clock=lambda: self.now).list_sessions()
        self.assertEqual(reopened["active_session"]["route"], "Void survival")
        self.assertEqual(reopened["sessions"][0]["drops"][0]["quantity"], 2)
        self.assertEqual(session["started_at"], "2026-10-07T12:00:00Z")

    def test_estimates_never_become_sales_and_unpriced_drops_stay_unknown(self):
        session = self.store.create_session({"started_at": "2026-10-07T11:00:00Z"})
        self.store.add_drop(session["id"], {"item": "Part", "quantity": 3, "estimated_unit_platinum": 0.1})
        self.store.add_drop(session["id"], {"item": "Resource", "quantity": 4})
        summary = self.store.list_sessions()["summary"]
        self.assertEqual(summary["estimated_drop_platinum"], 0.3)
        self.assertEqual(summary["unvalued_quantity"], 4)
        self.assertFalse(summary["valuation_complete"])
        self.assertEqual(summary["confirmed_sale_platinum"], 0)
        self.assertEqual(summary["observed_sale_platinum_per_hour"], 0)
        self.store.update_session(session["id"], {"confirmed_sale_platinum": 12.34})
        summary = self.store.list_sessions()["summary"]
        self.assertEqual(summary["confirmed_sale_platinum"], 12.34)
        self.assertEqual(summary["estimated_drop_platinum"], 0.3)
        self.assertEqual(summary["observed_sale_platinum_per_hour"], 12.34)

    def test_no_estimate_and_zero_estimate_are_distinct(self):
        session = self.store.create_session({})
        self.assertIsNone(session["estimated_drop_platinum"])
        self.assertIsNone(session["observed_sale_platinum_per_hour"])
        self.store.add_drop(session["id"], {"item": "Resource", "quantity": 1, "estimated_unit_platinum": 0})
        result = self.store.list_sessions()["sessions"][0]
        self.assertEqual(result["estimated_drop_platinum"], 0)
        self.assertTrue(result["valuation_complete"])

    def test_active_timer_advances_but_finished_timer_is_frozen(self):
        session = self.store.create_session({})
        self.now += timedelta(minutes=30)
        self.assertEqual(self.store.list_sessions()["summary"]["elapsed_seconds"], 1800)
        result = self.store.update_session(session["id"], {"finish": True, "confirmed_sale_platinum": 10})
        self.assertEqual(result["status"], "finished")
        self.assertEqual(result["observed_sale_platinum_per_hour"], 20)
        self.now += timedelta(hours=1)
        again = self.store.update_session(session["id"], {"finish": True})
        self.assertEqual(again["ended_at"], "2026-10-07T12:30:00Z")
        self.assertEqual(again["elapsed_seconds"], 1800)
        self.assertIsNone(self.store.list_sessions()["active_session"])
        self.store.create_session({})

    def test_recorded_session_edits_and_late_drops_do_not_reopen_timer(self):
        session = self.store.create_session({"started_at": "2026-10-07T10:00:00Z"})
        self.store.update_session(session["id"], {"ended_at": "2026-10-07T11:00:00Z"})
        result = self.store.update_session(session["id"], {
            "route": "Correction", "target": None,
            "started_at": "2026-10-07T10:30:00+02:00",
            "confirmed_sale_platinum": 25,
        })
        self.assertEqual(result["started_at"], "2026-10-07T08:30:00Z")
        self.assertEqual(result["elapsed_seconds"], 9000)
        self.assertEqual(result["status"], "finished")
        self.store.add_drop(session["id"], {"item": "Late entry", "quantity": 1})
        with self.assertRaises(ValueError):
            self.store.update_session(session["id"], {"ended_at": None})
        self.assertEqual(self.store.list_sessions()["sessions"][0]["confirmed_sale_platinum"], 25)

    def test_summary_is_all_sessions_not_page_or_mean_of_rates(self):
        first = self.store.create_session({"started_at": "2026-10-07T08:00:00Z"})
        self.store.update_session(first["id"], {"ended_at": "2026-10-07T09:00:00Z", "confirmed_sale_platinum": 10})
        second = self.store.create_session({"started_at": "2026-10-07T09:00:00Z"})
        self.store.update_session(second["id"], {"finish": True, "confirmed_sale_platinum": 90})
        active = self.store.create_session({})
        result = self.store.list_sessions(limit=1, offset=1)
        self.assertEqual(len(result["sessions"]), 1)
        self.assertEqual(result["summary"]["session_count"], 3)
        self.assertEqual(result["summary"]["elapsed_seconds"], 14400)
        self.assertEqual(result["summary"]["observed_sale_platinum_per_hour"], 25)
        self.assertEqual(result["active_session"]["id"], active["id"])
        self.assertTrue(result["has_more"])

    def test_two_connections_cannot_start_two_active_sessions(self):
        def start(_):
            store = self.store_class(self.db, clock=lambda: self.now)
            try:
                store.create_session({})
                return "created"
            except ValueError:
                return "conflict"
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertCountEqual(list(pool.map(start, range(2))), ["created", "conflict"])
        self.assertEqual(self.store.list_sessions()["summary"]["session_count"], 1)

    def test_bad_drop_and_session_inputs_are_rejected_without_writes(self):
        session = self.store.create_session({})
        for payload in [
            {"item": " ", "quantity": 1}, {"item": "x" * 201, "quantity": 1},
            {"item": "x", "quantity": 0}, {"item": "x", "quantity": 1000001},
            {"item": "x", "quantity": True}, {"item": "x", "quantity": 1.5},
            {"item": "x", "quantity": "2"}, {"item": "x", "quantity": 1, "estimated_unit_platinum": -1},
            {"item": "x", "quantity": 1, "estimated_unit_platinum": float("nan")},
            {"item": "x", "quantity": 1, "estimated_unit_platinum": 0.001},
            {"item": "x", "quantity": 1, "estimated_unit_platinum": True},
            {"item": "x", "quantity": 1, "estimated_unit_platinum": 1000000.01},
            {"item": "x", "quantity": 1, "estimated_unit_platinum": "10"},
            {"item": "x", "quantity": 1, "sale": 5},
        ]:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.store.add_drop(session["id"], payload)
        for payload in [{"route": "x" * 201}, {"route": False}, {"confirmed_sale_platinum": None},
                        {"confirmed_sale_platinum": -1}, {"confirmed_sale_platinum": float("inf")},
                        {"finish": "yes"}, {}, {"unknown": "value"}]:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.store.update_session(session["id"], payload)
        self.assertEqual(self.store.list_sessions()["sessions"][0]["drops"], [])

    def test_time_bounds_and_rejected_edits_are_atomic(self):
        for at in ["bad", "2026-10-07T12:00:00", "2026-10-08T00:00:00Z", 123]:
            with self.subTest(at=at), self.assertRaises(ValueError):
                self.store.create_session({"started_at": at})
        session = self.store.create_session({"started_at": "2026-10-07T11:00:00Z"})
        for payload in [{"started_at": None}, {"ended_at": "2026-10-07T10:59:59Z"},
                        {"ended_at": "2026-10-08T00:00:00Z"},
                        {"route": "must not persist", "started_at": "2026-10-08T00:00:00Z"}]:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.store.update_session(session["id"], payload)
        self.assertIsNone(self.store.list_sessions()["sessions"][0]["route"])

    def test_additive_schema_preserves_other_tables_and_parameterized_text(self):
        self.db.parent.mkdir()
        with closing(sqlite3.connect(self.db)) as connection, connection:
            connection.execute("CREATE TABLE caller_data(value TEXT)")
            connection.execute("INSERT INTO caller_data VALUES ('keep')")
        label = "'); DROP TABLE caller_data; --"
        session = self.store.create_session({"target": label})
        self.store.add_drop(session["id"], {"item": label, "quantity": 1})
        with closing(sqlite3.connect(self.db)) as connection:
            self.assertEqual(connection.execute("SELECT value FROM caller_data").fetchall(), [("keep",)])
        self.assertEqual(self.store.list_sessions()["sessions"][0]["target"], label)

    def test_missing_ids_and_bad_paging_never_write_entries(self):
        for action in [lambda: self.store.update_session(999, {"finish": True}),
                       lambda: self.store.add_drop(999, {"item": "x", "quantity": 1})]:
            with self.assertRaises(KeyError):
                action()
        for kwargs in [{"limit": 0}, {"limit": 201}, {"offset": -1}, {"limit": True}]:
            with self.assertRaises(ValueError):
                self.store.list_sessions(**kwargs)
        self.assertEqual(self.store.list_sessions()["summary"]["session_count"], 0)


if __name__ == "__main__":
    unittest.main()
