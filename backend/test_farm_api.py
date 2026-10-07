import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient


BASE = "/api/warframe/farm-journal/sessions"


class FarmApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "mounted" / "farm.db"

    def client(self, path=None):
        try:
            from backend.warframe_farm_api import create_farm_router
        except ImportError as exc:
            self.fail(f"Farm journal router missing: {exc}")
        app = FastAPI()
        app.include_router(create_farm_router(path or self.db))
        client = TestClient(app)
        self.addCleanup(client.close)
        return client

    def test_router_is_lazy_and_get_has_explicit_empty_summary(self):
        client = self.client()
        self.assertFalse(self.db.parent.exists())
        result = client.get(BASE)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["sessions"], [])
        self.assertIsNone(result.json()["active_session"])
        self.assertEqual(result.json()["summary"]["confirmed_sale_platinum"], 0)
        self.assertIsNone(result.json()["summary"]["estimated_drop_platinum"])

    def test_create_drop_finish_correct_and_reopen_contract(self):
        client = self.client()
        created = client.post(BASE, json={"target": "Forma", "started_at": "2026-10-01T10:00:00Z"})
        self.assertEqual(created.status_code, 201)
        session_id = created.json()["id"]
        url = f"{BASE}/{session_id}"
        drop = client.post(url + "/drops", json={"item": "Forma", "quantity": 2, "estimated_unit_platinum": 8})
        self.assertEqual(drop.status_code, 201)
        self.assertEqual(drop.json()["estimated_drop_platinum"], 16)
        self.assertEqual(drop.json()["confirmed_sale_platinum"], 0)
        finished = client.patch(url, json={"ended_at": "2026-10-01T11:00:00Z", "confirmed_sale_platinum": 7})
        self.assertEqual(finished.status_code, 200)
        self.assertEqual(finished.json()["elapsed_seconds"], 3600)
        self.assertEqual(client.patch(url, json={"route": "Edited"}).json()["status"], "finished")
        reopened = self.client().get(BASE).json()
        self.assertEqual(reopened["sessions"][0]["route"], "Edited")
        self.assertEqual(reopened["summary"]["observed_sale_platinum_per_hour"], 7)

    def test_active_conflict_missing_ids_and_cannot_reopen_finished(self):
        client = self.client()
        session = client.post(BASE, json={}).json()
        self.assertEqual(client.post(BASE, json={}).status_code, 409)
        self.assertEqual(client.patch(BASE + "/999", json={"finish": True}).status_code, 404)
        self.assertEqual(client.post(BASE + "/999/drops", json={"item": "x", "quantity": 1}).status_code, 404)
        url = f"{BASE}/{session['id']}"
        self.assertEqual(client.patch(url, json={"finish": True}).status_code, 200)
        self.assertEqual(client.patch(url, json={"ended_at": None}).status_code, 422)
        self.assertEqual(client.post(BASE, json={}).status_code, 201)

    def test_invalid_payloads_and_query_bounds_are_clear_422(self):
        client = self.client()
        for payload in [{"route": 2}, {"route": " "}, {"target": "x" * 201},
                        {"started_at": "2026-10-01T10:00:00"}, {"extra": 1},
                        {"confirmed_sale_platinum": 10}]:
            with self.subTest(payload=payload):
                response = client.post(BASE, json=payload)
                self.assertEqual(response.status_code, 422, response.text)
                self.assertIn("detail", response.json())
        session = client.post(BASE, json={}).json()
        url = f"{BASE}/{session['id']}"
        for payload in [{}, {"finish": 1}, {"confirmed_sale_platinum": True},
                        {"confirmed_sale_platinum": 0.001}, {"confirmed_sale_platinum": "5"}]:
            with self.subTest(payload=payload):
                self.assertEqual(client.patch(url, json=payload).status_code, 422)
        for payload in [{"item": "x"}, {"item": "x", "quantity": False},
                        {"item": "x", "quantity": 0}, {"item": "x", "quantity": 2.2},
                        {"item": "x", "quantity": 1, "estimated_unit_platinum": -3}]:
            with self.subTest(payload=payload):
                self.assertEqual(client.post(url + "/drops", json=payload).status_code, 422)
        for params in [{"limit": 0}, {"limit": 201}, {"offset": -1}, {"limit": "bad"}]:
            with self.subTest(params=params):
                self.assertEqual(client.get(BASE, params=params).status_code, 422)
        self.assertEqual(client.get(BASE).json()["sessions"][0]["drops"], [])

    def test_unavailable_path_is_503_without_leaking_disk_details(self):
        blocker = Path(self.temp.name) / "not-a-directory"
        blocker.write_text("keep", encoding="utf-8")
        response = self.client(blocker / "journal.db").get(BASE)
        self.assertEqual(response.status_code, 503)
        self.assertNotIn(str(blocker), response.text)


if __name__ == "__main__":
    unittest.main()
