import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from backend import main


class HubPolishTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.contexts = [
            patch.object(main, "LAST_GOOD_FILE", Path(self.temp.name) / "saved.json"),
            patch.dict(main._LAST_GOOD, {}, clear=True),
            patch.dict(main._API_CACHE, {}, clear=True),
            patch.dict(main._PROVIDER_BREAKERS, {}, clear=True),
        ]
        for context in self.contexts:
            context.start()

    def tearDown(self):
        for context in reversed(self.contexts):
            context.stop()
        self.temp.cleanup()

    def test_open_meteo_time_has_real_local_offset(self):
        for offset in (28800, -14400):
            main._API_CACHE.clear()
            main._LAST_GOOD.clear()
            with self.subTest(offset=offset), patch.object(main, "_http_get_json", return_value=(
                {"current": {"temperature_2m": 20, "time": "2026-10-07T12:00"},
                 "utc_offset_seconds": offset}, None,
            )):
                weather, error = main._fetch_open_meteo_weather(1, 2)
                self.assertIsNone(error)
                measured = datetime.fromisoformat(weather["date"])
                self.assertIsNotNone(measured.tzinfo)
                self.assertEqual(measured.utcoffset().total_seconds(), offset)

    def test_openf1_429_does_not_retry_and_honors_retry_after(self):
        for retry_after in ("180", format_datetime(datetime.fromtimestamp(time.time() + 180, timezone.utc))):
            with self.subTest(retry_after=retry_after):
                main._PROVIDER_BREAKERS.clear()
                response = Mock(status_code=429, headers={"Retry-After": retry_after})
                response.raise_for_status.side_effect = requests.HTTPError("429", response=response)
                with patch.object(main.requests, "get", return_value=response) as get:
                    _, error = main._http_get_json("https://api.openf1.org/v1/weather")
                    _, second_error = main._http_get_json("https://api.openf1.org/v1/weather")
                self.assertIn("429", error)
                self.assertIn("circuit_open", second_error)
                self.assertEqual(get.call_count, 1)
                self.assertGreater(main._PROVIDER_BREAKERS["openf1"]["open_until"], time.time() + 170)

    def test_weather_requests_share_cache_and_persist_across_restart(self):
        rows = [{"air_temperature": 21, "date": "2026-10-07T12:00:00Z"}]
        with patch.object(main, "_http_get_json", return_value=(rows, None)) as get:
            with ThreadPoolExecutor(max_workers=4) as pool:
                responses = list(pool.map(lambda _: main._openf1_get("weather", params={"meeting_key": 1296}), range(4)))
            self.assertEqual(get.call_count, 1)
            self.assertTrue(all(result == (rows, None) for result in responses))
            main._API_CACHE.clear()
            main._LAST_GOOD.clear()
            main._LAST_GOOD.update(main._load_last_good_store())
            self.assertEqual(main._openf1_get("weather", params={"meeting_key": 1296}), (rows, None))
            self.assertEqual(get.call_count, 1)

    def test_weather_failure_keeps_old_readings_without_repeat_fetch(self):
        rows = [{"air_temperature": 21}]
        with patch.object(main, "_http_get_json", return_value=(rows, None)):
            main._openf1_get("weather", params={"meeting_key": 1296})
        main._API_CACHE.clear()
        for entry in main._LAST_GOOD.values():
            entry["ts"] = time.time() - 1000
        with patch.object(main, "_http_get_json", return_value=(None, "429")) as get:
            first, error = main._openf1_get("weather", params={"meeting_key": 1296})
            second, _ = main._openf1_get("weather", params={"meeting_key": 1296})
        self.assertEqual(first, rows)
        self.assertEqual(second, rows)
        self.assertIn("429", error)
        self.assertEqual(get.call_count, 1)

    def test_appearance_settings_validate_and_roundtrip(self):
        settings = main._normalize_settings({"ux": {"theme": "black-cyan", "glow": False, "compact": True}})
        self.assertEqual(settings["ux"]["theme"], "black-cyan")
        self.assertFalse(settings["ux"]["glow"])
        self.assertTrue(settings["ux"]["compact"])
        self.assertEqual(main._normalize_settings({"ux": {"theme": "bad"}})["ux"]["theme"], "navy-neon")

    def test_grouping_uses_specific_metadata_not_home_network_category(self):
        for service, expected in [
            ({"name": "Home Assistant", "category": "Home Network", "group": "Infra", "service_kind": "smarthome"}, "systems"),
            ({"name": "TrueNAS", "category": "Home Network", "group": "Storage"}, "systems"),
            ({"name": "Pi-hole", "category": "Home Network", "group": "Network"}, "network"),
            ({"name": "Jellyfin", "group": "Media"}, "media"),
            ({"name": "Automation", "group": "Services"}, "services"),
        ]:
            with self.subTest(service=service):
                self.assertEqual(main._homelab_bucket(service), expected)

    def test_imported_monitor_stays_matched_after_rename_and_url_change(self):
        service = {"id": "local", "name": "My NAS", "link": "https://nas.example.com", "group": "Storage",
                   "kuma_monitor_id": "19", "status": "offline"}
        snapshot = {"uptime_kuma": {"status": "healthy", "services": [
            {"id": "19", "name": "TrueNAS", "url": "http://192.168.1.2:80", "status": "online"},
        ]}}
        with patch.object(main, "home_lab_overview", return_value={"services": [service]}), \
             patch.object(main, "collect_provider_snapshots", return_value=snapshot):
            result = main.homelab_command_center_overview()
        self.assertEqual(result["summary"]["total"], 1)
        self.assertEqual(result["systems"][0]["status"], "online")
        self.assertEqual(main.Tool(**{**service, "category": "Home Network", "provider": "Self-hosted",
                                     "local_or_cloud": "local", "auth_type": "none", "cost_hint": ""}).model_dump()["kuma_monitor_id"], "19")

    def test_same_ip_with_different_ports_does_not_merge_services(self):
        snapshot = {"uptime_kuma": {"status": "healthy", "services": [
            {"id": "20", "name": "Jellyfin", "url": "http://192.168.1.2:8096", "status": "online"},
        ]}}
        service = {"id": "local", "name": "Portainer", "link": "http://192.168.1.2:9000", "status": "offline"}
        with patch.object(main, "home_lab_overview", return_value={"services": [service]}), \
             patch.object(main, "collect_provider_snapshots", return_value=snapshot):
            result = main.homelab_command_center_overview()
        self.assertEqual(result["summary"]["total"], 2)
        self.assertEqual(result["summary"]["offline"], 1)

    def test_tool_normalization_keeps_kuma_binding(self):
        self.assertEqual(main._normalize_tool_record({"id": "nas", "kuma_monitor_id": "19"})["kuma_monitor_id"], "19")

    def test_inflight_success_or_failure_does_not_cancel_cooldown(self):
        deadline = time.time() + 180
        main._PROVIDER_BREAKERS["openf1"] = {"open_until": deadline, "fail_count": 1, "last_error": "429"}
        main._provider_breaker_success("openf1")
        self.assertEqual(main._PROVIDER_BREAKERS["openf1"]["open_until"], deadline)
        main._provider_breaker_error("openf1", "timeout")
        self.assertGreaterEqual(main._PROVIDER_BREAKERS["openf1"]["open_until"], deadline)

    def test_open_meteo_fallback_is_persisted_and_preserved_on_failure(self):
        raw = {"current": {"temperature_2m": 21, "time": "2026-10-07T12:00"}, "utc_offset_seconds": 7200}
        with patch.object(main, "_http_get_json", return_value=(raw, None)) as get:
            first, _ = main._fetch_open_meteo_weather(1, 2)
            main._API_CACHE.clear()
            main._LAST_GOOD.clear()
            main._LAST_GOOD.update(main._load_last_good_store())
            cached, _ = main._fetch_open_meteo_weather(1, 2)
            self.assertEqual(get.call_count, 1)
            self.assertEqual(cached["date"], first["date"])
        main._API_CACHE.clear()
        for entry in main._LAST_GOOD.values():
            entry["ts"] = time.time() - 1000
        with patch.object(main, "_http_get_json", return_value=(None, "timeout")) as get:
            stale, error = main._fetch_open_meteo_weather(1, 2)
            again, _ = main._fetch_open_meteo_weather(1, 2)
        self.assertEqual(get.call_count, 1)
        self.assertEqual(stale["air_temperature"], 21)
        self.assertEqual(again["date"], first["date"])
        self.assertTrue(stale["stale"])
        self.assertIn("timeout", error)

    def test_overview_keeps_existing_weather_for_same_meeting_only(self):
        measured = datetime.now(timezone.utc).isoformat()
        saved = {"weekend": {"meeting": {"meeting_key": 1296}, "weather": {"air_temperature": 21, "date": measured}}}
        main._set_last_good("f1:current", saved)
        for key, expected in [(1296, 21), (1297, None)]:
            with self.subTest(meeting=key), \
                 patch.object(main, "_fetch_f1_overview", return_value=({"standings": [{"position": 1}]}, [], "test")), \
                 patch.object(main, "_fetch_openf1_weekend_context", return_value=({"meeting": {"meeting_key": key}, "weather": {}}, ["weather:timeout"])):
                result = main.f1_overview(force=True)
            self.assertEqual(result["weekend"]["weather"].get("air_temperature"), expected)
            if expected:
                self.assertTrue(result["weekend"]["weather"]["stale"])

    def test_overview_refresh_times_are_timezone_aware(self):
        with patch.object(main, "_load_tools", return_value=[]):
            homelab = main.home_lab_overview()
        self.assertIsNotNone(datetime.fromisoformat(homelab["generated_at"]).tzinfo)
        with patch.object(main, "_fetch_f1_overview", return_value=({"standings": [{"position": 1}]}, [], "test")), \
             patch.object(main, "_fetch_openf1_weekend_context", return_value=({}, [])):
            f1 = main.f1_overview(force=True)
        self.assertIsNotNone(datetime.fromisoformat(f1["generated_at"]).tzinfo)


if __name__ == "__main__":
    unittest.main()
