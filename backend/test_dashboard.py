import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import BackgroundTasks
from backend import main


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.patches = [
            patch.object(main, "LAST_GOOD_FILE", Path(self.temp.name) / "last_good.json"),
            patch.object(main, "_load_settings", return_value=main._normalize_settings({})),
            patch.dict(main._API_CACHE, {}, clear=True),
            patch.dict(main._LAST_GOOD, {}, clear=True),
        ]
        for context in self.patches:
            context.start()
        self.key = "dashboard:weather:48.3705:10.8978"
        self.saved = {"location": "Augsburg", "air_temperature": 0, "date": "2026-10-07T12:00:00+02:00"}

    def tearDown(self):
        for context in reversed(self.patches):
            context.stop()
        self.temp.cleanup()

    def test_persisted_weather_is_immediate_after_restart_without_network(self):
        main._set_last_good(self.key, self.saved)
        main._LAST_GOOD.clear()
        main._LAST_GOOD.update(main._load_last_good_store())
        tasks = BackgroundTasks()
        with patch.object(main, "_fetch_open_meteo_weather", side_effect=AssertionError("Network called")):
            response = main.dashboard_weather(tasks)
        self.assertEqual(response["air_temperature"], 0)
        self.assertTrue(response["cached"])
        self.assertEqual(tasks.tasks, [])

    def test_stale_reading_and_force_return_before_background_refresh(self):
        main._LAST_GOOD[self.key] = {"ts": time.time() - 3600, "value": self.saved}
        tasks = BackgroundTasks()
        with patch.object(main, "_fetch_open_meteo_weather", side_effect=AssertionError("Blocking network called")):
            response = main.dashboard_weather(tasks, force=True)
        self.assertEqual(response["air_temperature"], 0)
        self.assertTrue(response["stale"])
        self.assertTrue(response["refreshing"])
        self.assertFalse(response["loading"])
        self.assertEqual(len(tasks.tasks), 1)

    def test_successful_refresh_persists_weather_with_correct_timezone(self):
        weather = {"air_temperature": 11, "date": "2026-10-07T12:00", "utc_offset_seconds": 7200}
        with patch.object(main, "_fetch_open_meteo_weather", return_value=(weather, None)):
            main._refresh_dashboard_weather(self.key, "Augsburg", 48.3705, 10.8978)
        saved, _ = main._get_last_good(self.key)
        self.assertEqual(saved["date"], "2026-10-07T12:00:00+02:00")
        self.assertEqual(main._load_last_good_store()[self.key]["value"]["air_temperature"], 11)

    def test_failed_weather_refresh_keeps_saved_data_and_cools_down(self):
        main._LAST_GOOD[self.key] = {"ts": time.time() - 3600, "value": self.saved}
        with patch.object(main, "_fetch_open_meteo_weather", return_value=({}, "timeout")):
            main._refresh_dashboard_weather(self.key, "Augsburg", 48.3705, 10.8978)
        tasks = BackgroundTasks()
        response = main.dashboard_weather(tasks)
        self.assertEqual(response["air_temperature"], 0)
        self.assertTrue(response["stale"])
        self.assertFalse(response["refreshing"])
        self.assertEqual(tasks.tasks, [])
        self.assertEqual(main._LAST_GOOD[self.key]["value"], self.saved)

    def test_invalid_cold_response_does_not_create_fake_weather(self):
        with patch.object(main, "_fetch_open_meteo_weather", return_value=({"air_temperature": "bad", "date": None}, None)):
            main._refresh_dashboard_weather(self.key, "Augsburg", 48.3705, 10.8978)
        response = main.dashboard_weather(BackgroundTasks())
        self.assertIsNone(response.get("air_temperature"))
        self.assertFalse(response["loading"])
        self.assertNotIn(self.key, main._LAST_GOOD)

    def test_kuma_snapshot_filters_groups_limits_rows_and_uses_configured_link(self):
        snapshot = {"status": "warning", "checked_at": "2026-10-07T12:00:00Z", "services": [
            {"name": "Services", "type": "group", "status": "online"},
            {"name": "Jellyfin", "status": "online", "url": "http://jellyfin"},
            {"name": "Pi-hole", "status": "offline"},
            {"name": "TrueNAS", "status": "online"},
            {"name": "Portainer", "status": "online"},
        ]}
        with patch.object(main, "collect_provider_snapshots", return_value={"uptime_kuma": snapshot}) as collect, \
             patch.dict("os.environ", {"UPTIME_KUMA_URL": "http://kuma:3001/dashboard"}, clear=True):
            response = main.dashboard_kuma()
        self.assertEqual(response["summary"], {"total": 4, "online": 3, "offline": 1})
        self.assertEqual(len(response["services"]), 3)
        self.assertEqual(response["services"][0]["name"], "Pi-hole")
        self.assertNotIn("url", response["services"][0])
        self.assertEqual(response["dashboard_url"], "http://kuma:3001/dashboard")
        self.assertEqual(collect.call_args.kwargs["providers"], ("uptime_kuma",))

    def test_weather_settings_preserve_zero_coordinates_and_reject_nonfinite_values(self):
        zero = main._normalize_settings({"homelab": {"weather_latitude": 0, "weather_longitude": 0}})
        self.assertEqual(zero["homelab"]["weather_latitude"], 0)
        invalid = main._normalize_settings({"homelab": {"weather_latitude": float("nan"), "weather_longitude": 181}})
        self.assertEqual(invalid["homelab"]["weather_latitude"], 48.3705)
        self.assertEqual(invalid["homelab"]["weather_longitude"], 10.8978)


if __name__ == "__main__":
    unittest.main()
