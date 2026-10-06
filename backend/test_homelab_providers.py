import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from .homelab_providers import collect_provider_snapshots, overall_provider_status, _parse_prometheus, _url
except ImportError:
    from homelab_providers import collect_provider_snapshots, overall_provider_status, _parse_prometheus, _url


class HomelabProviderTests(unittest.TestCase):
    def test_prometheus_monitor_samples_are_normalized(self):
        samples = _parse_prometheus(
            'monitor_status{monitor_name="Jellyfin",monitor_url="http://jellyfin"} 1\n'
            'monitor_response_time{monitor_name="Jellyfin"} 42.5\n'
        )
        self.assertEqual(samples[0][0], "monitor_status")
        self.assertEqual(samples[0][1]["monitor_name"], "Jellyfin")
        self.assertEqual(samples[1][2], 42.5)

    def test_provider_status_uses_worst_snapshot(self):
        self.assertEqual(overall_provider_status({"kuma": {"status": "healthy"}, "beszel": {"status": "warning"}}), "warning")
        self.assertEqual(overall_provider_status({}), "unknown")

    def test_kuma_dashboard_url_resolves_to_server_root(self):
        with patch.dict("os.environ", {"UPTIME_KUMA_URL": "http://192.168.178.32:31050/dashboard/19"}, clear=True):
            self.assertEqual(_url("UPTIME_KUMA_URL"), "http://192.168.178.32:31050")

    def test_unconfigured_providers_do_not_create_fake_data(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {}, clear=True):
            result = collect_provider_snapshots(Path(directory) / "providers.json")
        self.assertEqual(result, {})

    def test_kuma_only_collection_keeps_beszel_cache_without_calling_it(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {
            "UPTIME_KUMA_URL": "http://kuma", "BESZEL_URL": "http://beszel",
        }, clear=True):
            path = Path(directory) / "providers.json"
            preserved = {"stored_at": 1, "snapshot": {"status": "healthy"}}
            path.write_text(json.dumps({"beszel": preserved}))
            with patch(f"{collect_provider_snapshots.__module__}._uptime_kuma", return_value={"services": []}) as kuma, \
                 patch(f"{collect_provider_snapshots.__module__}._beszel", side_effect=AssertionError("Unrelated provider called")):
                first = collect_provider_snapshots(path, providers=("uptime_kuma",))
                second = collect_provider_snapshots(path, providers=("uptime_kuma",))
            self.assertEqual(set(first), {"uptime_kuma"})
            self.assertTrue(second["uptime_kuma"]["cached"])
            self.assertEqual(kuma.call_count, 1)
            self.assertEqual(json.loads(path.read_text())["beszel"], preserved)

    def test_failed_refresh_preserves_saved_snapshot_and_its_check_time(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {"UPTIME_KUMA_URL": "http://kuma"}, clear=True):
            path = Path(directory) / "providers.json"
            saved = {"status": "healthy", "checked_at": "2026-10-01T12:00:00Z", "errors": [],
                     "services": [{"name": "Jellyfin", "status": "online"}]}
            path.write_text(json.dumps({"uptime_kuma": {"stored_at": 1, "snapshot": saved}}))
            with patch(f"{collect_provider_snapshots.__module__}._uptime_kuma", side_effect=TimeoutError("Timeout")):
                response = collect_provider_snapshots(path, providers=("uptime_kuma",))
            self.assertTrue(response["uptime_kuma"]["stale"])
            self.assertEqual(response["uptime_kuma"]["checked_at"], saved["checked_at"])
            self.assertEqual(json.loads(path.read_text())["uptime_kuma"]["snapshot"], saved)


if __name__ == "__main__":
    unittest.main()
