import unittest
from fastapi import BackgroundTasks
from unittest.mock import patch
from backend import main
from backend.warframe_drop_routes import flatten_mission_rewards


class DropRouteTests(unittest.TestCase):
    def test_rotations_and_expected_reward_rolls_are_explicit(self):
        rows = flatten_mission_rewards({"missionRewards":{"Earth":{"Everest":{"gameMode":"Excavation","rewards":{"B":[{"itemName":"Target Mod","chance":10}],"C":[{"itemName":"Target Mod","chance":0},{"itemName":"Bad","chance":"nan"}]}}}}})
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]["rotation"],"B")
        self.assertEqual(rows[0]["expected_reward_checks"],10)
        self.assertEqual(rows[0]["planet"],"Earth")
        self.assertFalse(rows[0]["is_event"])

    def test_refresh_failure_preserves_last_good_and_retries_are_deduplicated(self):
        saved={"items":[{"item_name":"Target Mod","planet":"Earth","node":"Everest","chance":10}],"fetched_at":"2026-10-07T12:00:00Z"}
        first, second = BackgroundTasks(), BackgroundTasks()
        with patch.object(main, "_cache_get", return_value=None), patch.object(main, "_get_last_good", return_value=(saved, 200)), patch.object(main, "_http_get_json", return_value=(None, 'Invalid JSON')), patch.object(main, "_cache_set") as cache, patch.object(main, "_set_last_good") as persist:
            try:
                result = main.warframe_drop_routes(first, q='target')
                again = main.warframe_drop_routes(second, q='target')
                self.assertTrue(result['stale'])
                self.assertTrue(again['refreshing'])
                self.assertEqual(len(first.tasks), 1)
                self.assertEqual(len(second.tasks), 0)
                main._refresh_mission_drop_routes()
                persist.assert_not_called()
                self.assertEqual(cache.call_args.args[0], 'warframe:mission-drop-routes:retry')
            finally:
                if main._warframe_drop_routes_lock.locked():
                    main._warframe_drop_routes_lock.release()

    def test_invalid_data_is_not_a_successful_empty_dataset(self):
        with self.assertRaises(ValueError):
            flatten_mission_rewards({"missionRewards":[]})

    def test_endpoint_returns_saved_routes_without_blocking_network(self):
        saved={"items":[{"item_name":"Target Mod","planet":"Earth","node":"Everest","chance":10}],"fetched_at":"2026-10-07T12:00:00Z"}
        with patch.object(main,"_cache_get",return_value=saved), patch.object(main,"_http_get_json",side_effect=AssertionError("Network blocked")):
            result=main.warframe_drop_routes(BackgroundTasks(),q="target")
        self.assertEqual(result["items"][0]["chance"],10)
        self.assertFalse(result["refreshing"])


if __name__ == '__main__': unittest.main()
