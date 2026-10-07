import io
import json
import sqlite3
import tempfile
import unittest
import zipfile
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from backend import main
from backend.warframe_farm_store import FarmJournalStore


class DashboardLayoutTests(unittest.TestCase):
    def test_defaults_preserve_quiet_dashboard_and_optional_media(self):
        ux = main._normalize_settings({})["ux"]
        self.assertEqual(ux["dashboard_order"], ["launch", "weather", "monitoring", "favorites", "jellyfin"])
        self.assertEqual(ux["dashboard_hidden"], ["jellyfin"])

    def test_valid_layout_roundtrips_and_duplicates_are_removed(self):
        ux = main._normalize_settings({"ux": {"dashboard_order": ["weather", "weather", "launch", "bad"], "dashboard_hidden": ["favorites", "favorites", "bad"]}})["ux"]
        self.assertEqual(ux["dashboard_order"], ["weather", "launch", "monitoring", "favorites", "jellyfin"])
        self.assertEqual(ux["dashboard_hidden"], ["favorites"])

    def test_invalid_payload_cannot_break_dashboard_or_credentials(self):
        settings = main._normalize_settings({"ux": {"dashboard_order": "bad", "dashboard_hidden": {"launch": True}}, "homelab": {"jellyfin_api_key": "private"}})
        self.assertEqual(settings["ux"]["dashboard_hidden"], ["jellyfin"])
        self.assertEqual(settings["homelab"]["jellyfin_api_key"], "private")

    def test_all_hidden_is_permitted_and_new_ids_are_not_lost(self):
        ids = ["launch", "weather", "monitoring", "favorites", "jellyfin"]
        ux = main._normalize_settings({"ux": {"dashboard_order": [], "dashboard_hidden": ids}})["ux"]
        self.assertEqual(ux["dashboard_order"], ids)
        self.assertEqual(ux["dashboard_hidden"], ids)

    def test_backup_preserves_journal_and_media_without_exporting_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / 'data'
            store = FarmJournalStore(data / 'warframe_farm_journal.db')
            session = store.create_session({'target': 'Forma'})
            store.add_drop(session['id'], {'item': 'Forma', 'quantity': 2})
            snapshot = {'version': 1, 'scope': 'hashed-credentials', 'items': []}
            (data / 'jellyfin_dashboard_cache.json').write_text(json.dumps(snapshot))
            settings = main._normalize_settings({'ux': {'dashboard_order': ['weather', 'launch']}, 'homelab': {'jellyfin_api_key': 'fixture-private-key'}})
            with patch.object(main, 'BASE_DIR', root), patch.object(main, '_load_settings', return_value=settings), patch.object(main, '_load_tldr_imap_config', return_value={}):
                regular = main._build_runtime_backup()
                migration = main._build_runtime_backup(include_secrets=True)
            with zipfile.ZipFile(io.BytesIO(regular)) as bundle:
                self.assertIn('data/warframe_farm_journal.db', bundle.namelist())
                self.assertEqual(json.loads(bundle.read('data/jellyfin_dashboard_cache.json')), snapshot)
                self.assertNotIn('data/settings.json', bundle.namelist())
                bundle.extract('data/warframe_farm_journal.db', root / 'restored')
            with closing(sqlite3.connect(root / 'restored/data/warframe_farm_journal.db')) as database:
                self.assertEqual(database.execute('SELECT item, quantity FROM warframe_farm_drops').fetchall(), [('Forma', 2)])
            with zipfile.ZipFile(io.BytesIO(migration)) as bundle:
                restored = json.loads(bundle.read('data/settings.json'))
                self.assertEqual(restored['ux']['dashboard_order'][0], 'weather')
                self.assertEqual(restored['homelab']['jellyfin_api_key'], 'fixture-private-key')


if __name__ == "__main__":
    unittest.main()
