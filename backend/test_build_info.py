import importlib
import json
import tempfile
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


class BuildInfoTests(unittest.TestCase):
    def setUp(self):
        self.module = importlib.import_module('backend.build_info')
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'backend').mkdir()
        (self.root / 'frontend/assets').mkdir(parents=True)
        (self.root / 'backend/main.py').write_text('print("app")')
        (self.root / 'frontend/index.html').write_text('<main>App</main>')

    def test_code_identity_excludes_user_data_secrets_and_tests(self):
        first = self.module.read_build_info(self.root)
        self.assertEqual(first['source'], 'source-files')
        self.assertIsNone(first['revision'])
        self.assertIsNone(first['built_at'])
        self.assertEqual(len(first['source_id']), 64)
        (self.root / 'data').mkdir()
        (self.root / 'data/settings.json').write_text('{"key":"private-fixture"}')
        (self.root / '.env').write_text('SECRET=private-fixture')
        (self.root / 'backend/test_local.py').write_text('print("test")')
        self.assertEqual(self.module.read_build_info(self.root)['source_id'], first['source_id'])
        (self.root / 'frontend/index.html').write_text('<main>Updated</main>')
        self.assertNotEqual(self.module.read_build_info(self.root)['source_id'], first['source_id'])

    def test_manifest_matches_delivered_files_and_does_not_invent_revision(self):
        manifest = self.module.create_build_info(self.root, revision='a' * 40)
        (self.root / 'build-info.json').write_text(json.dumps(manifest))
        result = self.module.read_build_info(self.root)
        self.assertEqual(result['revision'], 'a' * 40)
        self.assertEqual(result['source'], 'image-build')
        self.assertTrue(result['built_at'].endswith('+00:00'))
        self.assertEqual(self.module.create_build_info(self.root, revision='main')['revision'], None)
        (self.root / 'backend/main.py').write_text('print("changed")')
        changed = self.module.read_build_info(self.root)
        self.assertIsNone(changed['revision'])
        self.assertIsNone(changed['built_at'])
        self.assertEqual(changed['source'], 'source-files')

    def test_corrupt_manifest_falls_back_without_breaking_application(self):
        (self.root / 'build-info.json').write_text('{broken')
        result = self.module.read_build_info(self.root)
        self.assertEqual(result['source'], 'source-files')
        self.assertNotIn('private-fixture', json.dumps(result))

    def test_public_endpoint_has_no_store_and_no_runtime_config(self):
        from fastapi.testclient import TestClient
        from backend import main
        client = TestClient(main.app)
        try:
            with patch.object(main, 'get_build_info', return_value=self.module.read_build_info(self.root)):
                response = client.get('/api/build-info')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers['cache-control'], 'no-store')
            self.assertEqual(response.json()['version'], self.module.APP_VERSION)
            self.assertNotIn('settings', response.json())
        finally:
            client.close()

    def test_docker_build_stamp_command_generates_valid_metadata(self):
        output = self.root / 'generated.json'
        result = subprocess.run([sys.executable, '-m', 'backend.build_info', '--write', str(output), '--revision', 'd' * 40], cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(output.read_text())
        self.assertEqual(data['revision'], 'd' * 40)
        self.assertEqual(len(data['source_id']), 64)
        self.assertEqual(data['source'], 'image-build')
