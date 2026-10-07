import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
from backend import learning_store as store
from backend.learning_api import create_learning_router


class PersonalLearningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / 'learning.db'
        self.seed = Path(self.temp.name) / 'seed.json'
        self.seed_card = dict(id='seed-a', deck='English', category='Measurement & Data',
                              skill='Recognition', prompt='Seed prompt', answer='Seed answer',
                              example='Example', explanation='Explanation', source='seed', tags=[])
        self.seed.write_text(json.dumps([self.seed_card]), encoding='utf-8')
        app = FastAPI()
        app.include_router(create_learning_router(self.db, self.seed))
        self.client = TestClient(app)
        self.addCleanup(self.client.close)
        self.personal = dict(deck='My deck', category='Custom category', prompt='My prompt', answer='My answer')

    def snapshot(self):
        with closing(sqlite3.connect(self.db)) as conn:
            return [conn.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall()
                    for table in ('learning_progress', 'learning_reviews')]

    def test_crud_preserves_progress_and_history_and_soft_deletes(self):
        response = self.client.post('/api/learning/cards', json=self.personal)
        self.assertEqual(201, response.status_code, response.text)
        card = response.json()
        self.assertEqual('personal', card['ownership'])
        self.client.post('/api/learning/reviews', json=dict(card_id=card['id'], rating=3))
        before = self.snapshot()
        edited = self.client.patch('/api/learning/cards/' + card['id'], json={'prompt': 'Edited'})
        self.assertEqual(200, edited.status_code, edited.text)
        self.assertEqual('Edited', edited.json()['prompt'])
        self.assertEqual(before, self.snapshot())
        self.assertEqual(200, self.client.delete('/api/learning/cards/' + card['id']).status_code)
        self.assertEqual(before, self.snapshot())
        self.assertEqual([], self.client.get('/api/learning/cards', params={'deck': 'My deck'}).json())
        self.assertEqual(404, self.client.patch('/api/learning/cards/' + card['id'], json={'answer': 'x'}).status_code)
        self.assertEqual(404, self.client.post('/api/learning/reviews', json=dict(card_id=card['id'], rating=3)).status_code)

    def test_seed_is_read_only_and_sync_preserves_personal_and_review_history(self):
        card = self.client.post('/api/learning/cards', json=self.personal).json()
        self.client.post('/api/learning/reviews', json=dict(card_id='seed-a', rating=4))
        before = self.snapshot()
        for method in ('patch', 'delete'):
            response = getattr(self.client, method)('/api/learning/cards/seed-a', **({'json': {'prompt': 'Bad'}} if method == 'patch' else {}))
            self.assertEqual(403, response.status_code)
        self.seed_card['prompt'] = 'Revised seed'
        self.seed.write_text(json.dumps([self.seed_card]), encoding='utf-8')
        store.sync_seed_cards(self.db, self.seed)
        self.assertEqual(before, self.snapshot())
        self.assertEqual(card['id'], store.list_learning_cards(self.db, deck='My deck')[0]['id'])
        self.assertEqual('Revised seed', store.list_learning_cards(self.db, deck='English')[0]['prompt'])

    def test_custom_filters_and_summary_keep_old_categories(self):
        self.assertEqual(201, self.client.post('/api/learning/cards', json=self.personal).status_code)
        for endpoint in ('cards', 'session'):
            rows = self.client.get('/api/learning/' + endpoint, params={'deck': 'My deck', 'category': 'Custom category'}).json()
            self.assertEqual(['My prompt'], [row['prompt'] for row in rows])
            self.assertEqual([], self.client.get('/api/learning/' + endpoint, params={'deck': 'English', 'category': 'Custom category'}).json())
        summary = self.client.get('/api/learning/summary').json()
        self.assertEqual({'My deck', 'English'}, {row['deck'] for row in summary['decks']})
        self.assertEqual({'Custom category', 'Measurement & Data'}, {row['category'] for row in summary['categories']})

    def test_json_csv_preview_is_read_only_and_reimports_skip_duplicates(self):
        for fmt, content in [('json', json.dumps([self.personal, self.personal])),
                             ('csv', 'deck,category,prompt,answer,tags\nMy deck,Custom category,"My prompt","My answer",\n')]:
            payload = dict(format=fmt, content=content)
            preview = self.client.post('/api/learning/import/preview', json=payload)
            self.assertEqual(200, preview.status_code, preview.text)
            self.assertEqual(1 if fmt == 'json' else 2, self.client.get('/api/learning/summary').json()['total_cards'])
            self.assertIn('cards', preview.json())
            result = self.client.post('/api/learning/import', json=payload)
            self.assertEqual(200, result.status_code, result.text)
            self.assertEqual(1 if fmt == 'json' else 0, result.json()['imported'])
        again = self.client.post('/api/learning/import', json=dict(format='json', content=json.dumps([self.personal])))
        self.assertEqual({'imported': 0, 'skipped': 1}, again.json())
        self.assertEqual(2, self.client.get('/api/learning/summary').json()['total_cards'])

    def test_invalid_batches_and_collisions_fail_atomically(self):
        self.client.get('/api/learning/summary')
        payloads = [dict(format='json', content=json.dumps([self.personal, {'prompt': 'missing answer'}])),
                    dict(format='json', content=json.dumps([self.personal, self.seed_card])),
                    dict(format='json', content=json.dumps([self.personal] * 501)),
                    dict(format='json', content=' ' * (1024 * 1024 + 1)),
                    dict(format='csv', content='prompt,answer\nvalid,answer\ninvalid\n'),
                    dict(format='csv', content='prompt,answer,prompt\na,b,c\n'),
                    dict(format='json', content=json.dumps([{**self.personal, 'ownership': 'seed'}]))]
        for payload in payloads:
            for endpoint in ('import/preview', 'import'):
                with self.subTest(payload=payload['format'], endpoint=endpoint):
                    self.assertIn(self.client.post('/api/learning/' + endpoint, json=payload).status_code, (409, 422))
                    self.assertEqual(1, self.client.get('/api/learning/summary').json()['total_cards'])

    def test_store_rejects_bad_card_and_seed_collision_without_partial_writes(self):
        store.sync_seed_cards(self.db, self.seed)
        self.assertTrue(callable(getattr(store, 'create_personal_card', None)), 'Personal store CRUD is missing')
        with self.assertRaises(ValueError):
            store.create_personal_card(self.db, {**self.personal, 'answer': ' '})
        card = store.create_personal_card(self.db, self.personal)
        seed = {**self.seed_card, 'id': card['id']}
        self.seed.write_text(json.dumps([self.seed_card, seed]), encoding='utf-8')
        with self.assertRaises(ValueError):
            store.sync_seed_cards(self.db, self.seed)
        self.assertEqual('My prompt', store.list_learning_cards(self.db, deck='My deck')[0]['prompt'])

    def test_legacy_database_migration_retains_content_progress_and_history(self):
        store.sync_seed_cards(self.db, self.seed)
        store.record_learning_review(self.db, 'seed-a', 3)
        before = self.snapshot()
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute('ALTER TABLE learning_cards DROP COLUMN ownership')
            conn.execute('ALTER TABLE learning_cards DROP COLUMN deleted_at')
        store.init_learning_db(self.db)
        store.init_learning_db(self.db)
        self.assertEqual(before, self.snapshot())
        migrated = store.list_learning_cards(self.db)[0]
        self.assertEqual('seed', migrated['ownership'])
        self.assertEqual('Seed prompt', migrated['prompt'])

    def test_conflicting_import_id_rolls_back_preceding_insert(self):
        self.client.get('/api/learning/summary')
        for rows in ([{**self.personal, 'id': 'same'}, {**self.personal, 'id': 'same', 'answer': 'Different'}],
                     [{**self.personal, 'id': 'bad/id'}]):
            payload = dict(format='json', content=json.dumps(rows))
            for endpoint in ('import/preview', 'import'):
                response = self.client.post('/api/learning/' + endpoint, json=payload)
                self.assertIn(response.status_code, (409, 422), response.text)
                self.assertEqual(1, self.client.get('/api/learning/summary').json()['total_cards'])

    def test_optional_fields_tags_unicode_and_csv_quoted_multiline(self):
        content = 'prompt,answer,tags\r\n"Line 1\nLine 2","Answer, with comma",one;two;one\r\n'
        result = self.client.post('/api/learning/import', json=dict(format='csv', content=content))
        self.assertEqual(200, result.status_code, result.text)
        card = self.client.get('/api/learning/cards', params={'deck': 'Personal'}).json()[0]
        self.assertEqual('Line 1\nLine 2', card['prompt'])
        self.assertEqual(['one', 'two'], card['tags'])
        for patch in ({'prompt': 'x' * 4001}, {'tags': ['tag'] * 31}, {'review_count': 0}, {'ownership': 'seed'}, {'id': 'seed-a'}, {'answer': None}):
            self.assertEqual(422, self.client.patch('/api/learning/cards/' + card['id'], json=patch).status_code)
        huge = json.dumps([{**self.personal, 'prompt': '\u20ac' * 4000}] * 100, ensure_ascii=False)
        self.assertLess(len(huge), 1024 * 1024)
        self.assertEqual(422, self.client.post('/api/learning/import', json=dict(format='json', content=huge)).status_code)

    def test_seed_content_without_id_is_skipped_and_existing_personal_id_never_overwritten(self):
        self.client.get('/api/learning/summary')
        content = {key: value for key, value in self.seed_card.items() if key != 'id'}
        result = self.client.post('/api/learning/import', json=dict(format='json', content=json.dumps([content])))
        self.assertEqual({'imported': 0, 'skipped': 1}, result.json())
        card = self.client.post('/api/learning/cards', json=self.personal).json()
        result = self.client.post('/api/learning/import', json=dict(format='json', content=json.dumps([{**self.personal, 'id': card['id'], 'answer': 'Changed'}])))
        self.assertEqual(409, result.status_code)
        self.assertEqual('My answer', self.client.get('/api/learning/cards', params={'deck': 'My deck'}).json()[0]['answer'])


if __name__ == '__main__':
    unittest.main()
