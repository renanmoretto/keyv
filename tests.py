import unittest
import tempfile
import shutil
from pathlib import Path
import keyv


class TestKeyV(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / 'test_db.db'
        self.db = keyv.connect(self.db_path, use_pickle=True)

    def tearDown(self):
        if self.db._conn:
            self.db._conn.close()
        shutil.rmtree(self.test_dir)

    def test_put_and_get(self):
        key, value = 'key1', 'value1'
        self.db.put(key, value)
        retrieved_value = self.db.get(key)
        self.assertEqual(retrieved_value, value)

    def test_put_duplicate_key(self):
        key, value = 'key1', 'value1'
        self.db.put(key, value)
        with self.assertRaises(ValueError):
            self.db.put(key, 'value2')

    def test_put_replace_if_exists_true(self):
        self.db.put('k1', 'old')
        self.db.put('k1', 'new', replace_if_exists=True)
        self.assertEqual(self.db.get('k1'), 'new')

    def test_update(self):
        key, value = 'key1', 'value1'
        new_value = 'new_value1'
        self.db.put(key, value)
        self.db.update(key, new_value)
        retrieved_value = self.db.get(key)
        self.assertEqual(retrieved_value, new_value)

    def test_delete(self):
        key, value = 'key1', 'value1'
        self.db.put(key, value)
        self.db.delete(key)
        retrieved_value = self.db.get(key)
        self.assertIsNone(retrieved_value)

    def test_search(self):
        self.db.put('key1', 'value1')
        self.db.put('key2', 'value1')
        self.db.put('key3', 'value2')
        results = self.db.search('value1')
        self.assertEqual(results, ['key1', 'key2'])

    def test_keys(self):
        self.db.put('key1', 'value1')
        self.db.put('key2', 'value2')
        keys = self.db.keys()
        self.assertEqual(keys, ['key1', 'key2'])


if __name__ == '__main__':
    unittest.main()
