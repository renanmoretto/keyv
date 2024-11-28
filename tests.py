import unittest
import tempfile
import shutil
from pathlib import Path
import keyv
from keyv import Collection


class TestKeyV(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / 'test_db.db'
        self.db = keyv.connect(self.db_path)

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

    def test_values(self):
        self.db.put('key1', 'value1')
        self.db.put('key2', 'value2')
        values = self.db.values()
        self.assertEqual(set(values), {'value1', 'value2'})

    def test_key_exists(self):
        self.db.put('key1', 'value1')
        self.assertTrue(self.db.key_exists('key1'))
        self.assertFalse(self.db.key_exists('non_existent_key'))

    def test_collection_put_and_get(self):
        collection = self.db.create_collection('test_collection')
        key, value = 'col_key1', 'col_value1'
        collection.put(key, value)
        retrieved_value = collection.get(key)
        self.assertEqual(retrieved_value, value)

    def test_collection_put_duplicate_key(self):
        collection = self.db.create_collection('test_collection2')
        key, value = 'col_key1', 'col_value1'
        collection.put(key, value)
        with self.assertRaises(ValueError):
            collection.put(key, 'col_value2')

    def test_collection_put_replace_if_exists_true(self):
        collection = self.db.create_collection('test_collection3')
        collection.put('col_k1', 'old')
        collection.put('col_k1', 'new', replace_if_exists=True)
        self.assertEqual(collection.get('col_k1'), 'new')

    def test_collection_update(self):
        collection = self.db.create_collection('test_collection4')
        key, value = 'col_key1', 'col_value1'
        new_value = 'col_new_value1'
        collection.put(key, value)
        collection.update(key, new_value)
        retrieved_value = collection.get(key)
        self.assertEqual(retrieved_value, new_value)

    def test_collection_delete(self):
        collection = self.db.create_collection('test_collection5')
        key, value = 'col_key1', 'col_value1'
        collection.put(key, value)
        collection.delete(key)
        retrieved_value = collection.get(key)
        self.assertIsNone(retrieved_value)

    def test_collection_search(self):
        collection = self.db.create_collection('test_collection6')
        collection.put('col_key1', 'col_value1')
        collection.put('col_key2', 'col_value1')
        collection.put('col_key3', 'col_value2')
        results = collection.search('col_value1')
        self.assertEqual(set(results), {'col_key1', 'col_key2'})

    def test_collection_keys(self):
        collection = self.db.create_collection('test_collection7')
        collection.put('col_key1', 'col_value1')
        collection.put('col_key2', 'col_value2')
        keys = collection.keys()
        self.assertEqual(set(keys), {'col_key1', 'col_key2'})

    def test_collection_values(self):
        collection = self.db.create_collection('test_collection8')
        collection.put('col_key1', 'col_value1')
        collection.put('col_key2', 'col_value2')
        values = collection.values()
        self.assertEqual(set(values), {'col_value1', 'col_value2'})

    def test_collection_key_exists(self):
        collection = self.db.create_collection('test_collection9')
        collection.put('col_key1', 'col_value1')
        self.assertTrue(collection.key_exists('col_key1'))
        self.assertFalse(collection.key_exists('non_existent_key'))

    def test_from_method(self):
        collection_name = 'test_collection10'
        self.db.create_collection(collection_name)
        collection = self.db.from_(collection_name)
        self.assertIsInstance(collection, Collection)
        self.assertEqual(collection.name, collection_name)

    def test_collections_method(self):
        initial_collections = set(self.db.collections())
        new_collection_name = 'test_collection11'
        self.db.create_collection(new_collection_name)
        updated_collections = set(self.db.collections())
        self.assertEqual(
            updated_collections, initial_collections | {new_collection_name}
        )

    def test_from_method_with_non_existent_collection(self):
        with self.assertRaises(ValueError):
            self.db.from_('non_existent_collection', create_if_not_exists=False)

    def test_from_method_create_if_not_exists(self):
        new_collection_name = 'new_collection'
        collection = self.db.from_(new_collection_name, create_if_not_exists=True)
        self.assertIsInstance(collection, Collection)
        self.assertEqual(collection.name, new_collection_name)
        self.assertIn(new_collection_name, self.db.collections())


if __name__ == '__main__':
    unittest.main()
