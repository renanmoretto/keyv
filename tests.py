import unittest
import tempfile
import shutil
from pathlib import Path
import keyv
import json
import pickle
import sqlite3
import os


class TestClass:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __eq__(self, other):
        if not isinstance(other, TestClass):
            return False
        return self.name == other.name and self.value == other.value


def test_func(x):
    return x * 2


class TestKeyV(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / 'test_db.db'
        self.db = keyv.connect(self.db_path)
        # Create a default collection for testing
        self.collection = self.db.collection('test_collection')

    def tearDown(self):
        if self.db.connection:
            self.db.connection.close()
        shutil.rmtree(self.test_dir)

    def test_collection_set_and_get(self):
        key, value = 'key1', 'value1'
        self.collection.set(key, value)
        retrieved_value = self.collection.get(key)
        self.assertEqual(retrieved_value, value)

    def test_collection_set_duplicate_key(self):
        key, value = 'key1', 'value1'
        self.collection.set(key, value)
        with self.assertRaises(ValueError):
            self.collection.set(key, 'value2')

    def test_collection_set_replace_if_exists_true(self):
        self.collection.set('k1', 'old')
        self.collection.set('k1', 'new', replace_if_exists=True)
        self.assertEqual(self.collection.get('k1'), 'new')

    def test_collection_update(self):
        key, value = 'key1', 'value1'
        new_value = 'new_value1'
        self.collection.set(key, value)
        self.collection.update(key, new_value)
        retrieved_value = self.collection.get(key)
        self.assertEqual(retrieved_value, new_value)

    def test_collection_delete(self):
        key, value = 'key1', 'value1'
        self.collection.set(key, value)
        self.collection.delete(key)
        retrieved_value = self.collection.get(key)
        self.assertIsNone(retrieved_value)

    def test_collection_search(self):
        self.collection.set('key1', 'value1')
        self.collection.set('key2', 'value1')
        self.collection.set('key3', 'value2')
        results = self.collection.search('value1')
        self.assertEqual(set(results), {'key1', 'key2'})

    def test_collection_keys(self):
        self.collection.set('key1', 'value1')
        self.collection.set('key2', 'value2')
        keys = self.collection.keys()
        self.assertEqual(set(keys), {'key1', 'key2'})

    def test_collection_values(self):
        self.collection.set('key1', 'value1')
        self.collection.set('key2', 'value2')
        values = self.collection.values()
        self.assertEqual(set(values), {'value1', 'value2'})

    def test_collection_key_exists(self):
        self.collection.set('key1', 'value1')
        self.assertTrue(self.collection.key_exists('key1'))
        self.assertFalse(self.collection.key_exists('non_existent_key'))

    def test_create_collection(self):
        collection_name = 'new_collection'
        collection = self.db.create_collection(collection_name)
        self.assertEqual(collection.name, collection_name)
        self.assertIn(collection_name, self.db.collections())

    def test_get_collection(self):
        collection_name = 'another_collection'
        self.db.create_collection(collection_name)
        collection = self.db.collection(collection_name)
        self.assertEqual(collection.name, collection_name)

    def test_collection_with_non_existent_name(self):
        with self.assertRaises(ValueError):
            self.db.collection('non_existent_collection', create_if_not_exists=False)

    def test_collection_create_if_not_exists(self):
        new_collection_name = 'auto_created_collection'
        collection = self.db.collection(new_collection_name, create_if_not_exists=True)
        self.assertEqual(collection.name, new_collection_name)
        self.assertIn(new_collection_name, self.db.collections())

    def test_collections_method(self):
        initial_collections = set(self.db.collections())
        new_collection_name = 'test_collection_list'
        self.db.create_collection(new_collection_name)
        updated_collections = set(self.db.collections())
        self.assertEqual(
            updated_collections, initial_collections | {new_collection_name}
        )

    def test_json_serializer_collection(self):
        json_collection = self.db.collection('json_collection', serializer='json')

        test_dict = {'name': 'John', 'age': 30, 'roles': ['admin', 'user']}
        json_collection.set('user', test_dict)
        retrieved_dict = json_collection.get('user')
        self.assertEqual(retrieved_dict, test_dict)

        test_list = [1, 2, 3, 4, 5]
        json_collection.set('numbers', test_list)
        retrieved_list = json_collection.get('numbers')
        self.assertEqual(retrieved_list, test_list)

        nested_data = {
            'users': [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}],
            'settings': {'active': True, 'options': ['opt1', 'opt2']},
        }
        json_collection.set('complex', nested_data)
        retrieved_nested = json_collection.get('complex')
        self.assertEqual(retrieved_nested, nested_data)

    def test_pickle_serializer_collection(self):
        pickle_collection = self.db.collection('pickle_collection', serializer='pickle')

        test_obj = TestClass('test', 123)
        pickle_collection.set('obj', test_obj)
        retrieved_obj = pickle_collection.get('obj')
        self.assertEqual(retrieved_obj.name, test_obj.name)
        self.assertEqual(retrieved_obj.value, test_obj.value)
        self.assertEqual(retrieved_obj, test_obj)

        pickle_collection.set('func', test_func)
        retrieved_func = pickle_collection.get('func')
        self.assertEqual(retrieved_func(5), 10)

    def test_override_collection_serializer(self):
        mixed_collection = self.db.collection('mixed_collection', serializer='json')

        json_data = {'a': 1, 'b': 2}
        mixed_collection.set('json_data', json_data)

        custom_obj = TestClass('test_object', 123)
        mixed_collection.set('pickle_data', custom_obj, serializer='pickle')

        retrieved_json = mixed_collection.get('json_data')
        retrieved_pickle = mixed_collection.get('pickle_data', serializer='pickle')

        self.assertEqual(retrieved_json, json_data)
        self.assertEqual(retrieved_pickle, custom_obj)

        with self.assertRaises(Exception):
            mixed_collection.get('pickle_data')

    def test_serializer_search(self):
        json_collection = self.db.collection('search_collection', serializer='json')

        common_value = {'status': 'active', 'type': 'user'}
        json_collection.set('item1', common_value)
        json_collection.set('item2', common_value)
        json_collection.set('item3', {'status': 'inactive'})

        search_results = json_collection.search(common_value)
        self.assertEqual(set(search_results), {'item1', 'item2'})

    def test_iteritems(self):
        items_collection = self.db.collection('items_collection')
        test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}

        for key, value in test_data.items():
            items_collection.set(key, value)

        retrieved_items = dict(items_collection.iteritems())
        self.assertEqual(retrieved_items, test_data)

        json_collection = self.db.collection('json_items', serializer='json')
        complex_data = {
            'obj1': {'name': 'Object 1', 'id': 1},
            'obj2': {'name': 'Object 2', 'id': 2},
        }

        for key, value in complex_data.items():
            json_collection.set(key, value)

        retrieved_json_items = dict(json_collection.iteritems())
        self.assertEqual(retrieved_json_items, complex_data)

    def test_iterkeys(self):
        keys_collection = self.db.collection('keys_collection')
        test_keys = ['key1', 'key2', 'key3', 'key4']

        for key in test_keys:
            keys_collection.set(key, f'value-{key}')

        retrieved_keys = list(keys_collection.iterkeys())
        self.assertEqual(set(retrieved_keys), set(test_keys))

        numeric_collection = self.db.collection('numeric_keys')
        numeric_keys = [1, 2, 3, 4.5]

        for key in numeric_keys:
            numeric_collection.set(key, f'value-{key}')

        retrieved_numeric_keys = list(numeric_collection.iterkeys())
        self.assertEqual(set(retrieved_numeric_keys), set(numeric_keys))

        empty_collection = self.db.collection('empty_collection')
        self.assertEqual(list(empty_collection.iterkeys()), [])

    def test_itervalues(self):
        values_collection = self.db.collection('values_collection')
        test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}

        for key, value in test_data.items():
            values_collection.set(key, value)

        retrieved_values = list(values_collection.itervalues())
        self.assertEqual(set(retrieved_values), set(test_data.values()))

        json_values_collection = self.db.collection('json_values', serializer='json')
        complex_values = {
            'obj1': {'name': 'Object 1', 'id': 1},
            'obj2': {'name': 'Object 2', 'id': 2},
            'obj3': [1, 2, 3, 4],
        }

        for key, value in complex_values.items():
            json_values_collection.set(key, value)

        retrieved_complex_values = list(json_values_collection.itervalues())
        self.assertEqual(
            set(str(v) for v in retrieved_complex_values),
            set(str(v) for v in complex_values.values()),
        )

        pickle_values_collection = self.db.collection(
            'pickle_values', serializer='pickle'
        )
        test_obj1 = TestClass('test1', 100)
        test_obj2 = TestClass('test2', 200)

        pickle_values_collection.set('obj1', test_obj1)
        pickle_values_collection.set('obj2', test_obj2)

        retrieved_pickle_values = list(pickle_values_collection.itervalues())
        self.assertEqual(len(retrieved_pickle_values), 2)
        self.assertTrue(
            all(isinstance(obj, TestClass) for obj in retrieved_pickle_values)
        )
        self.assertTrue(
            any(
                obj.name == 'test1' and obj.value == 100
                for obj in retrieved_pickle_values
            )
        )
        self.assertTrue(
            any(
                obj.name == 'test2' and obj.value == 200
                for obj in retrieved_pickle_values
            )
        )

    def test_get_with_default(self):
        """Test the get method with a custom default value for non-existent keys."""
        self.assertEqual(
            self.collection.get('non_existent_key', default='custom_default'),
            'custom_default',
        )

        self.assertEqual(self.collection.get('non_existent_key', default=42), 42)

        default_dict = {'default': True, 'data': [1, 2, 3]}
        self.assertEqual(
            self.collection.get('non_existent_key', default=default_dict), default_dict
        )

        self.collection.set('existing_key', 'existing_value')
        self.assertEqual(
            self.collection.get('existing_key', default='default_value'),
            'existing_value',
        )

    def test_get_raise_if_missing(self):
        """Test the get method with raise_if_missing parameter."""
        with self.assertRaises(ValueError):
            self.collection.get('non_existent_key', raise_if_missing=True)

        self.collection.set('existing_key', 'existing_value')
        self.assertEqual(
            self.collection.get('existing_key', raise_if_missing=True), 'existing_value'
        )

        with self.assertRaises(ValueError):
            self.collection.get(
                'non_existent_key', default='ignored', raise_if_missing=True
            )

    def test_get_with_serializer_and_default(self):
        """Test get with serializer and default parameters together."""
        json_collection = self.db.collection('json_defaults', serializer='json')

        default_obj = {'name': 'Default', 'id': 0}
        retrieved = json_collection.get('non_existent', default=default_obj)
        self.assertEqual(retrieved, default_obj)

        test_obj = {'name': 'Test', 'id': 1}
        json_collection.set('test_key', test_obj)
        retrieved = json_collection.get('test_key')
        self.assertEqual(retrieved, test_obj)

        pickle_collection = self.db.collection('pickle_defaults', serializer='pickle')
        custom_obj = TestClass('default_obj', 100)

        result = pickle_collection.get('non_existent', default=custom_obj)
        self.assertEqual(result, custom_obj)

        test_dict = {'a': 1, 'b': 2}
        pickle_collection.set('json_in_pickle', test_dict, serializer='json')
        retrieved = pickle_collection.get('json_in_pickle', serializer='json')
        self.assertEqual(retrieved, test_dict)

    def test_with_statement_basic(self):
        """Test using a collection with a 'with' statement."""
        with self.db.collection('with_test') as collection:
            collection.set('key1', 'value1')
            self.assertEqual(collection.get('key1'), 'value1')

    def test_with_statement_auto_close(self):
        """Test that the database connection is closed after exiting the with block."""
        db_path = Path(self.test_dir) / 'with_db.db'
        db = keyv.connect(db_path)

        with db.collection('with_test') as collection:
            collection.set('key1', 'value1')
            self.assertIsNotNone(db.connection)
            db.connection.execute('SELECT 1')

        with self.assertRaises(sqlite3.ProgrammingError):
            db._conn.execute('SELECT 1')

    def test_with_statement_exception_handling(self):
        """Test that the connection is closed even if an exception is raised in the with block."""
        db_path = Path(self.test_dir) / 'exception_db.db'
        db = keyv.connect(db_path)

        try:
            with db.collection('except_test') as collection:
                collection.set('key1', 'value1')
                db.connection.execute('SELECT 1')
                raise ValueError('Test exception')
        except ValueError:
            pass

        with self.assertRaises(sqlite3.ProgrammingError):
            db._conn.execute('SELECT 1')

    def test_with_statement_nested(self):
        """Test using nested with statements with different collections."""
        with self.db.collection('outer_collection') as outer:
            outer.set('outer_key', 'outer_value')

            inner_db_path = Path(self.test_dir) / 'inner_db.db'
            inner_db = keyv.connect(inner_db_path)

            with inner_db.collection('inner_collection') as inner:
                inner.set('inner_key', 'inner_value')

                self.assertEqual(outer.get('outer_key'), 'outer_value')
                self.assertEqual(inner.get('inner_key'), 'inner_value')

                outer_db_conn = self.db.connection
                inner_db_conn = inner_db.connection
                outer_db_conn.execute('SELECT 1')
                inner_db_conn.execute('SELECT 1')

            with self.assertRaises(sqlite3.ProgrammingError):
                inner_db._conn.execute('SELECT 1')
            self.db._conn.execute('SELECT 1')

        with self.assertRaises(sqlite3.ProgrammingError):
            self.db._conn.execute('SELECT 1')

    def test_change_name_basic(self):
        original_name = 'rename_test'
        new_name = 'renamed_collection'

        original_collection = self.db.create_collection(original_name)
        original_collection.set('key1', 'value1')

        renamed_collection = original_collection.change_name(new_name)

        self.assertEqual(renamed_collection.name, new_name)
        self.assertEqual(original_collection.name, new_name)
        self.assertIn(new_name, self.db.collections())
        self.assertNotIn(original_name, self.db.collections())

    def test_change_name_preserves_data(self):
        original_name = 'data_collection'
        new_name = 'preserved_data'

        collection = self.db.create_collection(original_name)
        test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}

        for key, value in test_data.items():
            collection.set(key, value)

        collection.change_name(new_name)
        for key, expected_value in test_data.items():
            self.assertEqual(collection.get(key), expected_value)

        fresh_collection = self.db.collection(new_name)
        for key, expected_value in test_data.items():
            self.assertEqual(fresh_collection.get(key), expected_value)

    def test_change_name_method_chaining(self):
        collection = self.db.create_collection('chain_test')
        result = collection.change_name('chain_renamed').set('key', 'value')
        self.assertIsNone(result)
        self.assertEqual(collection.name, 'chain_renamed')
        self.assertEqual(collection.get('key'), 'value')

    def test_delete_collection_basic(self):
        collection_name = 'to_be_deleted'
        self.db.create_collection(collection_name)
        self.assertIn(collection_name, self.db.collections())
        self.db.delete_collection(collection_name)
        self.assertNotIn(collection_name, self.db.collections())

    def test_delete_nonexistent_collection(self):
        nonexistent_name = 'never_existed'
        self.assertNotIn(nonexistent_name, self.db.collections())
        self.db.delete_collection(nonexistent_name)
        self.assertNotIn(nonexistent_name, self.db.collections())

    def test_recreate_after_deletion(self):
        collection_name = 'recreate_me'

        collection = self.db.create_collection(collection_name)
        collection.set('original_key', 'original_value')
        self.db.delete_collection(collection_name)

        recreated = self.db.create_collection(collection_name)
        self.assertIsNone(recreated.get('original_key'))
        recreated.set('new_key', 'new_value')
        self.assertEqual(recreated.get('new_key'), 'new_value')

    def test_items_basic(self):
        items_collection = self.db.collection('items_test_collection')
        test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}

        for key, value in test_data.items():
            items_collection.set(key, value)

        retrieved_items = dict(items_collection.items())
        self.assertEqual(retrieved_items, test_data)

    def test_items_with_serializer(self):
        json_collection = self.db.collection('json_items_test', serializer='json')
        json_data = {
            'obj1': {'name': 'Object 1', 'id': 1},
            'obj2': {'name': 'Object 2', 'id': 2},
        }

        for key, value in json_data.items():
            json_collection.set(key, value)

        retrieved_json_items = dict(json_collection.items())
        self.assertEqual(retrieved_json_items, json_data)

        pickle_collection = self.db.collection('pickle_items_test', serializer='pickle')
        test_obj1 = TestClass('item1', 100)
        test_obj2 = TestClass('item2', 200)
        pickle_data = {'obj1': test_obj1, 'obj2': test_obj2}

        for key, value in pickle_data.items():
            pickle_collection.set(key, value)

        retrieved_pickle_items = dict(pickle_collection.items())
        self.assertEqual(len(retrieved_pickle_items), 2)
        self.assertEqual(retrieved_pickle_items['obj1'], test_obj1)
        self.assertEqual(retrieved_pickle_items['obj2'], test_obj2)


if __name__ == '__main__':
    unittest.main()
