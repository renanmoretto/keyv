import shutil
import sqlite3
import unittest
import tempfile
from pathlib import Path

import keyv


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

    def test_collection_set_replace_if_exists_false(self):
        key, value = 'key1', 'value1'
        self.collection.set(key, value)
        with self.assertRaises(ValueError):
            self.collection.set(key, 'value2', replace_if_exists=False)

    def test_collection_set_replace_if_exists_true(self):
        self.collection.set('k1', 'old')
        self.collection.set('k1', 'new')
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

    def test_iteritems(self):
        items_collection = self.db.collection('items_collection')
        test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}

        for key, value in test_data.items():
            items_collection.set(key, value)

        retrieved_items = dict(items_collection.iteritems())
        self.assertEqual(retrieved_items, test_data)

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

    # Additional test cases for edge cases
    def test_empty_values(self):
        """Test setting and getting empty strings and None values."""
        self.collection.set('empty_string', '')
        self.collection.set('none_value', None)

        self.assertEqual(self.collection.get('empty_string'), '')
        self.assertEqual(self.collection.get('none_value'), None)

    def test_special_characters(self):
        """Test keys and values with special characters."""
        special_chars_key = '!@#$%^&*()_+{}[]|\\;:\'",.<>/?'
        special_chars_value = 'áéíóúñÁÉÍÓÚÑüÜ€£¥©®™'

        self.collection.set(special_chars_key, special_chars_value)
        self.assertEqual(self.collection.get(special_chars_key), special_chars_value)

    def test_unicode_characters(self):
        """Test unicode characters in keys and values."""
        unicode_key = '你好世界'
        unicode_value = 'こんにちは世界'

        self.collection.set(unicode_key, unicode_value)
        self.assertEqual(self.collection.get(unicode_key), unicode_value)

    def test_numeric_keys(self):
        """Test using numeric keys of different types."""
        test_data = {42: 'integer key', 3.14: 'float key', -100: 'negative integer'}

        for key, value in test_data.items():
            self.collection.set(key, value)
            self.assertEqual(self.collection.get(key), value)

    def test_update_nonexistent_key(self):
        """Test updating a non-existent key."""
        with self.assertRaises(Exception):
            self.collection.update('nonexistent_key', 'value')
            self.assertIsNone(self.collection.get('nonexistent_key'))

    def test_collection_name_validation(self):
        """Test that collection names are properly validated to prevent SQL injection."""
        invalid_names = [
            'collection; DROP TABLE test_collection;--',
            "collection' OR '1'='1",
            'collection" OR "1"="1',
        ]

        for name in invalid_names:
            try:
                # This should either raise an exception or sanitize the name
                collection = self.db.collection(name)
                # If it doesn't raise an exception, make sure we can use it safely
                collection.set('test_key', 'test_value')
                self.assertEqual(collection.get('test_key'), 'test_value')
                # Check that no other collections were affected
                self.assertIn('test_collection', self.db.collections())
            except Exception as e:
                # If it raises an exception, that's also acceptable
                self.assertIsInstance(e, (ValueError, sqlite3.OperationalError))

    def test_connection_recovery(self):
        """Test that the connection can recover after being closed."""
        # Close the connection
        self.db.close()

        # Try to use the collection
        self.collection.set('recovery_key', 'recovery_value')
        self.assertEqual(self.collection.get('recovery_key'), 'recovery_value')

    def test_database_initialization_parameters(self):
        """Test creating a database with different initialization parameters."""
        custom_db_path = Path(self.test_dir) / 'custom_params.db'

        # Test with different PRAGMA settings
        custom_db = keyv.connect(
            custom_db_path,
            init_command='PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF;',
            isolation_level='EXCLUSIVE',
        )

        # Check that we can use the database
        collection = custom_db.collection('custom_collection')
        collection.set('custom_key', 'custom_value')
        self.assertEqual(collection.get('custom_key'), 'custom_value')

        # Clean up
        custom_db.close()

    def test_concurrent_access(self):
        """Test concurrent access to the database from multiple threads."""
        import threading
        import random

        # Create a new collection for this test
        collection_name = 'concurrent_test'
        concurrent_collection = self.db.collection(collection_name)

        # Number of threads and operations
        num_threads = 10
        operations_per_thread = 50

        # Counter for successful operations
        successful_ops = [0]
        lock = threading.Lock()

        def worker(thread_id):
            """Worker function for each thread."""
            for i in range(operations_per_thread):
                try:
                    key = f'thread_{thread_id}_key_{i}'
                    value = f'value_{random.randint(1, 1000)}'

                    # Randomly choose an operation
                    op = random.choice(['set', 'get', 'update', 'delete'])

                    if op == 'set':
                        concurrent_collection.set(key, value, replace_if_exists=True)
                    elif op == 'get':
                        concurrent_collection.get(key, default='not_found')
                    elif op == 'update':
                        try:
                            concurrent_collection.update(key, f'updated_{value}')
                        except Exception:
                            # Update might fail if key doesn't exist, that's ok
                            pass
                    elif op == 'delete':
                        concurrent_collection.delete(key)

                    with lock:
                        successful_ops[0] += 1
                except Exception:
                    # If any operation fails, continue with the next one
                    pass

        # Create and start threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Check that some operations were successful
        self.assertGreater(successful_ops[0], 0)

    def test_collection_reopening(self):
        """Test that a collection can be reopened after closing the database."""
        # Create data in collection
        self.collection.set('reopen_key', 'reopen_value')

        # Close database connection
        self.db.close()

        # Reopen database and collection
        reopened_db = keyv.connect(self.db_path)
        reopened_collection = reopened_db.collection('test_collection')

        # Verify data is still accessible
        self.assertEqual(reopened_collection.get('reopen_key'), 'reopen_value')

        # Clean up
        reopened_db.close()

    def test_collection_isolation(self):
        """Test that collections are properly isolated from each other."""
        # Create two collections
        collection1 = self.db.collection('isolation_test_1')
        collection2 = self.db.collection('isolation_test_2')

        # Add data to collection1
        collection1.set('key1', 'value1')

        # Verify key doesn't exist in collection2
        self.assertIsNone(collection2.get('key1'))

        # Add same key with different value to collection2
        collection2.set('key1', 'different_value')

        # Verify both collections maintain separate values
        self.assertEqual(collection1.get('key1'), 'value1')
        self.assertEqual(collection2.get('key1'), 'different_value')

    def test_binary_data(self):
        """Test storing and retrieving binary data."""
        # Binary data
        binary_data = b'\x00\x01\x02\x03\xff\xfe\xfd\xfc'
        self.collection.set('binary', binary_data)
        retrieved = self.collection.get('binary')
        self.assertEqual(retrieved, binary_data)

        # Image-like binary data (e.g., small PNG header)
        png_header = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00'
        self.collection.set('png_header', png_header)
        self.assertEqual(self.collection.get('png_header'), png_header)

    def test_boolean_values(self):
        """Test storing and retrieving boolean values."""
        self.collection.set('true_value', True)
        self.collection.set('false_value', False)

        self.assertEqual(self.collection.get('true_value'), True)
        self.assertEqual(self.collection.get('false_value'), False)

        # Test searching for boolean values
        results = self.collection.search(True)
        self.assertIn('true_value', results)
        self.assertNotIn('false_value', results)

    def test_transaction_rollback(self):
        """Test that transactions are rolled back when exceptions occur."""
        # Attempt to create a collection with an invalid name to trigger an exception
        try:
            with self.db._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute('BEGIN TRANSACTION')

                # Add a key-value pair that should be rolled back
                cursor.execute(
                    f'insert into {self.collection.name} (key, value) values (?, ?)',
                    ('transaction_key', 'transaction_value'),
                )

                # Execute invalid SQL to trigger an exception
                cursor.execute('INVALID SQL STATEMENT')

                # This should not be reached
                conn.commit()
        except sqlite3.OperationalError:
            # Exception expected
            pass

        # Verify the key doesn't exist (transaction should have been rolled back)
        self.assertIsNone(self.collection.get('transaction_key'))

    def test_serialization(self):
        """Test serializing complex data structures before storage."""
        import json
        import pickle

        # Dictionary to be stored
        data_dict = {'nested': {'dict': True}, 'list': [1, 2, 3]}

        # JSON serialization
        self.collection.set('json_data', json.dumps(data_dict))
        retrieved_json = self.collection.get('json_data')
        self.assertEqual(json.loads(retrieved_json), data_dict)

        # Pickle serialization
        pickled_data = pickle.dumps(data_dict)
        self.collection.set('pickle_data', pickled_data)
        retrieved_pickle = self.collection.get('pickle_data')
        self.assertEqual(pickle.loads(retrieved_pickle), data_dict)

        # Complex nested structure with mixed types
        complex_data = {
            'nested_list': [1, 2, {'key': 'value'}, [4, 5]],
            'tuple_data': (1, 2, 3),
            'set_data': [1, 2, 3],  # Convert set to list for JSON compatibility
        }

        # Demonstrate serialization as best practice
        self.collection.set('complex_json', json.dumps(complex_data))
        retrieved_complex = json.loads(self.collection.get('complex_json'))
        self.assertEqual(retrieved_complex['nested_list'], complex_data['nested_list'])
        # Note: tuple becomes list in JSON serialization
        self.assertEqual(
            retrieved_complex['tuple_data'], list(complex_data['tuple_data'])
        )


if __name__ == '__main__':
    unittest.main()
