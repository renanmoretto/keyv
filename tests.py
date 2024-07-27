import unittest
import tempfile
import shutil
from pathlib import Path
from nosqueel import NoSqueel


class TestNoSqueel(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "db.nosqueel"
        self.db = NoSqueel(self.db_path)

    def tearDown(self):
        if self.db._conn:
            self.db._conn.close()
        shutil.rmtree(self.test_dir)

    def test_put_and_get(self):
        key, value = "key1", "value1"
        self.db.put(key, value)
        retrieved_value = self.db.get(key)
        self.assertEqual(retrieved_value, value)

    def test_put_duplicate_key(self):
        key, value = "key1", "value1"
        self.db.put(key, value)
        with self.assertRaises(ValueError):
            self.db.put(key, "value2")

    def test_put_replace_if_exists_true(self):
        key = "key1"
        initial_value = 0
        new_value = 1

        self.db.put(key, initial_value)
        self.db.put(key, new_value, replace_if_exists=True)
        retrieved_value = self.db.get(key)
        self.assertEqual(retrieved_value, new_value)

    def test_update(self):
        key, value = "key1", "value1"
        new_value = "new_value1"
        self.db.put(key, value)
        self.db.update(key, new_value)
        retrieved_value = self.db.get(key)
        self.assertEqual(retrieved_value, new_value)

    def test_delete(self):
        key, value = "key1", "value1"
        self.db.put(key, value)
        self.db.delete(key)
        retrieved_value = self.db.get(key)
        self.assertIsNone(retrieved_value)

    def test_search(self):
        self.db.put("key1", "value1")
        self.db.put("key2", "value1")
        self.db.put("key3", "value2")
        results = self.db.search("value1")
        self.assertEqual(results, ["key1", "key2"])

    def test_keys(self):
        self.db.put("key1", "value1")
        self.db.put("key2", "value2")
        keys = self.db.keys()
        self.assertEqual(keys, ["key1", "key2"])


if __name__ == "__main__":
    unittest.main()
