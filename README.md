# keyv

Transform SQLite into a powerful and fast key-value store.

`keyv` is a lightweight library that turns SQLite into a NoSQL key-value database, combining the best of both worlds: the simplicity of key-value stores with the reliability of SQLite. Perfect for applications that need a robust local database without the complexity of a full DBMS.

# Quick Start

For simple use cases, you can create a default collection right away:

```python
# Create a database with a default collection
db = keyv.connect('keyv.db').collection('main')

# Now you can use the collection methods directly
db.set('key1', 'value1')
db.get('key1')
# >>> 'value1'
```

# Usage

```bash
pip install keyv
```

```python
import keyv

# initialize the database
db = keyv.connect('keyv.db') # or .anything, since is sqlite3 engine, you can choose any

# create or get a collection
collection = db.collection('my_collection')

# insert a key-value pair
collection.set('key1', 'value1')

# by default the .set method will replace existing values
# if you want to prevent overwriting existing keys, you can
# use replace_if_exists=False
collection.set('key1', 'value1', replace_if_exists=False)

# retrieve a value by key
collection.get('key1')
# >>> 'value1'
# note: the get method returns None for non-existing keys

# update a value
collection.update('key1', 123)
collection.get('key1')
# >>> 123

# delete a key-value pair
collection.delete('key1')

# search for keys by value
collection.set('key2', 'common_value')
collection.set('key3', 'common_value')
collection.search('common_value')
# >>> ['key2', 'key3']

# retrieve all keys, values and items
collection.keys()
# >>> ['key2', 'key3']

# retrieve all values
collection.values()
# >>> ['common_value', 'common_value']

# retrieve all key-value pairs
collection.items()
# >>> [('key2', 'common_value'), ('key3', 'common_value')]

# check if a key exists
collection.key_exists('key2')
# >>> True

# rename a collection
collection.change_name('new_collection_name')

# delete a collection
db.delete_collection('new_collection_name')

# get list of all collections
db.collections()
# >>> ['collection1', 'collection2', ...]

# iterate over key-value pairs
for key, value in collection.iteritems():
    print(f"{key}: {value}")

# iterate over keys
for key in collection.iterkeys():
    print(key)

# iterate over values
for value in collection.itervalues():
    print(value)

# get with default value for missing keys
collection.get('missing_key', default='default_value')
# >>> 'default_value'

# raise exception for missing keys
try:
    collection.get('missing_key', raise_if_missing=True)
except ValueError:
    print("Key not found!")
```

Collections allow you to organize your data into separate namespaces within the same database file.
In practice, they are tables inside the SQLite file.

# License

This project is licensed under the MIT License. See the LICENSE file for details.

# Contributing

Contributions are welcome! Please feel free to submit a PR.