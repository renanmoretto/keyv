# keyv

A lightweight, file-based NoSQL key-value database designed for simple and practical uses. It requires no external dependencies and stores all data locally in a single file. Perfect for applications needing a straightforward and efficient storage solution.

keyv uses sqlite3 as its engine, thereby benefiting from its power, integrity, and practicality. This looks strange but it works like a charm.

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

# by default the .set method is unique safe, i.e. it raises an error
# if the key already exists. if you don't want that behavior you can
# use replace_if_exists=True.
collection.set('key1', 'value1', replace_if_exists=True)

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

# Serializers

keyv supports serialization of complex data types using JSON or Pickle. You can specify a serializer when creating a collection or when setting individual values.

```python
import keyv

db = keyv.connect('keyv.db')

# Create a collection with JSON serialization
json_collection = db.collection('json_data', serializer='json')

# Store complex data types
json_collection.set('user', {'name': 'John', 'age': 30, 'roles': ['admin', 'user']})
json_collection.set('config', {'debug': True, 'theme': 'dark', 'cache_size': 1024})

# Retrieve data - it's automatically deserialized
user = json_collection.get('user')
print(user['name'])  # 'John'
print(user['roles'])  # ['admin', 'user']

# Create a collection with Pickle serialization for Python objects
pickle_collection = db.collection('pickle_data', serializer='pickle')

# Store Python objects
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

pickle_collection.set('person1', Person('Alice', 25))

# Retrieve and use the object
person = pickle_collection.get('person1')
print(person.name)  # 'Alice'

# Override collection serializer for a specific key-value pair/methods
json_collection.set('special_data', [1, 2, 3, 4], serializer='pickle')
json_collection.get('special_data', serializer='pickle')
```

When using serializers:
- JSON is good for data that needs to be human-readable or compatible with other systems
- Pickle is better for Python-specific objects but less secure and not cross-language compatible
- You can set a default serializer for a collection or specify one for individual operations
- Data is automatically deserialized when retrieved

# License

This project is licensed under the MIT License. See the LICENSE file for details.

# Contributing

Contributions are welcome! Please feel free to submit a PR.