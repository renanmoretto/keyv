# keyv

A lightweight, file-based NoSQL key-value database designed for simple and practical uses. It requires no external dependencies and stores all data locally in a single file. Perfect for applications needing a straightforward and efficient storage solution.

```bash
pip install keyv
```
# How it works
keyv uses sqlite3 as its engine, thereby benefiting from its power, integrity, and practicality. This looks strange but it works like a charm.

# Usage

```python
import keyv

# initialize the database
db = keyv.connect('db.keyv') # or .db, or .anything, you choose
```
###
```python
# insert a key-value pair
db.put('key1', 'value1')

# by default the .put method is unique safe, i.e. it raises an error
# if the key already exists. if you don't want that behavior you can
# use replace_if_exists=True.
db.put('key1', 'value1', replace_if_exists=True)

# retrieve a value by key
db.get('key1')
# output: 'value1'
# note: the get method returns None for non-existing keys

# update a value
db.update('key1', 123)
db.get('key1')
# output: 123

# delete a key-value pair
db.delete('key1')

# search for keys by value
db.put('key2', 'common_value')
db.put('key3', 'common_value')
db.search('common_value')
# output: ['key2', 'key3']

# retrieve all keys
db.keys()
# output: ['key2', 'key3']
```
###
keyv uses pickle to serialize data (keys and values). This means you are free to use any python type that can be serialized by pickle for both keys and values.

# Collections

Collections allow you to organize your data into separate namespaces within the same database file.
In practice, they are tables inside the SQLite file.
Here's how you can use them:

```python
# Create a new collection
collection = db.collection('my_collection')

# Insert a key-value pair into the collection
collection.put('key1', 'value1')

# Retrieve a value by key from the collection
collection.get('key1')
# Output: 'value1'

# Update a value in the collection
collection.update('key1', 'new_value')

# Check if a key exists in the collection
collection.key_exists('key1')
# Output: True

# Delete a key-value pair from the collection
collection.delete('key1')

# Search for keys by value in the collection
collection.put('key2', 'common_value')
collection.put('key3', 'common_value')
collection.search('common_value')
# Output: ['key2', 'key3']

# Retrieve all keys in the collection
collection.keys()
# Output: ['key2', 'key3']

# Retrieve all values in the collection
collection.values()
# Output: ['common_value', 'common_value']
```

Collections provide a flexible way to manage different sets of data within the same database, making it easier to organize and access your data efficiently.
