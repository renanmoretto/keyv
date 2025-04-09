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
db.set('key1', 'value1')

# by default the .set method is unique safe, i.e. it raises an error
# if the key already exists. if you don't want that behavior you can
# use replace_if_exists=True.
db.set('key1', 'value1', replace_if_exists=True)

# retrieve a value by key
db.get('key1')
# >>> 'value1'
# note: the get method returns None for non-existing keys

# update a value
db.update('key1', 123)
db.get('key1')
# >>> 123

# delete a key-value pair
db.delete('key1')

# search for keys by value
db.set('key2', 'common_value')
db.set('key3', 'common_value')
db.search('common_value')
# >>> ['key2', 'key3']

# retrieve all keys
db.keys()
# >>> ['key2', 'key3']
```

# Collections

Collections allow you to organize your data into separate namespaces within the same database file.
In practice, they are tables inside the SQLite file.
Here's how you can use them:

```python
# Create a new collection
collection = db.collection('my_collection')

# Insert a key-value pair into the collection
collection.set('key1', 'value1')

# Retrieve a value by key from the collection
collection.get('key1')
# >>> 'value1'

# Update a value in the collection
collection.update('key1', 'new_value')

# Check if a key exists in the collection
collection.key_exists('key1')
# >>> True

# Delete a key-value pair from the collection
collection.delete('key1')

# Search for keys by value in the collection
collection.set('key2', 'common_value')
collection.set('key3', 'common_value')
collection.search('common_value')
# >>> ['key2', 'key3']

# Retrieve all keys in the collection
collection.keys()
# >>> ['key2', 'key3']

# Retrieve all values in the collection
collection.values()
# >>> ['common_value', 'common_value']
```

Collections provide a flexible way to manage different sets of data within the same database, making it easier to organize and access your data efficiently.
