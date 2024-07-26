# nosqueel

A lightweight, file-based NoSQL key-value database designed for simple and practical uses. It requires no external dependencies and stores all data locally in a single file. Perfect for applications needing a straightforward and efficient storage solution.

```bash
pip install nosqueel
```
# How it works
nosqueel uses sqlite3 as its engine, thereby benefiting from its power, integrity, and practicality. This looks strange but it works like a charm.

# Usage

```python
from nosqueel import NoSqueel

# initialize the database
db = NoSqueel('db.nosqueel') # or .db, or .anything, you choose
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
value = db.get('key1')
print(value)  # output: 'value1'
# note: the get method returns None for non-existing keys

# update a value
db.update('key1', 123)
updated_value = db.get('key1')
print(updated_value)  # output: 123

# delete a key-value pair
db.delete('key1')

# search for keys by value
db.put('key2', 'common_value')
db.put('key3', 'common_value')
keys = db.search('common_value')
print(keys)  # output: ['key2', 'key3']

# retrieve all keys
all_keys = db.keys()
print(all_keys)  # output: ['key2', 'key3']
```
###
Nosqueel uses pickle to serialize data (keys and values). This means you are free to use any python type that can be serialized using pickle for both keys and values.

## TODO
- [ ] begin immediate transaction (for better locking)
- [ ] clean code/make it more efficient