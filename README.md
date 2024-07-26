# nosqueel

Lightweight NoSQL key-value database for simple and practical uses. 
Uses SQLite as its engine, thereby benefiting from its power, integrity, and practicality.

```bash
pip install nosqueel
```

# Usage

```python
from nosqueel import NoSqueel

# Initialize the database
db = NoSqueel('db.nosqueel') # or .db, or .anything, you choose
```
###
```python
# Insert a key-value pair
db.put('key1', 'value1')

# Retrieve a value by key
value = db.get('key1')
print(value)  # Output: value1

# Update a value
db.update('key1', 'new_value')
updated_value = db.get('key1')
print(updated_value)  # Output: new_value

# Delete a key-value pair
db.delete('key1')

# Search for keys by value
db.put('key2', 'common_value')
db.put('key3', 'common_value')
keys = db.search('common_value')
print(keys)  # Output: ['key2', 'key3']

# Retrieve all keys
all_keys = db.keys()
print(all_keys)  # Output: ['key2', 'key3']
```
###
nosqueel uses pickle to serialize data, i.e. keys and values.
So for keys and values, you are completely free to use any python type that can be serialized using pickle.