from __future__ import annotations

import sqlite3
from pathlib import Path
from sqlite3 import Connection
from typing import Any, Union, List


_DEFAULT_COLLECTION_NAME = '__main__'


class Collection:
    def __init__(self, db: KeyVDatabase, name: str):
        self.db = db
        self.name = name

    def _execute_sql(
        self,
        query: str,
        params: tuple = (),
        commit: bool = False,
    ) -> List[Any]:
        with self.db._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            if commit:
                conn.commit()
            return cursor.fetchall()

    def put(self, key: Any, value: Any, replace_if_exists: bool = False):
        """
        Inserts a key-value pair into the collection.

        Args:
            key: The key to insert.
            value: The value to associate with the key.
            replace_if_exists: If True, replaces the value if the key already exists. Defaults to False.

        Raises:
            ValueError: If the key already exists and replace_if_exists is False.
        """
        if self.key_exists(key):
            if replace_if_exists:
                self.update(key, value)
                return
            raise ValueError(f'key {key} already exists')

        self._execute_sql(
            f'insert into {self.name} (key, value) values (?, ?)',
            (key, value),
            commit=True,
        )

    def get(self, key: Any) -> Any:
        """
        Retrieves the value associated with the given key.

        Args:
            key: The key to look up.

        Returns:
            The value associated with the key, or None if the key does not exist.
        """
        result = self._execute_sql(
            f'select value from {self.name} where key = ?', (key,)
        )
        return result[0][0] if result else None

    def update(self, key: Any, value: Any):
        """
        Updates the value associated with the given key.

        Args:
            key: The key to update.
            value: The new value to associate with the key.
        """
        self._execute_sql(
            f'update {self.name} set value = ? where key = ?',
            (value, key),
            commit=True,
        )

    def delete(self, key: Any):
        """
        Deletes the key-value pair associated with the given key.

        Args:
            key: The key to delete.
        """
        self._execute_sql(f'delete from {self.name} where key = ?', (key,), commit=True)

    def search(self, value: Any) -> List[Any]:
        """
        Searches for keys associated with the given value.

        Args:
            value: The value to search for.

        Returns:
            A list of keys associated with the value.
        """
        result = self._execute_sql(
            f'select key from {self.name} where value = ?', (value,)
        )
        if result:
            return [row[0] for row in result]
        return []

    def keys(self) -> List[Any]:
        """
        Retrieves all keys in the collection.

        Returns:
            A list of all keys in the collection.
        """
        result = self._execute_sql(f'select key from {self.name}')
        if result:
            return [row[0] for row in result]
        return []

    def values(self) -> List[Any]:
        """
        Retrieves all values in the collection.

        Returns:
            A list of all values in the collection.
        """
        result = self._execute_sql(f'select value from {self.name}')
        if result:
            return [row[0] for row in result]
        return []

    def key_exists(self, key: Any) -> bool:
        """
        Checks if a key exists in the collection.

        Args:
            key: The key to check.

        Returns:
            True if the key exists, False otherwise.
        """
        sql = f'select count(*) from {self.name} where key = ?'
        result = self._execute_sql(sql, (key,))
        return result[0][0] > 0


class KeyVDatabase:
    def __init__(
        self,
        path: Union[str, Path],
        init_command: str,
        isolation_level: str,
        **kwargs,
    ):
        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self._init_command = init_command
        self._sqlite_kwargs = kwargs
        self._isolation_level = isolation_level
        self._conn: Connection | None = None

        self._create_dir_if_not_exists()
        self._init()
        self.create_collection(_DEFAULT_COLLECTION_NAME)

    def _create_dir_if_not_exists(self):
        parents = self.path.parent
        if not parents.exists():
            parents.mkdir(parents=True, exist_ok=True)

    def _get_conn(self):
        if self._conn is None:
            self._conn = sqlite3.connect(
                self.path,
                check_same_thread=False,
                isolation_level=self._isolation_level,
                **self._sqlite_kwargs,
            )
        return self._conn

    def _init(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        commands = self._init_command.split(';')
        for command in commands:
            cursor.execute(command)
        conn.commit()

    def _create_table_in_db(self, name: str):
        conn = self._get_conn()
        conn.execute(f'create table if not exists {name} (key blob unique, value blob)')
        conn.execute(f'create unique index if not exists idx_key on {name}(key)')
        conn.commit()

    def close(self):
        """
        Closes the database connection.
        """
        self._get_conn().close()

    def put(self, key: Any, value: Any, replace_if_exists: bool = False):
        """
        Inserts a key-value pair into the default collection.

        Args:
            key: The key to insert.
            value: The value to associate with the key.
            replace_if_exists: If True, replaces the value if the key already exists. Defaults to False.

        Raises:
            ValueError: If the key already exists and replace_if_exists is False.
        """
        self.from_(_DEFAULT_COLLECTION_NAME).put(key, value, replace_if_exists)

    def get(self, key: Any) -> Any:
        """
        Retrieves the value associated with the given key from the default collection.

        Args:
            key: The key to look up.

        Returns:
            The value associated with the key, or None if the key does not exist.
        """
        return self.from_(_DEFAULT_COLLECTION_NAME).get(key)

    def update(self, key: Any, value: Any):
        """
        Updates the value associated with the given key in the default collection.

        Args:
            key: The key to update.
            value: The new value to associate with the key.
        """
        self.from_(_DEFAULT_COLLECTION_NAME).update(key, value)

    def delete(self, key: Any):
        """
        Deletes the key-value pair associated with the given key from the default collection.

        Args:
            key: The key to delete.
        """
        self.from_(_DEFAULT_COLLECTION_NAME).delete(key)

    def search(self, value: Any) -> List[Any]:
        """
        Searches for keys associated with the given value in the default collection.

        Args:
            value: The value to search for.

        Returns:
            A list of keys associated with the value.
        """
        return self.from_(_DEFAULT_COLLECTION_NAME).search(value)

    def keys(self) -> List[Any]:
        """
        Retrieves all keys in the default collection.

        Returns:
            A list of all keys in the default collection.
        """
        return self.from_(_DEFAULT_COLLECTION_NAME).keys()

    def values(self) -> List[Any]:
        """
        Retrieves all values in the default collection.

        Returns:
            A list of all values in the default collection.
        """
        return self.from_(_DEFAULT_COLLECTION_NAME).values()

    def key_exists(self, key: Any) -> bool:
        """
        Checks if a key exists in the default collection.

        Args:
            key: The key to check.

        Returns:
            True if the key exists, False otherwise.
        """
        return self.from_(_DEFAULT_COLLECTION_NAME).key_exists(key)

    def from_(
        self,
        collection_name: str,
        create_if_not_exists: bool = True,
    ) -> Collection:
        """
        Retrieves a collection by name, creating it if it does not exist.

        Args:
            collection_name: The name of the collection.
            create_if_not_exists: If True, creates the collection if it does not exist. Defaults to True.

        Returns:
            The collection instance.
        """
        return self.collection(
            collection_name,
            create_if_not_exists=create_if_not_exists,
        )

    def create_collection(
        self,
        name: str,
    ) -> Collection:
        """
        Creates a new collection in the database.

        Args:
            name: The name of the collection.

        Returns:
            The newly created collection instance.
        """
        self._create_table_in_db(name=name)
        return self.collection(name=name)

    def collection(
        self,
        name: str,
        create_if_not_exists: bool = True,
    ) -> Collection:
        """
        Retrieves a collection by name, optionally creating it if it does not exist.

        Args:
            name: The name of the collection.
            create_if_not_exists: If True, creates the collection if it does not exist. Defaults to True.

        Returns:
            The collection instance.

        Raises:
            ValueError: If the collection does not exist and create_if_not_exists is False.
        """
        if name in self.collections():
            return Collection(db=self, name=name)

        if create_if_not_exists:
            return self.create_collection(name=name)

        raise ValueError(f'collection {name} does not exist')

    def collections(self) -> List[str]:
        """
        Retrieves the names of all collections in the database.

        Returns:
            A list of collection names.
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [table[0] for table in cursor.fetchall()]


def connect(
    path: Union[str, Path],
    init_command: str | None = None,
    isolation_level: str = 'IMMEDIATE',
) -> KeyVDatabase:
    """
    Returns a database instance

    args:
        path: Path to the database file.

        init_command:
            SQL command/pragmas to initialize the database.
            defaults to: 'PRAGMA journal_mode=WAL;PRAGMA synchronous=1;'

        isolation_level:
            SQLite isolation level.
            defaults to: 'IMMEDIATE'

    returns:
        Database instance
    """
    if init_command is None:
        init_command = 'PRAGMA journal_mode=WAL; PRAGMA synchronous=1;'

    return KeyVDatabase(
        path=path,
        init_command=init_command,
        isolation_level=isolation_level,
    )
