from __future__ import annotations

import json
import pickle
import sqlite3
from pathlib import Path
from sqlite3 import Connection
from typing import Any, Union, List, Literal, Optional, Iterator


class Collection:
    def __init__(
        self,
        db: KeyVDatabase,
        name: str,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ):
        self.db = db
        self.name = name
        self.serializer = serializer

    def __str__(self):
        return f'Collection(name={self.name}, db={self.db.path}, serializer={self.serializer})'

    def __repr__(self):
        return self.__str__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

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

    def _serialize_if_provided(
        self,
        value: Any,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ) -> Any:
        """Serialize the value using the specified serializer or the collection's default serializer.

        Args:
            value: The value to serialize
            serializer: Optional serializer to use, overrides the collection's serializer

        Returns:
            The serialized value if a serializer is specified, otherwise the original value
        """
        # use the provided serializer or fall back to the collection's serializer
        effective_serializer = serializer or self.serializer

        if effective_serializer == 'json':
            return json.dumps(value).encode('utf-8')
        elif effective_serializer == 'pickle':
            return pickle.dumps(value)
        return value

    def _deserialize_if_provided(
        self,
        value: Any,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ) -> Any:
        """Deserialize the value using the specified serializer or the collection's default serializer.

        Args:
            value: The value to serialize
            serializer: Optional serializer to use, overrides the collection's serializer

        Returns:
            The de  serialized value if a serializer is specified, otherwise the original value
        """
        # use the provided serializer or fall back to the collection's serializer
        effective_serializer = serializer or self.serializer

        if effective_serializer == 'json':
            return json.loads(value.decode('utf-8'))
        elif effective_serializer == 'pickle':
            return pickle.loads(value)
        return value

    def set(
        self,
        key: Any,
        value: Any,
        replace_if_exists: bool = False,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ):
        """
        Inserts a key-value pair into the collection.

        Args:
            key: The key to insert.
            value: The value to associate with the key.
            replace_if_exists: If True, replaces the value if the key already exists. Defaults to False.
            serializer: The serializer to use. Defaults to None.
                NOTE: this overwrites the serializer in the db/collection instance.
                For example, if the db/collection instance has a serializer of 'json',
                and you pass in this function a serializer='pickle', pickle will be used
                for this specific key-value pair.

        Raises:
            ValueError: If the key already exists and replace_if_exists is False.
        """
        if self.key_exists(key):
            if replace_if_exists:
                self.update(key, value)
                return
            raise ValueError(f'key {key} already exists')

        value = self._serialize_if_provided(value, serializer)

        self._execute_sql(
            f'insert into {self.name} (key, value) values (?, ?)',
            (key, value),
            commit=True,
        )

    def get(
        self,
        key: Any,
        default: Any = None,
        raise_if_missing: bool = False,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ) -> Any:
        """
        Retrieves the value associated with the given key.

        Args:
            key: The key to look up.
            default: The value to return if the key does not exist. Defaults to None.
            raise_if_missing: If True, raises a ValueError if the key does not exist. Defaults to False.
            serializer: The serializer to use. Defaults to None.

        Returns:
            The value associated with the key, or None if the key does not exist.
        """
        result = self._execute_sql(
            f'select value from {self.name} where key = ?', (key,)
        )
        if result:
            return self._deserialize_if_provided(result[0][0], serializer)
        if raise_if_missing:
            raise ValueError(f'key {key} does not exist')
        return default

    def update(
        self,
        key: Any,
        value: Any,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ):
        """
        Updates the value associated with the given key.

        Args:
            key: The key to update.
            value: The new value to associate with the key.
            serializer: The serializer to use. Defaults to None.
        """
        value = self._serialize_if_provided(value, serializer)
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
        value = self._serialize_if_provided(value)
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

        NOTE: this is an expensive operation, and should be used sparingly. We recommend using .iterkeys() instead.
        """
        result = self._execute_sql(f'select key from {self.name}')
        if result:
            return [row[0] for row in result]
        return []

    def iterkeys(self) -> Iterator[Any]:
        """
        Iterates over all keys in the collection.

        Returns:
            An iterator over all keys in the collection.
        """
        with self.db._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f'select key from {self.name}')
            for row in cursor:
                yield row[0]

    def values(
        self,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ) -> List[Any]:
        """
        Retrieves all values in the collection.

        Args:
            serializer: The serializer to use. Defaults to None.

        Returns:
            A list of all values in the collection.

        NOTE: this is an expensive operation, and should be used sparingly. We recommend using .itervalues() instead.
        """
        result = self._execute_sql(f'select value from {self.name}')
        if result:
            return [self._deserialize_if_provided(row[0], serializer) for row in result]
        return []

    def itervalues(
        self,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ) -> Iterator[Any]:
        """
        Iterates over all values in the collection.

        Args:
            serializer: The serializer to use. Defaults to None.

        Returns:
            An iterator over all values in the collection.
        """
        with self.db._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f'select value from {self.name}')
            for row in cursor:
                yield self._deserialize_if_provided(row[0], serializer)

    def iteritems(
        self,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ) -> Iterator[tuple[Any, Any]]:
        """
        Iterates over all key-value pairs in the collection.

        Args:
            serializer: The serializer to use for values. Defaults to None.

        Returns:
            An iterator over all key-value pairs in the collection as (key, value) tuples.
        """
        with self.db._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f'select key, value from {self.name}')
            for key, value in cursor:
                yield (key, self._deserialize_if_provided(value, serializer))

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
        init_command: str = 'PRAGMA journal_mode=WAL; PRAGMA synchronous=1;',
        isolation_level: str = 'IMMEDIATE',
        **sqlite_kwargs,
    ):
        """Initialize a new KeyVDatabase instance.

        Args:
            path (Union[str, Path]): Path to the SQLite database file. If a string is provided,
                it will be converted to a Path object.
            init_command (str): SQL command(s) to execute when initializing the database.
                This is typically used to set up PRAGMAs and other database settings.
            isolation_level (str): SQLite isolation level for transactions. Common values
                include 'DEFERRED', 'IMMEDIATE', and 'EXCLUSIVE'.
            **sqlite_kwargs: Additional keyword arguments to pass to sqlite3.connect().
                These can be used to configure various SQLite connection parameters.

        Note:
            The database directory will be created if it doesn't exist. The database
            connection is not established immediately, but is created lazily when needed.
        """
        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self._init_command = init_command
        self._sqlite_kwargs = sqlite_kwargs
        self._isolation_level = isolation_level
        self._conn: Connection | None = None

        self._create_dir_if_not_exists()
        self._init()

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
        with self._get_conn() as conn:
            cursor = conn.cursor()
            commands = self._init_command.split(';')
            for command in commands:
                cursor.execute(command)
            conn.commit()

    def _create_table_in_db(self, name: str):
        with self._get_conn() as conn:
            conn.execute(
                f'create table if not exists {name} (key blob unique, value blob)'
            )
            conn.execute(f'create unique index if not exists idx_key on {name}(key)')
            conn.commit()

    def close(self):
        """
        Closes the database connection.
        """
        self._get_conn().close()

    def create_collection(
        self,
        name: str,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ) -> Collection:
        """
        Creates a new collection in the database.

        Args:
            name: The name of the collection.
            serializer: The serializer to use. Defaults to None.
        Returns:
            The newly created collection instance.
        """
        self._create_table_in_db(name=name)
        return self.collection(name=name, serializer=serializer)

    def collection(
        self,
        name: str,
        create_if_not_exists: bool = True,
        serializer: Optional[Literal['json', 'pickle']] = None,
    ) -> Collection:
        """
        Retrieves a collection by name, optionally creating it if it does not exist.

        Args:
            name: The name of the collection.
            create_if_not_exists: If True, creates the collection if it does not exist. Defaults to True.
            serializer: The serializer to use. Defaults to None.

        Returns:
            The collection instance.

        Raises:
            ValueError: If the collection does not exist and create_if_not_exists is False.
        """
        if name in self.collections():
            return Collection(db=self, name=name, serializer=serializer)

        if create_if_not_exists:
            return self.create_collection(name=name, serializer=serializer)

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
    init_command: str = 'PRAGMA journal_mode=WAL; PRAGMA synchronous=1;',
    isolation_level: str = 'IMMEDIATE',
) -> KeyVDatabase:
    """
    Returns a database instance

    Args:
        path: Path to the database file.

        init_command:
            SQL command/pragmas to initialize the database.
            defaults to: 'PRAGMA journal_mode=WAL;PRAGMA synchronous=1;'

        isolation_level:
            SQLite isolation level.
            defaults to: 'IMMEDIATE'

    Returns:
        Database instance
    """
    return KeyVDatabase(
        path=path,
        init_command=init_command,
        isolation_level=isolation_level,
    )
