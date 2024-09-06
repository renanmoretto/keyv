from __future__ import annotations

import pickle
import sqlite3
from pathlib import Path
from sqlite3 import Connection, IntegrityError
from typing import Any, Union, List


_DEFAULT_COLLECTION_NAME = 'main'


def _encode(x: Any) -> bytes:
    return pickle.dumps(x)


def _decode(x: Any) -> bytes:
    return pickle.loads(x)


class Collection:
    def __init__(self, db: KeyVDatabase, name: str, use_pickle: bool):
        self.db = db
        self.name = name
        self.use_pickle = use_pickle

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
        if self.key_exists(key):
            if replace_if_exists:
                self.update(key, value)
                return
            raise ValueError(f'key {key} already exists')

        if self.use_pickle:
            key = _encode(key)
            value = _encode(value)

        self._execute_sql(
            f'insert into {self.name} (key, value) values (?, ?)',
            (key, value),
            commit=True,
        )

    def get(self, key: Any) -> Any:
        if self.use_pickle:
            key = _encode(key)

        result = self._execute_sql(
            f'select value from {self.name} where key = ?', (key,)
        )

        if result:
            value = result[0][0]
            return _decode(value) if self.use_pickle else value

        return None

    def update(self, key: Any, value: Any):
        if self.use_pickle:
            key = _encode(key)
            value = _encode(value)

        self._execute_sql(
            f'update {self.name} set value = ? where key = ?',
            (value, key),
            commit=True,
        )

    def delete(self, key: Any):
        if self.use_pickle:
            key = _encode(key)

        self._execute_sql(f'delete from {self.name} where key = ?', (key,), commit=True)

    def search(self, value: Any) -> List[Any]:
        if self.use_pickle:
            value = _encode(value)

        result = self._execute_sql(
            f'select key from {self.name} where value = ?', (value,)
        )
        if result:
            if self.use_pickle:
                data = [_decode(row[0]) for row in result]
            else:
                data = [row[0] for row in result]
            return data
        return []

    def keys(self) -> List[Any]:
        result = self._execute_sql(f'select key from {self.name}')
        if result:
            if self.use_pickle:
                data = [_decode(row[0]) for row in result]
            else:
                data = [row[0] for row in result]
            return data
        return []

    def values(self) -> List[Any]:
        result = self._execute_sql(f'select value from {self.name}')
        if result:
            if self.use_pickle:
                data = [_decode(row[0]) for row in result]
            else:
                data = [row[0] for row in result]
            return data
        return []

    def key_exists(self, key: Any) -> bool:
        if self.use_pickle:
            key = _encode(key)

        sql = f'select count(*) from {self.name} where key = ?'
        result = self._execute_sql(sql, (key,))
        return result[0][0] > 0


class KeyVDatabase:
    def __init__(
        self,
        path: Union[str, Path],
        init_command: str,
        isolation_level: str,
        use_pickle: bool = True,
        **kwargs,
    ):
        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self._init_command = init_command
        self._sqlite_kwargs = kwargs
        self._isolation_level = isolation_level
        self._conn: Connection | None = None
        self._use_pickle = use_pickle

        self._create_dir_if_not_exists()
        self._init()
        self.create_collection(_DEFAULT_COLLECTION_NAME, self._use_pickle)

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
        """Closes the database connection"""
        self._get_conn().close()

    def put(self, key: Any, value: Any, replace_if_exists: bool = False):
        self.from_(_DEFAULT_COLLECTION_NAME).put(key, value, replace_if_exists)

    def get(self, key: Any) -> Any:
        return self.from_(_DEFAULT_COLLECTION_NAME).get(key)

    def update(self, key: Any, value: Any):
        self.from_(_DEFAULT_COLLECTION_NAME).update(key, value)

    def delete(self, key: Any):
        self.from_(_DEFAULT_COLLECTION_NAME).delete(key)

    def search(self, value: Any) -> List[Any]:
        return self.from_(_DEFAULT_COLLECTION_NAME).search(value)

    def keys(self) -> List[Any]:
        return self.from_(_DEFAULT_COLLECTION_NAME).keys()

    def values(self) -> List[Any]:
        return self.from_(_DEFAULT_COLLECTION_NAME).values()

    def from_(
        self,
        collection_name: str,
        create_if_not_exists: bool = True,
    ) -> Collection:
        return self.collection(
            collection_name,
            create_if_not_exists=create_if_not_exists,
            use_pickle=self._use_pickle,
        )

    def create_collection(self, name: str, use_pickle: bool = True) -> Collection:
        self._create_table_in_db(name=name)
        return self.collection(name=name, use_pickle=use_pickle)

    def collection(
        self,
        name: str,
        create_if_not_exists: bool = True,
        use_pickle: bool = True,
    ) -> Collection:
        if name in self.collections():
            return Collection(db=self, name=name, use_pickle=use_pickle)

        if create_if_not_exists:
            return self.create_collection(name=name, use_pickle=use_pickle)

        raise ValueError(f'collection {name} does not exist')

    def collections(self) -> List[str]:
        """Executes SQL query to get all tables in the sqlite database"""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [table[0] for table in cursor.fetchall()]


def connect(
    path: Union[str, Path],
    init_command: str | None = None,
    isolation_level: str = 'IMMEDIATE',
    use_pickle: bool = True,
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

        use_pickle:
            If True, the library will use pickle to serialize and deserialize data.
            If False, the library will store data as it is.

    returns:
        Database instance
    """
    if init_command is None:
        init_command = 'PRAGMA journal_mode=WAL; PRAGMA synchronous=1;'

    return KeyVDatabase(
        path=path,
        init_command=init_command,
        isolation_level=isolation_level,
        use_pickle=use_pickle,
    )
