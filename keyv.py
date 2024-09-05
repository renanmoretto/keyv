import pickle
import sqlite3
from pathlib import Path
from sqlite3 import Connection, IntegrityError
from typing import Any, Union, List


def _encode(x: Any) -> bytes:
    return pickle.dumps(x)


def _decode(x: Any) -> bytes:
    return pickle.loads(x)


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

        # Creates base table
        conn.execute('create table if not exists data (key blob unique, value blob)')
        conn.execute('create unique index if not exists idx_key on data(key)')
        conn.commit()

        # self._ensure_wal_mode()

    def close(self):
        """Closes the database connection"""
        self._get_conn().close()

    def put(self, key: Any, value: Any, replace_if_exists: bool = False):
        kp = _encode(key)
        vp = _encode(value)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('insert into data (key, value) values (?, ?)', (kp, vp))
            except IntegrityError:
                if replace_if_exists:
                    self.update(key, value)
                    return
                raise ValueError(f'key {key} already exists')
            conn.commit()

    def get(self, key: Any) -> Any:
        kp = _encode(key)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('select value from data where key = ?', (kp,))
            result = cursor.fetchone()
            if result:
                vp = result[0]
                return _decode(vp)
            return None

    def update(self, key: Any, value: Any):
        # TODO
        # raises an good error key if does not exists
        # also option to create the key and not raise the error
        kp = _encode(key)
        vp = _encode(value)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('update data set value = ? where key = ?', (vp, kp))
            conn.commit()

    def delete(self, key: Any):
        kp = _encode(key)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('delete from data where key = ?', (kp,))
            conn.commit()

    def search(self, value: Any) -> List[Any]:
        vp = _encode(value)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('select key from data where value = ?', (vp,))
            result = cursor.fetchall()
            if result:
                data = [_decode(row[0]) for row in result]
                return data
            return []

    def keys(self) -> List[Any]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('select key from data')
            result = cursor.fetchall()
            if result:
                data = [_decode(row[0]) for row in result]
                return data
            return []


def connect(
    path: Union[str, Path],
    init_command: str | None = None,
    isolation_level: str = 'IMMEDIATE',
) -> KeyVDatabase:
    """Returns a new KeyVDatabase instance

    args:
        path: Path to the database file.

        init_command:
            SQL command/pragmas to initialize the database.
            defaults to: 'PRAGMA journal_mode=WAL;PRAGMA synchronous=1;'

        isolation_level: SQLite isolation level.

    returns:
        A new KeyVDatabase instance.
    """
    if init_command is None:
        init_command = 'PRAGMA journal_mode=WAL; PRAGMA synchronous=1;'
    return KeyVDatabase(path, init_command, isolation_level)
