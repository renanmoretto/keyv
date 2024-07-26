import pickle
import sqlite3
from pathlib import Path
from sqlite3 import Connection, IntegrityError
from typing import Any, Union, List


def _encode(x: Any) -> bytes:
    return pickle.dumps(x)


def _decode(x: Any) -> bytes:
    return pickle.loads(x)


class NoSqueel:
    def __init__(
        self,
        path: Union[str, Path],
        wal_mode: bool = True,
        synchronous: int = 1,
        **kwargs,
    ):
        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self._sqlite_kwargs = kwargs
        self._wal_mode = wal_mode
        self._synchronous = synchronous
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
                **self._sqlite_kwargs,
            )
        return self._conn

    def _init(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        if self._wal_mode:
            cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute(f"PRAGMA synchronous={self._synchronous};")
        conn.commit()

        # Creates base table
        conn.execute("create table if not exists data (key blob unique, value blob)")
        conn.execute("create unique index if not exists idx_key on data(key)")
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
                cursor.execute("insert into data (key, value) values (?, ?)", (kp, vp))
            except IntegrityError:
                if replace_if_exists:
                    self.update(key, value)
                    return
                raise ValueError(f"key {key} already exists")
            conn.commit()

    def get(self, key: Any) -> Any:
        kp = _encode(key)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("select value from data where key = ?", (kp,))
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
            cursor.execute("update data set value = ? where key = ?", (vp, kp))
            conn.commit()

    def delete(self, key: Any):
        kp = _encode(key)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("delete from data where key = ?", (kp,))
            conn.commit()

    def search(self, value: Any) -> List[Any]:
        vp = _encode(value)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("select key from data where value = ?", (vp,))
            result = cursor.fetchall()
            if result:
                data = [_decode(row[0]) for row in result]
                return data
            return []

    def keys(self) -> List[Any]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("select key from data")
            result = cursor.fetchall()
            if result:
                data = [_decode(row[0]) for row in result]
                return data
            return []
