import pickle
import sqlite3
from pathlib import Path
from sqlite3 import Connection, IntegrityError
from typing import Any


def _encode(x: Any) -> bytes:
    return pickle.dumps(x)


def _decode(x: Any) -> bytes:
    return pickle.loads(x)


class KeyValueDB:
    def __init__(self, path: str | Path, wal_mode: bool = True):
        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self._wal_mode = wal_mode
        self._conn: Connection | None = None
        self._create_dir_if_not_exists()
        self._init()

    def _create_dir_if_not_exists(self):
        parents = self.path.parent
        if not parents.exists():
            parents.mkdir(parents=True, exist_ok=True)

    def _get_conn(self):
        if self._conn is None:
            self._conn = sqlite3.connect(self.path, check_same_thread=False)
        return self._conn

    def _ensure_wal_mode(self):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")
            current_mode = cursor.fetchone()[0]
            i = 0
            while current_mode != "wal":
                if i > 10:
                    raise RuntimeError("error trying to setting wal mode")
                cursor.execute("PRAGMA journal_mode=WAL;")
                current_mode = cursor.fetchone()[0]
                i += 1
            conn.commit()

    def _init(self):
        conn = self._get_conn()
        conn.execute("create table if not exists data (key blob unique, value blob)")
        conn.execute("create unique index if not exists idx_key on data(key)")
        conn.commit()

        self._ensure_wal_mode()

    def put(self, key: Any, value: Any):
        kp = _encode(key)
        vp = _encode(value)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("insert into data (key, value) values (?, ?)", (kp, vp))
            except IntegrityError:
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

    def search(self, value: Any) -> list[Any]:
        vp = _encode(value)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("select key from data where value = ?", (vp,))
            result = cursor.fetchall()
            if result:
                data = [_decode(row[0]) for row in result]
                return data
            return []

    def keys(self) -> list[Any]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("select key from data")
            result = cursor.fetchall()
            if result:
                data = [_decode(row[0]) for row in result]
                return data
            return []
