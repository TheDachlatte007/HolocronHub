"""Durable provider snapshots and bounded, single-flight background refreshes."""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock
from typing import Callable


class WarframeCacheStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._schema_lock = Lock()
        self._ready = False

    @contextmanager
    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        with self._schema_lock:
            if not self._ready:
                connection.executescript("""
                    PRAGMA journal_mode=WAL;
                    CREATE TABLE IF NOT EXISTS provider_payloads (
                        cache_key TEXT PRIMARY KEY,
                        updated_at REAL NOT NULL,
                        payload_json TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS refresh_attempts (
                        cache_key TEXT PRIMARY KEY,
                        attempted_at REAL NOT NULL,
                        error TEXT
                    );
                """)
                self._ready = True
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def read(self, key: str):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT updated_at, payload_json FROM provider_payloads WHERE cache_key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError):
            return None
        return {"ts": row["updated_at"], "value": payload}

    def put(self, key: str, value: dict, *, timestamp: float | None = None):
        stamp = time.time() if timestamp is None else timestamp
        with self._connect() as connection:
            connection.execute("""
                INSERT INTO provider_payloads VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    updated_at=excluded.updated_at, payload_json=excluded.payload_json
                WHERE excluded.updated_at >= provider_payloads.updated_at
            """, (key, stamp, json.dumps(value, ensure_ascii=False, allow_nan=False)))

    def attempt(self, key: str, error: str | None):
        with self._connect() as connection:
            connection.execute("""
                INSERT INTO refresh_attempts VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    attempted_at=excluded.attempted_at, error=excluded.error
            """, (key, time.time(), error[:500] if error else None))

    def last_attempt(self, key: str):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT attempted_at, error FROM refresh_attempts WHERE cache_key = ?", (key,)
            ).fetchone()
        return dict(row) if row else {}

    def summary(self):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS entries, MAX(updated_at) AS last_saved FROM provider_payloads"
            ).fetchone()
        return dict(row)


class WarframeRefreshQueue:
    def __init__(self, store: WarframeCacheStore, *, workers=2, capacity=32, cooldown=60):
        self.store = store
        self.capacity = capacity
        self.cooldown = cooldown
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="warframe-refresh")
        self._lock = Lock()
        self._pending: set[str] = set()
        self._closed = False

    def pending(self, key: str) -> bool:
        with self._lock:
            return key in self._pending

    def request(self, key: str, loader: Callable, *, ttl=900, force=False) -> bool:
        entry = self.store.read(key)
        attempt = self.store.last_attempt(key)
        now = time.time()
        if not force and entry and now - entry["ts"] < ttl:
            return self.pending(key)
        if attempt.get("error") and now - attempt.get("attempted_at", 0) < self.cooldown:
            return self.pending(key)
        with self._lock:
            if self._closed or len(self._pending) >= self.capacity:
                return key in self._pending
            if key in self._pending:
                return True
            self._pending.add(key)
        try:
            self._pool.submit(self._run, key, loader)
        except RuntimeError:
            with self._lock:
                self._pending.discard(key)
            return False
        return True

    def _run(self, key, loader):
        try:
            loader()
            self.store.attempt(key, None)
        except Exception as exc:
            self.store.attempt(key, str(exc))
        finally:
            with self._lock:
                self._pending.discard(key)

    def summary(self):
        with self._lock:
            return {"pending": len(self._pending), "capacity": self.capacity}

    def close(self):
        with self._lock:
            self._closed = True
        self._pool.shutdown(wait=False, cancel_futures=True)
