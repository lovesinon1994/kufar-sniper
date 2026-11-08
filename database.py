"""database.py — хранилище просмотренных объявлений (SQLite + aiosqlite)."""
from __future__ import annotations

import time
from typing import Iterable

import aiosqlite

_CHUNK = 500  # безопасно ниже лимита переменных SQLite


class Database:
    def __init__(self, path: str = "kufar_sniper.db") -> None:
        self.path = path
        self._conn: aiosqlite.Connection | None = None

    # --- жизненный цикл -------------------------------------------------
    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_ads (
                ad_id         TEXT PRIMARY KEY,
                first_seen_at INTEGER NOT NULL
            )
            """
        )
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("База не подключена: вызовите connect() или используйте async with")
        return self._conn

    # --- операции -------------------------------------------------------
    async def filter_new(self, ids: Iterable[str]) -> set[str]:
        """Возвращает подмножество ids, которых ещё нет в базе."""
        ids = list(dict.fromkeys(str(i) for i in ids))  # уникальные, порядок сохранён
        seen: set[str] = set()
        for i in range(0, len(ids), _CHUNK):
            chunk = ids[i : i + _CHUNK]
            placeholders = ",".join("?" * len(chunk))
            async with self.conn.execute(
                f"SELECT ad_id FROM seen_ads WHERE ad_id IN ({placeholders})", chunk
            ) as cur:
                async for row in cur:
                    seen.add(row[0])
        return set(ids) - seen

    async def mark_seen(self, ids: Iterable[str]) -> None:
        now = int(time.time())
        rows = [(str(i), now) for i in ids]
        if not rows:
            return
        await self.conn.executemany(
            "INSERT OR IGNORE INTO seen_ads (ad_id, first_seen_at) VALUES (?, ?)", rows
        )
        await self.conn.commit()

    async def count(self) -> int:
        async with self.conn.execute("SELECT COUNT(*) FROM seen_ads") as cur:
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def cleanup(self, older_than_days: int = 60) -> int:
        """Удаляет старые записи, чтобы база не росла бесконечно."""
        threshold = int(time.time()) - older_than_days * 86400
        cur = await self.conn.execute(
            "DELETE FROM seen_ads WHERE first_seen_at < ?", (threshold,)
        )
        await self.conn.commit()
        return cur.rowcount
