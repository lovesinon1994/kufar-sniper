"""query_expander.py — ИИ-расширение поисковых запросов через API aiai.by."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any

import aiohttp

from database import Database

log = logging.getLogger("kufar_sniper.expander")

AIAI_URL = "https://api.aiai.by/v1/chat/completions"
# Точное название модели в шлюзе aiai.by
DEFAULT_MODEL = os.getenv("KUFAR_LLM_MODEL", "deepseek-v4-flash")
FALLBACK_COOLDOWN = 600  # сек: после сбоя API не долбим на каждом цикле

SYSTEM_PROMPT = (
    "Ты помогаешь искать товары на белорусской барахолке Kufar. "
    "Для названия товара верни JSON-массив из 4-6 строк: сокращения, русскую транслитерацию, "
    "популярный сленг, частые варианты написания и опечатки — так, как продавцы реально "
    "пишут в объявлениях. Только реально употребляемые варианты, без выдумок. "
    "Не включай исходное название. Ответ — ТОЛЬКО JSON-массив строк, без пояснений и без markdown."
)

BUILTIN_PHRASES: dict[str, list[str]] = {
    "nike air force 1": ["AF1", "air force 1", "форсы", "найк аир форс", "найки форсы", "эйр форс 1"],
    "air force 1": ["AF1", "nike air force", "форсы", "аир форс", "эйр форс 1"],
    "playstation 5": ["ps5", "пс5", "пс 5", "плейстейшн 5", "sony ps5"],
    "playstation 4": ["ps4", "пс4", "пс 4", "плейстейшн 4", "sony ps4"],
    "airpods": ["air pods", "аирподс", "эйрподс", "аирподы", "наушники apple"],
    "macbook": ["mac book", "макбук", "apple macbook", "мак бук"],
}

BUILTIN_WORDS: dict[str, str] = {
    "iphone": "айфон",
    "playstation": "плейстейшн",
    "macbook": "макбук",
    "airpods": "аирподс",
    "samsung": "самсунг",
    "xiaomi": "сяоми",
    "nike": "найк",
    "adidas": "адидас",
    "galaxy": "галакси",
    "jordan": "джордан",
}


class ExpanderError(Exception):
    pass


def _norm(text: str) -> str:
    return " ".join(text.lower().split())


def _clean(items: list, original_key: str, limit: int) -> list[str]:
    out: list[str] = []
    seen = {original_key}
    for item in items:
        if not isinstance(item, str):
            continue
        s = " ".join(item.strip().strip("\"'«»").split())
        key = _norm(s)
        if len(s) < 2 or len(s) > 60 or key in seen:
            continue
        seen.add(key)
        out.append(s)
        if len(out) >= limit:
            break
    return out


def _builtin(key: str) -> list[str]:
    if key in BUILTIN_PHRASES:
        return list(BUILTIN_PHRASES[key])

    variants: list[str] = []
    words = key.split()
    for i, w in enumerate(words):
        if w in BUILTIN_WORDS:
            variants.append(" ".join(words[:i] + [BUILTIN_WORDS[w]] + words[i + 1:]))
    if sum(w in BUILTIN_WORDS for w in words) > 1:
        variants.append(" ".join(BUILTIN_WORDS.get(w, w) for w in words))
    if len(words) > 1:
        variants.append("".join(words))
    return variants


class QueryExpander:
    def __init__(
        self,
        db: Database,
        api_key: str | None = None,
        model: str | None = None,
        max_variants: int = 6,
        timeout: float = 20.0,
    ) -> None:
        self.db = db
        self.api_key = api_key or os.getenv("AIAI_API_KEY")
        self.model = model or DEFAULT_MODEL
        if not self.api_key:
            log.warning("AIAI_API_KEY не задан — ИИ-синонимы отключены, работает офлайн-словарь")
        self.max_variants = max_variants
        self.timeout = timeout
        self._cooldown: dict[str, tuple[float, list[str]]] = {}

    async def init(self) -> None:
        await self.db.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS synonyms (
                query      TEXT PRIMARY KEY,
                variants   TEXT NOT NULL,
                source     TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        await self.db.conn.commit()

    async def _cache_get(self, key: str) -> list[str] | None:
        async with self.db.conn.execute(
            "SELECT variants FROM synonyms WHERE query = ?", (key,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        try:
            return list(json.loads(row[0]))
        except (ValueError, TypeError):
            return None

    async def _cache_set(self, key: str, variants: list[str], source: str) -> None:
        await self.db.conn.execute(
            "INSERT OR REPLACE INTO synonyms (query, variants, source, created_at) VALUES (?, ?, ?, ?)",
            (key, json.dumps(variants, ensure_ascii=False), source, int(time.time())),
        )
        await self.db.conn.commit()

    async def clear_cache(self, query: str | None = None) -> None:
        if query:
            await self.db.conn.execute("DELETE FROM synonyms WHERE query = ?", (_norm(query),))
        else:
            await self.db.conn.execute("DELETE FROM synonyms")
        await self.db.conn.commit()
        self._cooldown.clear()

    async def _call_llm(self, query: str) -> list:
        if not self.api_key:
            raise ExpanderError("API ключ aiai.by не задан")

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Товар: {query[:100]}"}
            ],
            "temperature": 0.3,
            "max_tokens": 300,
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(AIAI_URL, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:200]
                    raise ExpanderError(f"HTTP {resp.status}: {body}")
                data = await resp.json()

        try:
            text = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            raise ExpanderError(f"Некорректный формат ответа от API: {e}")

        match = re.search(r"\[.*\]", text, re.DOTALL)
        if not match:
            raise ExpanderError(f"В ответе нет JSON-массива: {text[:100]!r}")
        parsed = json.loads(match.group(0))
        if not isinstance(parsed, list):
            raise ExpanderError("Ответ не является массивом")
        return parsed

    async def expand(self, query: str) -> list[str]:
        key = _norm(query)
        if not key:
            return []

        cached = await self._cache_get(key)
        if cached is not None:
            return cached

        cooled = self._cooldown.get(key)
        if cooled and cooled[0] > time.monotonic():
            return cooled[1]

        if self.api_key:
            try:
                variants = _clean(await self._call_llm(query), key, self.max_variants)
                if variants:
                    await self._cache_set(key, variants, "llm")
                    log.info("aiai.by синонимы для %r: %s", query, variants)
                    return variants
            except (ExpanderError, aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                log.warning("aiai.by недоступен (%s), используем офлайн-запас", e)

        variants = _clean(_builtin(key), key, self.max_variants)
        self._cooldown[key] = (time.monotonic() + FALLBACK_COOLDOWN, variants)
        return variants

    async def all_queries(self, query: str) -> list[str]:
        return [query.strip(), *await self.expand(query)]
