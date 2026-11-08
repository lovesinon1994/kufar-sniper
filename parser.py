"""parser.py — асинхронный парсер Kufar (через внутренний JSON API) с фоновым циклом."""
from __future__ import annotations

import asyncio
import itertools
import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import aiohttp

from database import Database
from query_expander import QueryExpander

log = logging.getLogger("kufar_sniper")

_EPOCH = datetime.min.replace(tzinfo=timezone.utc)

API_URL = "https://api.kufar.by/search-api/v2/search/rendered-paginated"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]


@dataclass
class Listing:
    ad_id: str
    title: str
    price: float | None          # BYN
    url: str
    image_url: str | None
    created_at: datetime | None
    location: str | None


@dataclass
class SearchConfig:
    query: str = ""                       # поисковая фраза
    category: str | None = None
    region: int | None = None
    price_min: int | None = None
    price_max: int | None = None
    page_size: int = 30
    interval: float = 60.0
    jitter: float = 0.3
    request_timeout: float = 15.0
    max_retries: int = 4
    proxies: list[str] = field(default_factory=list)
    skip_initial: bool = True
    max_backoff: float = 900.0
    expand_queries: bool = True
    max_variants: int = 5
    query_delay: tuple[float, float] = (1.0, 3.0)


class KufarFetchError(Exception):
    pass


def _price_ok(price: float | None, lo: float | None, hi: float | None) -> bool:
    """Проверка цены (BYN) по фильтру. Лоты без цены не проходят, если фильтр задан."""
    if lo is None and hi is None:
        return True
    if price is None:
        return False
    return (lo is None or price >= lo) and (hi is None or price <= hi)


class KufarParser:
    def __init__(self, config: SearchConfig, db: Database) -> None:
        self.cfg = config
        self.db = db
        self._session: aiohttp.ClientSession | None = None
        self._proxy_cycle = itertools.cycle(config.proxies) if config.proxies else None
        self._stop = asyncio.Event()
        self.expander = QueryExpander(db, max_variants=config.max_variants)
        self._partial = False

    async def __aenter__(self) -> "KufarParser":
        timeout = aiohttp.ClientTimeout(total=self.cfg.request_timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)
        await self.expander.init()
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session:
            await self._session.close()

    def stop(self) -> None:
        self._stop.set()

    @property
    def last_pass_partial(self) -> bool:
        return self._partial

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Origin": "https://www.kufar.by",
            "Referer": "https://www.kufar.by/",
        }

    def _params(self, query: str, region: int | None = None, category: str | None = None) -> dict[str, Any]:
        p: dict[str, Any] = {"size": self.cfg.page_size, "sort": "lst.d", "lang": "ru"}
        if query:
            p["query"] = query
        if category or self.cfg.category:
            p["cat"] = category or self.cfg.category
        if region or self.cfg.region:
            p["rgn"] = region or self.cfg.region
        return p

    async def _fetch_json(
        self, query: str, region: int | None = None, category: str | None = None
    ) -> dict[str, Any]:
        assert self._session is not None, "Используйте 'async with KufarParser(...)'"
        last_error = "unknown"

        for attempt in range(1, self.cfg.max_retries + 1):
            proxy = next(self._proxy_cycle) if self._proxy_cycle else None
            try:
                async with self._session.get(
                    API_URL, params=self._params(query, region, category), headers=self._headers(), proxy=proxy
                ) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)

                    last_error = f"HTTP {resp.status}"
                    if resp.status in (403, 429):
                        retry_after = resp.headers.get("Retry-After")
                        delay = float(retry_after) if retry_after and retry_after.isdigit() else 30 * attempt
                        log.warning("Kufar вернул %s, пауза %.0f c", resp.status, delay)
                    elif resp.status >= 500:
                        delay = 2 ** attempt + random.random()
                    else:
                        raise KufarFetchError(f"Неожиданный статус {resp.status}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = f"{type(e).__name__}: {e}"
                delay = 2 ** attempt + random.random()
                log.warning("Сетевая ошибка (попытка %d/%d): %s", attempt, self.cfg.max_retries, last_error)

            if attempt < self.cfg.max_retries:
                await self._sleep(delay)

        raise KufarFetchError(f"Не удалось получить данные: {last_error}")

    @staticmethod
    def _parse_price(ad: dict[str, Any]) -> float | None:
        raw = ad.get("price_byn")
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return None
        return value / 100 if value > 0 else None

    @staticmethod
    def _parse_image(ad: dict[str, Any]) -> str | None:
        images = ad.get("images") or []
        if not images:
            return None
        img = images[0]
        path = img.get("path")
        if not path:
            return None
        if img.get("yams_storage"):
            return f"https://yams.kufar.by/api/v1/kufar-ads/images/{path}?rule=list_thumbs_2x"
        return f"https://rms.kufar.by/v1/list_thumbs_2x/{path}"

    @staticmethod
    def _parse_location(ad: dict[str, Any]) -> str | None:
        parts: list[str] = []
        for prm in ad.get("ad_parameters") or []:
            if prm.get("p") in ("region", "area") and prm.get("vl"):
                parts.append(str(prm["vl"]))
        if not parts:
            for prm in ad.get("account_parameters") or []:
                if prm.get("p") == "address" and prm.get("v"):
                    parts.append(str(prm["v"]))
        return ", ".join(parts) or None

    @staticmethod
    def _parse_time(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def _to_listing(self, ad: dict[str, Any]) -> Listing | None:
        ad_id = ad.get("ad_id")
        if ad_id is None:
            return None
        return Listing(
            ad_id=str(ad_id),
            title=(ad.get("subject") or "").strip(),
            price=self._parse_price(ad),
            url=ad.get("ad_link") or f"https://www.kufar.by/item/{ad_id}",
            image_url=self._parse_image(ad),
            created_at=self._parse_time(ad.get("list_time")),
            location=self._parse_location(ad),
        )

    async def _queries(self, query: str | None = None) -> list[str]:
        base = (self.cfg.query if query is None else query).strip()
        if not base or not self.cfg.expand_queries:
            return [base]
        return await self.expander.all_queries(base)

    async def check_once(
        self,
        query: str | None = None,
        *,
        price_min: float | None = None,
        price_max: float | None = None,
        region: int | None = None,
        category: str | None = None,
    ) -> list[Listing]:
        queries = await self._queries(query)
        collected: dict[str, Listing] = {}
        errors = 0

        for i, q in enumerate(queries):
            if i:
                await self._sleep(random.uniform(*self.cfg.query_delay))
            try:
                data = await self._fetch_json(q, region, category)
            except KufarFetchError as e:
                errors += 1
                log.warning("Запрос %r не удался: %s", q, e)
                continue
            for ad in data.get("ads", []):
                listing = self._to_listing(ad)
                if listing and listing.ad_id not in collected:
                    collected[listing.ad_id] = listing

        self._partial = errors > 0
        if errors == len(queries):
            raise KufarFetchError("Все запросы прохода завершились ошибкой")
        if price_min is not None or price_max is not None:
            collected = {k: l for k, l in collected.items() if _price_ok(l.price, price_min, price_max)}
        if not collected:
            return []

        new_ids = await self.db.filter_new(collected.keys())
        new = [l for l in collected.values() if l.ad_id in new_ids]
        new.sort(key=lambda l: l.created_at or _EPOCH)
        return new

    async def run(self, on_new: Callable[[list[Listing]], Awaitable[None]]) -> None:
        failures = 0
        first_pass = await self.db.count() == 0

        while not self._stop.is_set():
            try:
                new = await self.check_once()
                failures = 0

                if first_pass and self.cfg.skip_initial:
                    await self.db.mark_seen(l.ad_id for l in new)
                    log.info("Первый запуск: %d текущих лотов помечены как просмотренные", len(new))
                elif new:
                    log.info("Новых лотов: %d", len(new))
                    await on_new(new)
                    await self.db.mark_seen(l.ad_id for l in new)
                first_pass = first_pass and self._partial

                delay = self.cfg.interval * random.uniform(1 - self.cfg.jitter, 1 + self.cfg.jitter)

            except KufarFetchError as e:
                failures += 1
                delay = min(self.cfg.interval * 2 ** failures, self.cfg.max_backoff)
                log.error("Ошибка запроса (серия %d): %s. Следующая попытка через %.0f c", failures, e, delay)
            except Exception:
                failures += 1
                delay = min(self.cfg.interval * 2 ** failures, self.cfg.max_backoff)
                log.exception("Непредвиденная ошибка, продолжаем через %.0f c", delay)

            await self._sleep(delay)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
