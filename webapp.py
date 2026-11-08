"""webapp.py — HTTP-сервер Telegram Mini App (aiohttp.web) внутри процесса бота.

Отдаёт index.html и REST API:
  GET    /api/watches         список отслеживаемых запросов
  POST   /api/watches         добавить {name, price_min?, price_max?, region?, category?}
  DELETE /api/watches/{id}    удалить

Каждый запрос к /api/* подписан Telegram: заголовок  Authorization: tma <initData>.
Подпись проверяется (HMAC-SHA256 по токену бота), а пользователь должен совпадать
с TELEGRAM_CHAT_ID — чужие получат 401/403.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

from aiohttp import web

log = logging.getLogger("kufar_sniper.webapp")

INIT_DATA_MAX_AGE = 24 * 3600  # сек: насколько «свежей» должна быть подпись initData
MAX_PRICE = 10_000_000


# ───────────────────────── проверка подписи Telegram ─────────────────────────
def validate_init_data(
    init_data: str,
    bot_token: str,
    max_age: int = INIT_DATA_MAX_AGE,
    now: float | None = None,
) -> dict[str, Any] | None:
    """Проверяет Telegram.WebApp.initData. Возвращает объект user или None, если подпись неверна."""
    if not init_data or len(init_data) > 4096:
        return None
    try:
        pairs = parse_qsl(init_data, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        return None

    data = dict(pairs)
    received_hash = data.pop("hash", None)
    if not received_hash:
        return None

    check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    expected = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, received_hash):
        return None

    try:
        auth_date = int(data.get("auth_date", "0"))
        user = json.loads(data.get("user", "null"))
    except ValueError:
        return None
    if (time.time() if now is None else now) - auth_date > max_age:
        return None
    if not isinstance(user, dict) or "id" not in user:
        return None
    return user


# ───────────────────────── проверка тела POST ─────────────────────────
class PayloadError(ValueError):
    """Ошибка в данных от клиента; текст показывается пользователю."""


def _price(value: Any, label: str) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PayloadError(f"{label}: нужно число")
    v = float(value)
    if not math.isfinite(v) or v < 0 or v > MAX_PRICE:
        raise PayloadError(f"{label}: допустимы значения от 0 до {MAX_PRICE:,}".replace(",", " "))
    return round(v, 2)


def parse_watch_payload(data: Any, max_name_len: int = 100) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise PayloadError("Неверный формат запроса")

    name = data.get("name")
    if not isinstance(name, str):
        raise PayloadError("Введите название")
    name = " ".join(name.split())
    if not name:
        raise PayloadError("Введите название")
    if len(name) > max_name_len:
        raise PayloadError(f"Название длиннее {max_name_len} символов")

    price_min = _price(data.get("price_min"), "Мин. цена")
    price_max = _price(data.get("price_max"), "Макс. цена")
    if price_min is not None and price_max is not None and price_min > price_max:
        raise PayloadError("Мин. цена больше максимальной")

    region = data.get("region")
    if region is not None and (isinstance(region, bool) or not isinstance(region, int) or region <= 0):
        raise PayloadError("Неверный регион")

    category = data.get("category")
    if category is not None:
        if not isinstance(category, str) or len(category.strip()) > 64:
            raise PayloadError("Неверная категория")
        category = category.strip() or None

    return {
        "name": name, "price_min": price_min, "price_max": price_max,
        "region": region, "category": category,
    }


# ───────────────────────── приложение ─────────────────────────
def _json_error(status: int, message: str) -> web.Response:
    return web.json_response({"error": message}, status=status)


def _watch_json(w: Any) -> dict[str, Any]:
    return {
        "id": w.id, "name": w.display, "price_min": w.price_min, "price_max": w.price_max,
        "region": w.region, "category": w.category, "created_at": w.created_at, "seeded": w.seeded,
    }


def create_app(
    *,
    store: Any,
    monitor: Any,
    bot_token: str,
    admin_id: int,
    max_watches: int = 15,
    max_name_len: int = 100,
) -> web.Application:
    index_path = Path(__file__).with_name("index.html")

    @web.middleware
    async def guard(request: web.Request, handler):
        if request.path.startswith("/api/"):
            scheme, _, token = request.headers.get("Authorization", "").partition(" ")
            user = validate_init_data(token, bot_token) if scheme.lower() == "tma" else None
            if user is None:
                return _json_error(401, "Сессия недействительна. Закройте приложение и откройте заново.")
            if user.get("id") != admin_id:
                log.warning("Mini App: отказано пользователю id=%s", user.get("id"))
                return _json_error(403, "Доступ запрещён.")
        try:
            return await handler(request)
        except web.HTTPException:
            raise
        except Exception:
            log.exception("Ошибка в обработчике %s", request.path)
            return _json_error(500, "Внутренняя ошибка сервера.")

    async def index(request: web.Request) -> web.StreamResponse:
        if not index_path.is_file():
            return web.Response(status=500, text="index.html не найден рядом с webapp.py")
        return web.FileResponse(index_path, headers={"Cache-Control": "no-store"})

    async def health(request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def list_watches(request: web.Request) -> web.Response:
        watches = await store.all()
        return web.json_response({"watches": [_watch_json(w) for w in watches], "limit": max_watches})

    async def add_watch(request: web.Request) -> web.Response:
        try:
            payload = parse_watch_payload(await request.json(), max_name_len)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return _json_error(400, "Неверный формат запроса")
        except PayloadError as e:
            return _json_error(400, str(e))

        if len(await store.all()) >= max_watches:
            return _json_error(400, f"Достигнут лимит в {max_watches} запросов. Удалите ненужные.")
        watch_id = await store.add(
            payload["name"], payload["price_min"], payload["price_max"], payload["region"], payload["category"]
        )
        if watch_id is None:
            return _json_error(409, "Такой запрос уже отслеживается.")

        monitor.seed_soon()  # запомнить текущие объявления, чтобы не слать старые
        created = next((w for w in await store.all() if w.id == watch_id), None)
        return web.json_response(_watch_json(created), status=201)

    async def delete_watch(request: web.Request) -> web.Response:
        try:
            watch_id = int(request.match_info["id"])
        except ValueError:
            return _json_error(400, "Неверный id")
        if not await store.remove_by_id(watch_id):
            return _json_error(404, "Запрос не найден (возможно, уже удалён).")
        return web.json_response({"ok": True})

    app = web.Application(middlewares=[guard], client_max_size=16 * 1024)
    app.router.add_get("/", index)
    app.router.add_get("/healthz", health)
    app.router.add_get("/api/watches", list_watches)
    app.router.add_post("/api/watches", add_watch)
    app.router.add_delete("/api/watches/{id}", delete_watch)
    return app
