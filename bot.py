"""bot.py — Telegram-бот Kufar Sniper (aiogram 3.x).

Переменные окружения:
  TELEGRAM_BOT_TOKEN      токен от @BotFather                          (обязательно)
  TELEGRAM_CHAT_ID        id вашего чата: бот отвечает и шлёт карточки только туда (обязательно)
  AIAI_API_KEY            ключ api.aiai.by для ИИ-синонимов (без него работает офлайн-словарь)
  CHECK_INTERVAL_MINUTES  как часто проверять Kufar, по умолчанию 5
  DB_PATH                 путь к SQLite, по умолчанию kufar_sniper.db
  WEBAPP_URL              публичный HTTPS-адрес Mini App (например, из cloudflared); без него кнопки нет
  WEBAPP_HOST / WEBAPP_PORT  где слушает встроенный веб-сервер (по умолчанию 127.0.0.1:8080)

Секреты храните в файле .env рядом с bot.py (см. .env.example), а не в коде.

Запуск:  python3 bot.py
"""
from __future__ import annotations

import asyncio
import html
import logging
import os
import random
import sys
from dataclasses import dataclass
from datetime import timedelta, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    Message,
    WebAppInfo,
)
from aiohttp import web

from database import Database
from parser import KufarFetchError, KufarParser, Listing, SearchConfig
from webapp import create_app

try:  # необязательно: подхватить переменные из файла .env, если стоит python-dotenv
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

log = logging.getLogger("kufar_sniper.bot")

MINSK_TZ = timezone(timedelta(hours=3), "MSK")  # Беларусь: UTC+3 круглый год
MAX_WATCHES = 15          # лимит отслеживаемых запросов (каждый = до 6 запросов к Kufar за проход)
MAX_QUERY_LEN = 100
SEND_DELAY = 0.6          # пауза между карточками, чтобы не упереться в лимиты Telegram
BETWEEN_WATCHES = (2.0, 5.0)
MAX_PENALTY = 1800        # потолок доп. паузы (сек), если Kufar не отвечает на все запросы


def _norm(text: str) -> str:
    return " ".join(text.lower().split())


# ───────────────────────── хранилище запросов ─────────────────────────
@dataclass
class Watch:
    id: int         # rowid в таблице watches (используется в API Mini App)
    key: str        # нормализованный запрос (уникален)
    display: str    # как ввёл пользователь — по нему ищем
    seeded: bool    # текущие объявления уже «запомнены», дальше шлём только новые
    created_at: int = 0                 # unix-время добавления
    price_min: float | None = None      # BYN
    price_max: float | None = None      # BYN
    region: int | None = None           # id региона Kufar (на перспективу)
    category: str | None = None         # id категории Kufar (на перспективу)


def price_label(w: Watch) -> str:
    def fmt(v: float) -> str:
        return f"{v:g}"

    if w.price_min is not None and w.price_max is not None:
        return f"{fmt(w.price_min)}–{fmt(w.price_max)} BYN"
    if w.price_min is not None:
        return f"от {fmt(w.price_min)} BYN"
    if w.price_max is not None:
        return f"до {fmt(w.price_max)} BYN"
    return ""


class WatchStore:
    """Список отслеживаемых запросов в той же SQLite-базе (таблица watches)."""

    def __init__(self, db: Database) -> None:
        self.db = db

    async def init(self) -> None:
        await self.db.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watches (
                query      TEXT PRIMARY KEY,
                display    TEXT NOT NULL,
                seeded     INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            )
            """
        )
        # миграция: добавляем колонки фильтров в базу, созданную прошлой версией
        async with self.db.conn.execute("PRAGMA table_info(watches)") as cur:
            existing = {row[1] for row in await cur.fetchall()}
        for name, ddl in (
            ("price_min", "REAL"), ("price_max", "REAL"), ("region", "INTEGER"), ("category", "TEXT"),
        ):
            if name not in existing:
                await self.db.conn.execute(f"ALTER TABLE watches ADD COLUMN {name} {ddl}")
        await self.db.conn.commit()

    async def add(
        self,
        text: str,
        price_min: float | None = None,
        price_max: float | None = None,
        region: int | None = None,
        category: str | None = None,
    ) -> int | None:
        """Возвращает id нового запроса или None, если такой уже есть."""
        cur = await self.db.conn.execute(
            "INSERT OR IGNORE INTO watches (query, display, price_min, price_max, region, category) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (_norm(text), " ".join(text.split()), price_min, price_max, region, category),
        )
        await self.db.conn.commit()
        return cur.lastrowid if cur.rowcount > 0 else None

    async def remove(self, text: str) -> bool:
        cur = await self.db.conn.execute("DELETE FROM watches WHERE query = ?", (_norm(text),))
        await self.db.conn.commit()
        return cur.rowcount > 0

    async def remove_by_id(self, watch_id: int) -> bool:
        cur = await self.db.conn.execute("DELETE FROM watches WHERE rowid = ?", (watch_id,))
        await self.db.conn.commit()
        return cur.rowcount > 0

    async def all(self) -> list[Watch]:
        async with self.db.conn.execute(
            "SELECT rowid, query, display, seeded, created_at, price_min, price_max, region, category "
            "FROM watches ORDER BY created_at, rowid"
        ) as cur:
            rows = await cur.fetchall()
        return [Watch(r[0], r[1], r[2], bool(r[3]), r[4], r[5], r[6], r[7], r[8]) for r in rows]

    async def mark_seeded(self, key: str) -> None:
        await self.db.conn.execute("UPDATE watches SET seeded = 1 WHERE query = ?", (key,))
        await self.db.conn.commit()


# ───────────────────────── карточка объявления ─────────────────────────
def format_listing(l: Listing) -> str:
    title = html.escape((l.title or "Без названия")[:200])
    price = f"{l.price:.2f} BYN" if l.price else "не указана"
    location = html.escape(l.location or "не указана")
    when = l.created_at.astimezone(MINSK_TZ).strftime("%Y-%m-%d %H:%M") if l.created_at else "—"
    return (
        f"🆕 <b>{title}</b>\n"
        f"💰 Цена: {price}\n"
        f"📍 Локация: {location}\n"
        f"📅 Дата: {when}\n\n"
        f"🔗 {html.escape(l.url)}"
    )


# ───────────────────────── фоновый мониторинг ─────────────────────────
class Monitor:
    def __init__(
        self, bot: Bot, chat_id: int, parser: KufarParser, db: Database, store: WatchStore,
        interval_minutes: float,
    ) -> None:
        self.bot = bot
        self.chat_id = chat_id
        self.parser = parser
        self.db = db
        self.store = store
        self.interval = interval_minutes * 60
        self.lock = asyncio.Lock()  # один проход за раз (и /run, и фон, и «засев» после /add)
        self._penalty = 0.0
        self._tasks: set[asyncio.Task] = set()

    def seed_soon(self) -> None:
        """Сразу «засеять» только что добавленные запросы, не дожидаясь планового прохода."""
        task = asyncio.create_task(self.run_cycle(only_unseeded=True))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # --- отправка -------------------------------------------------------
    async def _deliver(self, l: Listing, text: str) -> None:
        if l.image_url:
            try:
                await self.bot.send_photo(self.chat_id, l.image_url, caption=text)
                return
            except TelegramBadRequest as e:  # битая/недоступная картинка — шлём текстом
                log.warning("Фото %s не отправилось (%s), шлю без него", l.ad_id, e)
        await self.bot.send_message(self.chat_id, text)

    async def send_listing(self, l: Listing) -> bool:
        """True — лот обработан (отправлен или безнадёжно отклонён), можно помечать просмотренным.
        False — временный сбой Telegram, попробуем в следующий проход."""
        text = format_listing(l)
        for _ in range(3):
            try:
                await self._deliver(l, text)
                return True
            except TelegramRetryAfter as e:
                log.warning("Telegram просит подождать %s c", e.retry_after)
                await asyncio.sleep(e.retry_after + 1)
            except TelegramBadRequest as e:
                log.error("Telegram отклонил лот %s: %s — пропускаю", l.ad_id, e)
                return True
            except TelegramAPIError as e:
                log.error("Не удалось отправить лот %s: %s", l.ad_id, e)
                return False
        return False

    async def say(self, text: str) -> None:
        try:
            await self.bot.send_message(self.chat_id, text)
        except TelegramAPIError as e:
            log.warning("Не удалось отправить сообщение: %s", e)

    # --- один запрос ----------------------------------------------------
    async def process_watch(self, w: Watch) -> int:
        """Возвращает число отправленных карточек. Для нового запроса — «засев» без отправки."""
        new = await self.parser.check_once(
            w.display,
            price_min=w.price_min,
            price_max=w.price_max,
            region=w.region,
            category=w.category,
        )

        if not w.seeded:
            await self.db.mark_seen(l.ad_id for l in new)
            if self.parser.last_pass_partial:
                log.warning("«%s»: засев неполный, повторю в следующий проход", w.display)
                return 0
            await self.store.mark_seeded(w.key)
            log.info("«%s»: запомнено %d текущих объявлений", w.display, len(new))
            await self.say(
                f"✅ «{html.escape(w.display)}»: запомнил текущие объявления ({len(new)} шт.). "
                f"Дальше присылаю только новые."
            )
            return 0

        sent = 0
        for l in new:
            if not await self.send_listing(l):
                break  # Telegram недоступен — остальное придёт в следующий проход
            await self.db.mark_seen([l.ad_id])
            sent += 1
            await asyncio.sleep(SEND_DELAY)
        if sent:
            log.info("«%s»: отправлено %d новых", w.display, sent)
        return sent

    # --- проход по всем запросам ---------------------------------------
    async def run_cycle(self, only_unseeded: bool = False) -> tuple[int, int, int]:
        """Возвращает (отправлено карточек, запросов с ошибкой, всего запросов)."""
        sent = failed = total = 0
        async with self.lock:
            watches = [w for w in await self.store.all() if not (only_unseeded and w.seeded)]
            total = len(watches)
            for i, w in enumerate(watches):
                if i:
                    await asyncio.sleep(random.uniform(*BETWEEN_WATCHES))
                try:
                    sent += await self.process_watch(w)
                except KufarFetchError as e:
                    failed += 1
                    log.warning("«%s»: Kufar недоступен: %s", w.display, e)
                except Exception:
                    failed += 1
                    log.exception("«%s»: непредвиденная ошибка", w.display)
        return sent, failed, total

    async def loop(self) -> None:
        """Фоновая проверка раз в N минут. Вызывать как asyncio-задачу."""
        while True:
            delay = self.interval
            try:
                _, failed, total = await self.run_cycle()
                if total and failed == total:  # похоже на бан/обрыв — увеличиваем паузу
                    self._penalty = min((self._penalty or self.interval) * 2, MAX_PENALTY)
                    delay += self._penalty
                    log.warning("Все запросы упали, следующая проверка через %.0f c", delay)
                else:
                    self._penalty = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Сбой в фоновом цикле")
            await asyncio.sleep(delay * random.uniform(0.85, 1.15))


# ───────────────────────── команды ─────────────────────────
def build_router(
    monitor: Monitor,
    store: WatchStore,
    admin_id: int,
    interval_minutes: float,
    webapp_url: str | None = None,
) -> Router:
    router = Router()
    router.message.filter(F.chat.id == admin_id)  # всё остальное бот игнорирует

    def app_keyboard() -> InlineKeyboardMarkup | None:
        if not webapp_url:
            return None
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📱 Открыть панель", web_app=WebAppInfo(url=webapp_url))
        ]])

    @router.message(Command("start"))
    async def cmd_start(m: Message) -> None:
        await m.answer(
            "👋 Я Kufar Sniper — слежу за новыми объявлениями на Kufar и присылаю их сюда.\n\n"
            "/app — панель управления с фильтрами цены (Mini App)\n"
            "/add iphone 13 — начать следить за запросом\n"
            "/list — что сейчас отслеживается\n"
            "/delete iphone 13 — перестать следить (можно номером из /list)\n"
            "/run — проверить прямо сейчас\n\n"
            "Каждый запрос я сам расширяю синонимами и сленгом (например, «айфон 13»), "
            f"поэтому ищи обычным названием. Проверка идёт автоматически раз в {interval_minutes:g} мин.",
            reply_markup=app_keyboard(),
        )

    @router.message(Command("app"))
    async def cmd_app(m: Message) -> None:
        kb = app_keyboard()
        if kb is None:
            await m.answer("Mini App пока не настроен: задайте HTTPS-адрес в переменной WEBAPP_URL и перезапустите бота.")
            return
        await m.answer("Панель управления запросами и фильтрами цены:", reply_markup=kb)

    @router.message(Command("add"))
    async def cmd_add(m: Message, command: CommandObject) -> None:
        text = " ".join((command.args or "").split())
        if not text:
            await m.answer("Напиши, что искать, например: /add iphone 13")
            return
        if len(text) > MAX_QUERY_LEN:
            await m.answer(f"Слишком длинный запрос (максимум {MAX_QUERY_LEN} символов).")
            return
        if len(await store.all()) >= MAX_WATCHES:
            await m.answer(f"Достигнут лимит в {MAX_WATCHES} запросов. Удали ненужные через /delete.")
            return
        if not await store.add(text):
            await m.answer(f"«{html.escape(text)}» уже отслеживается.")
            return
        await m.answer(
            f"➕ Добавил «{html.escape(text)}». Сейчас запомню текущие объявления, "
            f"чтобы не завалить тебя старыми — напишу, когда будет готово."
        )
        monitor.seed_soon()  # «засев» сразу, не дожидаясь планового прохода

    @router.message(Command("list"))
    async def cmd_list(m: Message) -> None:
        watches = await store.all()
        if not watches:
            await m.answer("Список пуст. Добавь запрос: /add iphone 13")
            return
        lines = [
            f"{i}. {html.escape(w.display)}"
            + (f" · {price_label(w)}" if price_label(w) else "")
            + ("" if w.seeded else "  ⏳ запоминаю текущие")
            for i, w in enumerate(watches, 1)
        ]
        await m.answer(
            f"📋 Отслеживаю ({len(watches)}):\n" + "\n".join(lines)
            + f"\n\nПроверка каждые {interval_minutes:g} мин."
        )

    @router.message(Command("delete"))
    async def cmd_delete(m: Message, command: CommandObject) -> None:
        arg = " ".join((command.args or "").split())
        if not arg:
            await m.answer("Что удалить? Например: /delete iphone 13 или /delete 2")
            return
        target = arg
        if arg.isdigit():  # номер из /list
            watches = await store.all()
            idx = int(arg)
            if not 1 <= idx <= len(watches):
                await m.answer("Нет запроса с таким номером. Смотри /list.")
                return
            target = watches[idx - 1].display
        if await store.remove(target):
            await m.answer(f"🗑 Удалил «{html.escape(target)}».")
        else:
            await m.answer(f"«{html.escape(target)}» в списке нет. Смотри /list.")

    @router.message(Command("run"))
    async def cmd_run(m: Message) -> None:
        if not await store.all():
            await m.answer("Список пуст. Добавь запрос: /add iphone 13")
            return
        if monitor.lock.locked():
            await m.answer("⏳ Проверка уже идёт, подожди немного.")
            return
        await m.answer("🔎 Проверяю…")
        sent, failed, total = await monitor.run_cycle()
        text = f"✅ Готово. Новых объявлений: {sent}."
        if failed:
            text += f" Не удалось проверить запросов: {failed} из {total} (подробности в логе)."
        await m.answer(text)

    return router


def build_fallback_router(admin_id: int) -> Router:
    router = Router()

    @router.message()
    async def other(m: Message) -> None:
        if m.chat.id == admin_id:
            await m.answer("Не понял. Список команд: /start")
        else:  # молча игнорируем, но пишем в лог — так проще узнать свой chat id при настройке
            log.warning(
                "Игнорирую сообщение из чужого чата id=%s. Если это ваш чат, "
                "задайте TELEGRAM_CHAT_ID=%s", m.chat.id, m.chat.id,
            )

    return router


# ───────────────────────── запуск ─────────────────────────
def _require_env() -> tuple[str, int]:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat:
        sys.exit("Задайте переменные окружения TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID (см. шапку bot.py).")
    try:
        return token, int(chat)
    except ValueError:
        sys.exit("TELEGRAM_CHAT_ID должен быть числом, например 123456789.")


def _webapp_url() -> str | None:
    url = os.getenv("WEBAPP_URL", "").strip()
    if not url:
        return None
    if not url.startswith("https://"):
        log.warning("WEBAPP_URL должен начинаться с https:// (требование Telegram) — кнопка Mini App отключена")
        return None
    return url


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    token, admin_id = _require_env()
    interval_minutes = float(os.getenv("CHECK_INTERVAL_MINUTES", "5"))
    db_path = os.getenv("DB_PATH", "kufar_sniper.db")

    bot = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    cfg = SearchConfig(query="")  # конкретные запросы подставляет бот в check_once(query)

    async with Database(db_path) as db, KufarParser(cfg, db) as parser:
        store = WatchStore(db)
        await store.init()
        monitor = Monitor(bot, admin_id, parser, db, store, interval_minutes)

        webapp_url = _webapp_url()
        dp = Dispatcher()
        dp.include_router(build_router(monitor, store, admin_id, interval_minutes, webapp_url))
        dp.include_router(build_fallback_router(admin_id))

        # встроенный веб-сервер Mini App (тот же event loop, что и бот)
        app = create_app(
            store=store, monitor=monitor, bot_token=token, admin_id=admin_id,
            max_watches=MAX_WATCHES, max_name_len=MAX_QUERY_LEN,
        )
        runner = web.AppRunner(app)
        await runner.setup()
        host = os.getenv("WEBAPP_HOST", "127.0.0.1")
        port = int(os.getenv("WEBAPP_PORT", "8080"))
        await web.TCPSite(runner, host, port).start()
        log.info("Mini App слушает http://%s:%s", host, port)

        await bot.delete_webhook(drop_pending_updates=True)
        if webapp_url:  # кнопка внизу экрана чата, открывает Mini App
            try:
                await bot.set_chat_menu_button(
                    chat_id=admin_id,
                    menu_button=MenuButtonWebApp(text="Запросы", web_app=WebAppInfo(url=webapp_url)),
                )
                log.info("Кнопка меню настроена: %s", webapp_url)
            except TelegramAPIError as e:
                log.warning("Не удалось поставить кнопку меню: %s", e)
        else:
            log.info("WEBAPP_URL не задан — кнопка меню не устанавливается (бот работает как раньше)")
        watches = await store.all()
        await monitor.say(
            f"🟢 Kufar Sniper запущен. Отслеживаю запросов: {len(watches)}. "
            f"Проверка каждые {interval_minutes:g} мин. Команды: /start"
        )

        monitor_task = asyncio.create_task(monitor.loop())
        try:
            await dp.start_polling(bot)
        finally:
            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)
            await runner.cleanup()
            await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
