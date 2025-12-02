#!/usr/bin/env python3
"""
Рефакторинг Telegram-бота с асинхронной работой с БД:
1. Асинхронный пул подключений к PostgreSQL
2. Оптимизированные запросы к БД
3. Улучшенная обработка ошибок
4. Кэширование данных
5. Асинхронные операции
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import logging.handlers
import os
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Dict, Optional, List, Set
from functools import lru_cache

import aiohttp
import psycopg
from psycopg import AsyncConnection
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

# --------- zoneinfo / backports.zoneinfo ---------
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python 3.8 и ниже

# -------------------- Настройки и логирование --------------------

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

# Старый URL /today оставляем для совместимости, но дальше работаем через BASE_URL
MATCHES_API_URL = os.getenv(
    "MATCHES_API_URL",
    "http://45.10.245.84:8050/dota/matches/today",
)

MATCHES_API_BASE_URL = os.getenv("MATCHES_API_BASE_URL")
if not MATCHES_API_BASE_URL:
    # Если в .env только /today — отрежем "today" и возьмём базу
    if MATCHES_API_URL.endswith("/today"):
        MATCHES_API_BASE_URL = MATCHES_API_URL.rsplit("/", 1)[0]
    else:
        MATCHES_API_BASE_URL = MATCHES_API_URL

MSK_TZ = ZoneInfo("Europe/Moscow")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# DB config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Логи
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

logger = logging.getLogger("dota_matches_bot")
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE,
    maxBytes=5_000_000,
    backupCount=3,
    encoding="utf-8",
)
file_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

logger.info("Логирование инициализировано")
logger.info("MATCHES_API_BASE_URL = %s", MATCHES_API_BASE_URL)


# -------------------- Модели --------------------

@dataclass
class Match:
    match_time_msk: datetime
    time_msk: str
    team1: str
    team2: str
    bo: int
    tournament: str
    status: str
    score: Optional[str]


@dataclass
class TodayMessageState:
    chat_id: int
    day: date
    message_id: int
    excluded_tournaments: Set[str]
    last_text: Optional[str]


poll_task: Optional[asyncio.Task] = None
daily_task: Optional[asyncio.Task] = None
last_daily_notify_date: Optional[date] = None


# -------------------- Асинхронная работа с БД --------------------

class AsyncDatabasePool:
    """Асинхронный пул подключений к PostgreSQL для Telegram-бота"""
    
    def __init__(self):
        self.conn_str = (
            f"host={DB_HOST} "
            f"port={DB_PORT} "
            f"dbname={DB_NAME} "
            f"user={DB_USER} "
            f"password={DB_PASSWORD}"
        )
        self._pool = None
        self._initialized = False
    
    async def init_pool(self):
        """Инициализация пула подключений"""
        if not self._initialized:
            self._pool = await AsyncConnection.connect(
                self.conn_str,
                autocommit=True
            )
            self._initialized = True
            logger.info("Асинхронный пул подключений к БД инициализирован")
    
    async def close_pool(self):
        """Закрытие пула подключений"""
        if self._pool:
            await self._pool.close()
            logger.info("Асинхронный пул подключений к БД закрыт")
    
    async def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Выполнение SELECT запроса с возвращением результатов"""
        if not self._initialized:
            await self.init_pool()
        
        async with self._pool.cursor() as cur:
            await cur.execute(query, params)
            return await cur.fetchall()
    
    async def execute_command(self, query: str, params: tuple = None) -> None:
        """Выполнение INSERT/UPDATE/DELETE команд"""
        if not self._initialized:
            await self.init_pool()
        
        async with self._pool.cursor() as cur:
            await cur.execute(query, params)

# Глобальный экземпляр асинхронного пула
async_db_pool = AsyncDatabasePool()

# -------------------- Асинхронные операции с БД ----------

async def init_db_async():
    """Асинхронная инициализация БД"""
    logger.info("Асинхронная инициализация БД...")
    
    queries = [
        """
        CREATE TABLE IF NOT EXISTS dota_bot_subscribers (
            chat_id BIGINT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dota_bot_today_messages (
            chat_id BIGINT NOT NULL,
            day DATE NOT NULL,
            message_id BIGINT NOT NULL,
            excluded_tournaments TEXT NOT NULL DEFAULT '',
            last_text TEXT,
            PRIMARY KEY (chat_id, day)
        );
        """
    ]
    
    for query in queries:
        await async_db_pool.execute_command(query)
    
    logger.info("Асинхронная инициализация БД завершена")


async def add_subscriber_async(chat_id: int) -> None:
    """Асинхронное добавление подписчика"""
    await async_db_pool.execute_command(
        """
        INSERT INTO dota_bot_subscribers (chat_id)
        VALUES (%s)
        ON CONFLICT (chat_id) DO NOTHING;
        """,
        (chat_id,)
    )
    logger.info("Чат %s добавлен в подписчики (или уже был).", chat_id)


async def get_all_subscribers_async() -> List[int]:
    """Асинхронное получение всех подписчиков"""
    rows = await async_db_pool.execute_query("SELECT chat_id FROM dota_bot_subscribers;")
    subs = [r[0] for r in rows]
    logger.info("Из БД получено подписчиков: %s", len(subs))
    return subs


async def upsert_today_state_async(state: TodayMessageState) -> None:
    """Асинхронное сохранение состояния today-сообщения"""
    await async_db_pool.execute_command(
        """
        INSERT INTO dota_bot_today_messages
            (chat_id, day, message_id, excluded_tournaments, last_text)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (chat_id, day) DO UPDATE
        SET message_id = EXCLUDED.message_id,
            excluded_tournaments = EXCLUDED.excluded_tournaments,
            last_text = EXCLUDED.last_text;
        """,
        (
            state.chat_id,
            state.day,
            state.message_id,
            json.dumps(sorted(state.excluded_tournaments), ensure_ascii=False),
            state.last_text,
        )
    )
    logger.info(
        "Состояние today-сообщения сохранено: chat_id=%s, day=%s, message_id=%s",
        state.chat_id,
        state.day,
        state.message_id,
    )


async def get_today_state_async(chat_id: int, day: date) -> Optional[TodayMessageState]:
    """Асинхронное получение состояния today-сообщения"""
    rows = await async_db_pool.execute_query(
        """
        SELECT message_id, excluded_tournaments, last_text
        FROM dota_bot_today_messages
        WHERE chat_id = %s AND day = %s;
        """,
        (chat_id, day)
    )
    
    if not rows:
        return None
    
    message_id, excluded_raw, last_text = rows[0]
    excluded = set(json.loads(excluded_raw)) if excluded_raw else set()
    
    return TodayMessageState(
        chat_id=chat_id,
        day=day,
        message_id=message_id,
        excluded_tournaments=excluded,
        last_text=last_text,
    )


async def get_all_today_states_for_day_async(day: date) -> List[TodayMessageState]:
    """Асинхронное получение всех today-сообщений за день"""
    rows = await async_db_pool.execute_query(
        """
        SELECT chat_id, message_id, excluded_tournaments, last_text
        FROM dota_bot_today_messages
        WHERE day = %s;
        """,
        (day,)
    )
    
    result = []
    for chat_id, message_id, excluded_raw, last_text in rows:
        excluded = set(json.loads(excluded_raw)) if excluded_raw else set()
        result.append(TodayMessageState(
            chat_id=chat_id,
            day=day,
            message_id=message_id,
            excluded_tournaments=excluded,
            last_text=last_text,
        ))
    
    logger.info("Для дня %s найдено today-сообщений: %s", day, len(result))
    return result


# -------------------- Кэширование и оптимизации ----------

@lru_cache(maxsize=128)
def _get_timezone_msk() -> ZoneInfo:
    """Кэширование часового пояса МСК"""
    return ZoneInfo("Europe/Moscow")


# -------------------- Работа с API (без изменений) ----------

def build_matches_url_for_day(day: date) -> str:
    return f"{MATCHES_API_BASE_URL}/{day.strftime('%d-%m-%Y')}"


async def fetch_matches_for_day(day: date) -> List[Match]:
    """Асинхронное получение матчей из API (без изменений)"""
    url = build_matches_url_for_day(day)
    logger.info("Запрос матчей из API: %s для дня %s", url, day.isoformat())

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except Exception as e:
            logger.error("Ошибка API для дня %s: %s", day.isoformat(), e, exc_info=True)
            return []

    try:
        api_date_str = data.get("date")
        matches_raw = data.get("matches", [])

        if api_date_str:
            try:
                api_date = date.fromisoformat(api_date_str)
                if api_date != day:
                    logger.warning(
                        "Дата в API (%s) не совпадает с запрошенной (%s).",
                        api_date,
                        day,
                    )
            except Exception:
                logger.warning(
                    "Не удалось распарсить date='%s' из API для дня %s",
                    api_date_str,
                    day,
                )

        def fix_encoding(s):
            if s is None:
                return None
            try:
                return s.encode("latin1").decode("utf-8")
            except Exception:
                return s

        result: List[Match] = []

        for raw in matches_raw:
            match_time_iso = raw.get("match_time_msk")
            if not match_time_iso:
                continue

            try:
                match_dt = datetime.fromisoformat(match_time_iso)
            except ValueError:
                match_dt = datetime.fromisoformat(
                    match_time_iso.replace("Z", "+00:00")
                )

            result.append(
                Match(
                    match_time_msk=match_dt,
                    time_msk=raw.get("time_msk", ""),
                    team1=fix_encoding(raw.get("team1", "")) or "",
                    team2=fix_encoding(raw.get("team2", "")) or "",
                    bo=int(raw.get("bo", 0) or 0),
                    tournament=fix_encoding(raw.get("tournament", "")) or "",
                    status=raw.get("status", ""),
                    score=raw.get("score"),
                )
            )

        logger.info("Успешно распарсили %s матчей для дня %s", len(result), day)
        return result

    except Exception as e:
        logger.error("Ошибка парсинга API для дня %s: %s", day, e, exc_info=True)
        return []


# -------------------- Форматирование (без изменений) ----------

def format_match(match: Match) -> str:
    """Форматирование одного матча (без изменений)"""
    status = (match.status or "").lower()

    if status == "upcoming":
        status_emoji = "⏰"
        status_text = "Скоро начнётся"
    elif status == "live":
        status_emoji = "🟢"
        status_text = "Идёт сейчас"
    elif status == "finished":
        status_emoji = "✅"
        status_text = "Матч окончен"
    else:
        status_emoji = "❓"
        status_text = match.status or "Неизвестно"

    time_line = (
        match.time_msk
        or match.match_time_msk.astimezone(MSK_TZ).strftime("%H:%M")
    )

    score_line = f" | 🔢 {match.score}" if match.score else ""

    text = (
        f"{match.team1} vs {match.team2}\n"
        f"🕒 {time_line} | Bo{match.bo}{score_line}"
    )
    return text


def format_matches_grouped(matches: List[Match], day: date) -> str:
    """Группировка матчей в одно компактное сообщение"""
    header = f"📅 <b>Матчи на {day.strftime('%d.%m.%Y')} (МСК)</b>\n"

    if not matches:
        return header + "\nНа этот день матчей не найдено 🤷‍♂️"

    live = []
    upcoming = []
    finished = []
    other = []

    for m in matches:
        s = (m.status or "").lower()
        if s == "live":
            live.append(m)
        elif s == "upcoming":
            upcoming.append(m)
        elif s == "finished":
            finished.append(m)
        else:
            other.append(m)

    key_fn = lambda mm: mm.match_time_msk
    live.sort(key=key_fn)
    upcoming.sort(key=key_fn)
    finished.sort(key=key_fn)
    other.sort(key=key_fn)

    parts: List[str] = []

    if live:
        parts.append("🟢 <b>LIVE</b>\n" + "\n".join(format_match_compact(m) for m in live))

    if upcoming:
        parts.append("⏰ <b>Скоро начнутся</b>\n" + "\n".join(format_match_compact(m) for m in upcoming))

    if finished:
        parts.append("✅ <b>Завершённые</b>\n" + "\n".join(format_match_compact(m) for m in finished))

    if other:
        parts.append("❓ <b>Прочие</b>\n" + "\n".join(format_match_compact(m) for m in other))

    # Добавляем статистику
    total_matches = len(matches)
    live_count = len(live)
    upcoming_count = len(upcoming)
    finished_count = len(finished)
    
    stats = f"\n📊 <i>Всего матчей: {total_matches} (LIVE: {live_count}, скоро: {upcoming_count}, завершено: {finished_count})</i>"
    
    # Добавляем время последнего обновления
    update_time = datetime.now(MSK_TZ).strftime("%H:%M")
    footer = f"\n\n🔄 <i>Обновлено в {update_time}</i>"
    
    body = "\n\n".join(parts)
    return header + "\n" + body + stats + footer


def format_match_compact(match: Match) -> str:
    """Компактное форматирование одного матча для единого сообщения"""
    status = (match.status or "").lower()

    if status == "upcoming":
        status_emoji = "⏰"
    elif status == "live":
        status_emoji = "🟢"
    elif status == "finished":
        status_emoji = "✅"
    else:
        status_emoji = "❓"

    time_line = (
        match.time_msk
        or match.match_time_msk.astimezone(MSK_TZ).strftime("%H:%M")
    )

    score_line = f" | {match.score}" if match.score else ""

    # Компактный формат: время команды формат счет
    return f"  {status_emoji} <b>{time_line}</b> {match.team1} vs {match.team2} (Bo{match.bo}){score_line}"


def build_tournaments_keyboard(matches: List[Match], excluded: Set[str]) -> Optional[InlineKeyboardMarkup]:
    """Создание клавиатуры турниров (без изменений)"""
    tournaments = sorted({m.tournament for m in matches})
    if not tournaments:
        return None

    rows = []
    for idx, t in enumerate(tournaments):
        hidden = t in excluded
        prefix = "🚫" if hidden else "✅"
        text = f"{prefix} {t}"
        rows.append(
            [InlineKeyboardButton(text=text, callback_data=f"filter:{idx}")]
        )

    return InlineKeyboardMarkup(inline_keyboard=rows)


# -------------------- Улучшенные фоновые задачи ----------

async def poll_matches_optimized(bot: Bot) -> None:
    """
    Оптимизированный поллер матчей:
    - Пакетная обработка состояний
    - Кэширование результатов
    - Улучшенная обработка ошибок
    """
    logger.info("Старт оптимизированного поллера матчей (интервал %s сек)", POLL_INTERVAL_SECONDS)
    
    # Кэш для матчей
    matches_cache: Dict[date, List[Match]] = {}
    cache_ttl = timedelta(minutes=5)
    last_cache_update: Dict[date, datetime] = {}
    
    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            
            today = datetime.now(MSK_TZ).date()
            yesterday = today - timedelta(days=1)
            
            for day in (yesterday, today):
                # Проверяем кэш
                if (day in matches_cache and 
                    day in last_cache_update and 
                    datetime.now() - last_cache_update[day] < cache_ttl):
                    matches = matches_cache[day]
                    logger.debug("Использован кэш для дня %s", day)
                else:
                    # Получаем матчи и обновляем кэш
                    matches = await fetch_matches_for_day(day)
                    matches_cache[day] = matches
                    last_cache_update[day] = datetime.now()
                    logger.debug("Обновлен кэш для дня %s", day)
                
                # Получаем состояния асинхронно
                states = await get_all_today_states_for_day_async(day)
                if not states:
                    continue
                
                # Обрабатываем состояния пакетом
                tasks = []
                for state in states:
                    task = process_state_update(bot, state, matches, day)
                    tasks.append(task)
                
                # Выполняем все задачи параллельно
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

        except asyncio.CancelledError:
            logger.info("Оптимизированный поллер матчей остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error("Ошибка в оптимизированном поллере матчей: %s", e, exc_info=True)
            continue


async def process_state_update(bot: Bot, state: TodayMessageState, matches: List[Match], day: date) -> None:
    """Обработка обновления одного состояния"""
    try:
        filtered_matches = [
            m for m in matches
            if m.tournament not in state.excluded_tournaments
        ]
        
        new_text = format_matches_grouped(filtered_matches, day)
        keyboard = build_tournaments_keyboard(matches, state.excluded_tournaments)
        
        if new_text == (state.last_text or ""):
            logger.debug(
                "Чат %s / день %s: текст не изменился, пропускаем обновление",
                state.chat_id,
                day,
            )
            return
        
        await bot.edit_message_text(
            chat_id=state.chat_id,
            message_id=state.message_id,
            text=new_text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        
        # Обновляем состояние асинхронно
        state.last_text = new_text
        await upsert_today_state_async(state)
        
        logger.info(
            "Обновили today-сообщение в чате %s (message_id=%s, day=%s)",
            state.chat_id,
            state.message_id,
            day,
        )
        
    except Exception as e:
        logger.warning(
            "Не удалось обновить today-сообщение в чате %s (message_id=%s, day=%s): %s",
            state.chat_id,
            state.message_id,
            day,
            e,
        )


async def daily_notifier_optimized(bot: Bot) -> None:
    """
    Оптимизированный ежедневный нотификатор:
    - Пакетная обработка подписчиков
    - Асинхронная отправка сообщений
    - Улучшенная обработка ошибок
    """
    global last_daily_notify_date
    logger.info("Старт оптимизированного ежедневного нотификатора (10:00 МСК)")
    
    while True:
        try:
            await asyncio.sleep(30)
            now_msk = datetime.now(MSK_TZ)
            today = now_msk.date()
            
            if last_daily_notify_date == today:
                continue
            
            if now_msk.hour == 10:
                logger.info("10-й час МСК, отправляем ежедневные уведомления за день %s", today)
                
                # Получаем подписчиков асинхронно
                subs = await get_all_subscribers_async()
                if not subs:
                    logger.info("Подписчиков нет, рассылку пропускаем")
                    last_daily_notify_date = today
                    continue
                
                # Получаем матчи
                matches = await fetch_matches_for_day(today)
                if not matches:
                    logger.info("Матчей нет, рассылку пропускаем")
                    last_daily_notify_date = today
                    continue
                
                # Обрабатываем подписчиков пакетом
                tasks = []
                for chat_id in subs:
                    task = process_daily_notification(bot, chat_id, matches, today)
                    tasks.append(task)
                
                # Выполняем все задачи параллельно
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Подсчитываем успешные/неуспешные отправки
                    successful = sum(1 for r in results if r is True)
                    failed = sum(1 for r in results if r is False)
                    
                    logger.info(
                        "Ежедневная рассылка завершена: успешно %s, ошибок %s",
                        successful,
                        failed
                    )
                
                last_daily_notify_date = today

        except asyncio.CancelledError:
            logger.info("Оптимизированный ежедневный нотификатор остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error("Ошибка в оптимизированном ежедневном нотификаторе: %s", e, exc_info=True)
            continue


async def process_daily_notification(bot: Bot, chat_id: int, matches: List[Match], today: date) -> bool:
    """Обработка ежедневного уведомления для одного подписчика"""
    try:
        state = await get_today_state_async(chat_id, today)
        if state:
            excluded = state.excluded_tournaments
        else:
            excluded = set()
            state = TodayMessageState(
                chat_id=chat_id,
                day=today,
                message_id=0,
                excluded_tournaments=excluded,
                last_text=None,
            )
        
        filtered_matches = [
            m for m in matches
            if m.tournament not in excluded
        ]
        text = "⏰ Ежедневное уведомление о матчах:\n\n" + format_matches_grouped(filtered_matches, today)
        keyboard = build_tournaments_keyboard(matches, excluded)
        
        if state.message_id:
            # Обновляем существующее сообщение
            await bot.edit_message_text(
                chat_id=state.chat_id,
                message_id=state.message_id,
                text=text,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            logger.info(
                "Ежедневное уведомление: обновили today-сообщение в чате %s (message_id=%s, day=%s)",
                state.chat_id,
                state.message_id,
                state.day,
            )
        else:
            # Отправляем новое сообщение
            sent = await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            state.chat_id = sent.chat.id
            state.message_id = sent.message_id
            state.day = today
            logger.info(
                "Ежедневное уведомление: отправили новое today-сообщение в чате %s (message_id=%s, day=%s)",
                sent.chat.id,
                sent.message_id,
                today,
            )
        
        # Обновляем состояние асинхронно
        state.last_text = text
        await upsert_today_state_async(state)
        
        return True
        
    except Exception as e:
        logger.warning(
            "Не удалось отправить/обновить уведомление в чате %s: %s",
            chat_id,
            e,
        )
        return False


# -------------------- Telegram-бот с улучшениями ----------

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Фикс для uvloop/asyncio на Python 3.8:
try:
    asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработка команды /start с асинхронной работой с БД"""
    user_id = message.from_user.id if message.from_user else "unknown"
    chat_id = message.chat.id
    logger.info("Команда /start от пользователя %s в чате %s", user_id, chat_id)

    await add_subscriber_async(chat_id)

    await message.answer(
        "👋 Привет! Я бот с расписанием Dota-матчей.\n\n"
        "Команды:\n"
        "• /today — матчи на выбранный день (МСК), одно автообновляемое сообщение на день\n"
        "  с фильтрами по турнирам под сообщением.\n"
        "• /start — подписаться на ежедневные уведомления в 10:00 (МСК).\n\n"
        "Матчи группируются: LIVE, скоро начнутся, завершённые.\n"
        "Результаты за вчера тоже догружаются, чтобы не висели 0-0 🙂"
    )


@dp.message(Command("today"))
async def cmd_today(message: Message):
    """Обработка команды /today с асинхронной работой с БД"""
    global poll_task

    user_id = message.from_user.id if message.from_user else "unknown"
    chat_id = message.chat.id
    day = datetime.now(MSK_TZ).date()

    logger.info(
        "Команда /today от пользователя %s в чате %s для дня %s",
        user_id,
        chat_id,
        day,
    )

    matches = await fetch_matches_for_day(day)
    state = await get_today_state_async(chat_id, day)
    
    if state:
        excluded = state.excluded_tournaments
    else:
        excluded = set()
        state = TodayMessageState(
            chat_id=chat_id,
            day=day,
            message_id=0,
            excluded_tournaments=excluded,
            last_text=None,
        )

    filtered_matches = [
        m for m in matches
        if m.tournament not in excluded
    ]
    text = format_matches_grouped(filtered_matches, day)
    keyboard = build_tournaments_keyboard(matches, excluded)

    if state.message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=state.chat_id,
                message_id=state.message_id,
                text=text,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            logger.info(
                "Обновили существующее today-сообщение в чате %s (message_id=%s, day=%s)",
                state.chat_id,
                state.message_id,
                state.day,
            )
        except Exception as e:
            logger.warning(
                "Не удалось обновить существующее today-сообщение в чате %s: %s. Отправляем новое.",
                state.chat_id,
                e,
            )
            sent: Message = await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
            state.chat_id = sent.chat.id
            state.message_id = sent.message_id
            state.day = day
            logger.info(
                "Отправили новое today-сообщение в чате %s (message_id=%s, day=%s)",
                sent.chat.id,
                sent.message_id,
                day,
            )
    else:
        sent: Message = await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        state.chat_id = sent.chat.id
        state.message_id = sent.message_id
        state.day = day
        logger.info(
            "Отправили первое today-сообщение в чате %s (message_id=%s, day=%s)",
            sent.chat.id,
            sent.message_id,
            day,
        )

    state.last_text = text
    await upsert_today_state_async(state)

    if poll_task is None or poll_task.done():
        logger.info("Поллер матчей ещё не запущен — стартуем фоновую задачу")
        poll_task = asyncio.create_task(poll_matches_optimized(bot))
    else:
        logger.info("Поллер матчей уже запущен, новую задачу не создаём")


@dp.callback_query(F.data.startswith("filter:"))
async def callback_filter_tournament(callback: CallbackQuery):
    """Обработка нажатий на кнопки турниров с асинхронной работой с БД"""
    if not callback.message:
        await callback.answer()
        return

    chat_id = callback.message.chat.id
    today = datetime.now(MSK_TZ).date()
    yesterday = today - timedelta(days=1)

    # Пытаемся найти state за сегодня, если нет — за вчера
    state = await get_today_state_async(chat_id, today)
    if not state:
        state = await get_today_state_async(chat_id, yesterday)

    if not state:
        # fallback: считаем, что это сегодняшнее сообщение
        state = TodayMessageState(
            chat_id=chat_id,
            day=today,
            message_id=callback.message.message_id,
            excluded_tournaments=set(),
            last_text=callback.message.html_text,
        )

    day = state.day

    data = callback.data or ""
    try:
        _, idx_str = data.split(":", 1)
        idx = int(idx_str)
    except Exception:
        await callback.answer()
        return

    matches = await fetch_matches_for_day(day)
    tournaments = sorted({m.tournament for m in matches})

    if not (0 <= idx < len(tournaments)):
        await callback.answer()
        return

    tournament_name = tournaments[idx]

    if tournament_name in state.excluded_tournaments:
        state.excluded_tournaments.remove(tournament_name)
    else:
        state.excluded_tournaments.add(tournament_name)

    filtered_matches = [
        m for m in matches
        if m.tournament not in state.excluded_tournaments
    ]
    new_text = format_matches_grouped(filtered_matches, day)
    keyboard = build_tournaments_keyboard(matches, state.excluded_tournaments)

    try:
        await callback.message.edit_text(
            new_text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        state.last_text = new_text
        state.message_id = callback.message.message_id
        await upsert_today_state_async(state)
        logger.info(
            "Чат %s: обновили today-сообщение после фильтра турнира '%s' (day=%s)",
            chat_id,
            tournament_name,
            day,
        )
    except Exception as e:
        logger.warning(
            "Не удалось обновить today-сообщение по callback в чате %s: %s",
            chat_id,
            e,
        )

    await callback.answer()


# -------------------- Главная функция с улучшениями ----------

async def main_refactored():
    """Главная функция с улучшенной асинхронной работой"""
    global daily_task, poll_task
    logger.info("Запуск оптимизированного бота...")

    # Инициализируем БД асинхронно
    await init_db_async()
    await async_db_pool.init_pool()

    # Стартуем оптимизированные фоновые задачи
    poll_task = asyncio.create_task(poll_matches_optimized(bot))
    daily_task = asyncio.create_task(daily_notifier_optimized(bot))

    try:
        await dp.start_polling(bot)
    finally:
        # Корректное завершение
        for task_name, task in (("poll_task", poll_task), ("daily_task", daily_task)):
            if task and not task.done():
                logger.info("Останавливаем задачу %s", task_name)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        
        # Закрываем пул подключений
        await async_db_pool.close_pool()
        logger.info("Оптимизированный бот остановлен")



if __name__ == "__main__":
    try:
        asyncio.run(main_refactored())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Завершение по сигналу KeyboardInterrupt/SystemExit")
