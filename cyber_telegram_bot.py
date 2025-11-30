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

import aiohttp
import psycopg
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


# -------------------- Работа с БД --------------------

def get_db_conn():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def init_db():
    """Создаём таблицы подписчиков и today-сообщений, если их ещё нет."""
    logger.info("Инициализация БД...")
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            # Подписчики
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dota_bot_subscribers (
                    chat_id BIGINT PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            # Сообщения /today и утренние
            cur.execute(
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
            )
        conn.commit()
    logger.info("БД и таблицы инициализированы.")


def add_subscriber(chat_id: int):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dota_bot_subscribers (chat_id)
                VALUES (%s)
                ON CONFLICT (chat_id) DO NOTHING;
                """,
                (chat_id,),
            )
        conn.commit()
    logger.info("Чат %s добавлен в подписчики (или уже был).", chat_id)


def get_all_subscribers() -> List[int]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id FROM dota_bot_subscribers;")
            rows = cur.fetchall()
    subs = [r[0] for r in rows]
    logger.info("Из БД получено подписчиков: %s", len(subs))
    return subs


def _serialize_excluded(excluded: Set[str]) -> str:
    if not excluded:
        return ""
    return json.dumps(sorted(excluded), ensure_ascii=False)


def _deserialize_excluded(raw: Optional[str]) -> Set[str]:
    if not raw:
        return set()
    try:
        arr = json.loads(raw)
        return set(arr)
    except Exception:
        return set()


def upsert_today_state(state: TodayMessageState):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
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
                    _serialize_excluded(state.excluded_tournaments),
                    state.last_text,
                ),
            )
        conn.commit()
    logger.info(
        "Состояние today-сообщения сохранено: chat_id=%s, day=%s, message_id=%s",
        state.chat_id,
        state.day,
        state.message_id,
    )


def get_today_state(chat_id: int, day: date) -> Optional[TodayMessageState]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT message_id, excluded_tournaments, last_text
                FROM dota_bot_today_messages
                WHERE chat_id = %s AND day = %s;
                """,
                (chat_id, day),
            )
            row = cur.fetchone()
    if not row:
        return None

    message_id, excluded_raw, last_text = row
    return TodayMessageState(
        chat_id=chat_id,
        day=day,
        message_id=message_id,
        excluded_tournaments=_deserialize_excluded(excluded_raw),
        last_text=last_text,
    )


def get_all_today_states_for_day(day: date) -> List[TodayMessageState]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT chat_id, message_id, excluded_tournaments, last_text
                FROM dota_bot_today_messages
                WHERE day = %s;
                """,
                (day,),
            )
            rows = cur.fetchall()

    result: List[TodayMessageState] = []
    for chat_id, message_id, excluded_raw, last_text in rows:
        result.append(
            TodayMessageState(
                chat_id=chat_id,
                day=day,
                message_id=message_id,
                excluded_tournaments=_deserialize_excluded(excluded_raw),
                last_text=last_text,
            )
        )
    logger.info(
        "Для дня %s найдено today-сообщений: %s",
        day,
        len(result),
    )
    return result


# -------------------- Работа с API --------------------

def build_matches_url_for_day(day: date) -> str:
    # формат DD-MM-YYYY
    return f"{MATCHES_API_BASE_URL}/{day.strftime('%d-%m-%Y')}"


async def fetch_matches_for_day(day: date) -> List[Match]:
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


# -------------------- Форматирование --------------------

def format_match(match: Match) -> str:
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
    """
    Группируем:
    1) LIVE (сверху)
    2) Скоро начнутся
    3) Завершённые
    """
    header = f"📅 Матчи на {day.isoformat()} (МСК)\n"

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
        parts.append("🟢 LIVE\n\n" + "\n\n".join(format_match(m) for m in live))

    if upcoming:
        parts.append("⏰ Скоро начнутся\n\n" + "\n\n".join(format_match(m) for m in upcoming))

    if finished:
        parts.append("✅ Завершённые\n\n" + "\n\n".join(format_match(m) for m in finished))

    if other:
        parts.append("❓ Прочие\n\n" + "\n\n".join(format_match(m) for m in other))

    body = "\n\n────────────\n\n".join(parts)
    return header + "\n" + body


def build_tournaments_keyboard(matches: List[Match], excluded: Set[str]) -> Optional[InlineKeyboardMarkup]:
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


# -------------------- Фоновый поллер матчей --------------------

async def poll_matches(bot: Bot) -> None:
    """
    Поллер:
    - раз в POLL_INTERVAL_SECONDS тянет матчи;
    - берёт today-сообщения за сегодня и за вчера;
    - для каждого применяет его фильтры и, если текст поменялся, редактирует сообщение.
    """
    logger.info(
        "Старт фонового поллера матчей (интервал %s сек)", POLL_INTERVAL_SECONDS
    )

    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

            today = datetime.now(MSK_TZ).date()
            yesterday = today - timedelta(days=1)

            for day in (yesterday, today):
                states = get_all_today_states_for_day(day)
                if not states:
                    continue

                matches = await fetch_matches_for_day(day)

                for state in states:
                    filtered_matches = [
                        m for m in matches
                        if m.tournament not in state.excluded_tournaments
                    ]

                    new_text = format_matches_grouped(filtered_matches, day)
                    keyboard = build_tournaments_keyboard(matches, state.excluded_tournaments)

                    if new_text == (state.last_text or ""):
                        logger.info(
                            "Чат %s / день %s: текст не изменился, пропускаем обновление",
                            state.chat_id,
                            day,
                        )
                        continue

                    try:
                        await bot.edit_message_text(
                            chat_id=state.chat_id,
                            message_id=state.message_id,
                            text=new_text,
                            parse_mode="HTML",
                            reply_markup=keyboard,
                        )
                        state.last_text = new_text
                        upsert_today_state(state)
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

        except asyncio.CancelledError:
            logger.info("Фоновый поллер матчей остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error("Ошибка в поллере матчей: %s", e, exc_info=True)
            continue


# -------------------- Ежедневные уведомления --------------------

async def daily_notifier(bot: Bot) -> None:
    """
    В 10:00 МСК:
    - для каждого подписчика берём/создаём TodayMessageState на сегодняшний день;
    - тянем матчи для ЭТОГО дня через /dota/matches/DD-MM-YYYY;
    - формируем форматированный текст (с группировкой) и фильтрами.
    """
    global last_daily_notify_date
    logger.info("Старт ежедневного нотификатора (10:00 МСК)")

    while True:
        try:
            await asyncio.sleep(30)
            now_msk = datetime.now(MSK_TZ)
            today = now_msk.date()

            if last_daily_notify_date == today:
                continue

            if now_msk.hour == 10:
                logger.info("10-й час МСК, отправляем ежедневные уведомления за день %s", today)
                matches = await fetch_matches_for_day(today)

                subs = get_all_subscribers()
                if not subs:
                    logger.info("Подписчиков нет, рассылку пропускаем")
                    last_daily_notify_date = today
                    continue

                for chat_id in subs:
                    state = get_today_state(chat_id, today)
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

                    try:
                        if state.message_id:
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

                        state.last_text = text
                        upsert_today_state(state)
                    except Exception as e:
                        logger.warning(
                            "Не удалось отправить/обновить уведомление в чате %s: %s",
                            chat_id,
                            e,
                        )

                last_daily_notify_date = today

        except asyncio.CancelledError:
            logger.info("Ежедневный нотификатор остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error("Ошибка в ежедневном нотификаторе: %s", e, exc_info=True)
            continue


# -------------------- Telegram-бот --------------------

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
    user_id = message.from_user.id if message.from_user else "unknown"
    chat_id = message.chat.id
    logger.info("Команда /start от пользователя %s в чате %s", user_id, chat_id)

    add_subscriber(chat_id)

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

    state = get_today_state(chat_id, day)
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
    upsert_today_state(state)

    if poll_task is None or poll_task.done():
        logger.info("Поллер матчей ещё не запущен — стартуем фоновую задачу")
        poll_task = asyncio.create_task(poll_matches(message.bot))
    else:
        logger.info("Поллер матчей уже запущен, новую задачу не создаём")


@dp.callback_query(F.data.startswith("filter:"))
async def callback_filter_tournament(callback: CallbackQuery):
    """
    Обработка нажатий на кнопки турниров.
    Переключает турнир в excluded_tournaments и перерисовывает текущее сообщение.
    """
    if not callback.message:
        await callback.answer()
        return

    chat_id = callback.message.chat.id
    today = datetime.now(MSK_TZ).date()
    yesterday = today - timedelta(days=1)

    # Пытаемся найти state за сегодня, если нет — за вчера
    state = get_today_state(chat_id, today)
    if not state:
        state = get_today_state(chat_id, yesterday)

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
        upsert_today_state(state)
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


async def main():
    global daily_task, poll_task
    logger.info("Запуск бота...")

    init_db()

    # Стартуем поллер и ежедневный нотификатор сразу,
    # а не ждём команду /today
    poll_task = asyncio.create_task(poll_matches(bot))
    daily_task = asyncio.create_task(daily_notifier(bot))

    try:
        await dp.start_polling(bot)
    finally:
        for task_name, task in (("poll_task", poll_task), ("daily_task", daily_task)):
            if task and not task.done():
                logger.info("Останавливаем задачу %s", task_name)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        logger.info("Бот остановлен")



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Завершение по сигналу KeyboardInterrupt/SystemExit")
