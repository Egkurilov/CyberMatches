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

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python 3.8 и ниже

from aiogram.exceptions import TelegramBadRequest

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder



# -------------------- Настройки и логирование --------------------

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

MATCHES_API_URL = os.getenv(
    "MATCHES_API_URL",
    "http://45.10.245.84:8050/dota/matches/today",
)

# Базовый URL для /dota/matches/DD-MM-YYYY
MATCHES_API_BASE_URL = os.getenv("MATCHES_API_BASE_URL")
if not MATCHES_API_BASE_URL:
    if MATCHES_API_URL.endswith("/today"):
        MATCHES_API_BASE_URL = MATCHES_API_URL.rsplit("/", 1)[0]
    else:
        MATCHES_API_BASE_URL = MATCHES_API_URL

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

MSK_TZ = ZoneInfo("Europe/Moscow")

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

logger = logging.getLogger("dota_matches_bot")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
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
    # новые поля с URL команд (если приходят из API)
    team1_url: Optional[str] = None
    team2_url: Optional[str] = None
    liquipedia_match_id: Optional[str] = None



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

UPDATED_MARKER = "\n\n🔄 Обновлено в "


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
    logger.info("Инициализация БД...")
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dota_bot_subscribers (
                    chat_id BIGINT PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
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
                SET
                    message_id = EXCLUDED.message_id,
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
    logger.info("Для дня %s найдено today-состояний: %s", day, len(result))
    return result


# -------------------- работа с напоминаниями  --------------------
def build_main_keyboard(
    filtered_matches: List[Match],
    all_matches: List[Match],
    excluded: Set[str],
) -> InlineKeyboardMarkup:
    """
    Главная клавиатура под сообщением:
    - сверху фильтры турниров (по всем матчам дня),
    - ниже кнопки 'Напомнить' по будущим матчам из filtered_matches.
    """
    filters_kb = build_tournaments_keyboard(all_matches, excluded)
    reminders_kb = build_reminders_keyboard(filtered_matches)

    rows: List[List[InlineKeyboardButton]] = []

    if filters_kb and filters_kb.inline_keyboard:
        rows.extend(filters_kb.inline_keyboard)

    if reminders_kb and reminders_kb.inline_keyboard:
        rows.extend(reminders_kb.inline_keyboard)

    return InlineKeyboardMarkup(inline_keyboard=rows)




# -------------------- работа с напоминаниями  --------------------


def build_reminders_keyboard(matches: List[Match]) -> InlineKeyboardMarkup:
    """
    Строит клавиатуру с кнопками 'Напомнить' по матчам.
    Используем liquipedia_match_id как идентификатор.
    Напоминания предлагаются ТОЛЬКО по матчам:
      - которые не finished и не live
      - и время которых ещё не прошло.
    """
    kb = InlineKeyboardBuilder()
    now_msk = datetime.now(MSK_TZ)

    for m in matches:
        status = (m.status or "").lower()

        # Не напоминаем о завершённых и идущих матчах
        if status in ("finished", "live"):
            continue

        # Не напоминаем о матчах, время которых уже прошло
        if m.match_time_msk <= now_msk:
            continue

        match_key = m.liquipedia_match_id
        if not match_key:
            continue

        if m.team1 and m.team2:
            title = f"{m.team1} vs {m.team2}"
        elif m.team1 or m.team2:
            title = m.team1 or m.team2
        else:
            title = m.tournament or "Матч"

        time_str = m.match_time_msk.strftime("%H:%M")
        text = f"🔔 {time_str} {title}"

        cb_data = f"remind:{match_key}"

        kb.row(
            InlineKeyboardButton(
                text=text[:64],
                callback_data=cb_data,
            )
        )

    return kb.as_markup()


REMIND_OFFSET_MINUTES = 0  # можно сделать 5 или 10, если хочешь напоминать заранее

def create_match_reminder(
    chat_id: int,
    liquipedia_match_id: str,
    remind_at: datetime,
    title: str,
) -> bool:
    """
    Создаёт напоминание о матче.
    Возвращает True, если запись создана, False — если уже было такое же (по UNIQUE).
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO dota_bot_match_reminders (
                        chat_id,
                        liquipedia_match_id,
                        remind_at,
                        title
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (chat_id, liquipedia_match_id, remind_at) DO NOTHING;
                    """,
                    (chat_id, liquipedia_match_id, remind_at, title),
                )
                inserted = cur.rowcount > 0
            except Exception as e:
                logger.error("Ошибка при создании напоминания: %s", e, exc_info=True)
                conn.rollback()
                return False

        conn.commit()

    return inserted


def get_match_by_id(match_id: int) -> Optional[Match]:
    # тут уже зависит от твоей архитектуры:
    # либо запрос в таблицу матчей, либо API, либо кэш
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT match_id, match_time_msk, team1, team2, tournament
                FROM dota_matches
                WHERE match_id = %s
                """,
                (match_id,),
            )
            row = cur.fetchone()

    if not row:
        return None

    # Пример адаптации под твою модель Match
    return Match(
        match_id=row[0],
        match_time_msk=row[1],
        team1=row[2],
        team2=row[3],
        tournament=row[4],
        # остальные поля по умолчанию/None
    )




# -------------------- Вспомогательные функции матчей --------------------

def build_matches_url_for_day(day: date) -> str:
    return f"{MATCHES_API_BASE_URL}/{day.strftime('%d-%m-%Y')}"


def _status_rank(status: str) -> int:
    s = (status or "").lower()
    if s == "upcoming":
        return 0
    if s == "live":
        return 1
    if s == "finished":
        return 2
    return -1


def _is_bad_score(score: Optional[str]) -> bool:
    if not score:
        return False
    s = str(score).strip()
    return s in {"0:0", "0-0", "-", "—", "–"}


def deduplicate_matches(matches: List[Match]) -> List[Match]:
    """
    Дедупликация матчей:
    - ключ по (match_time_msk, team1, team2, tournament, bo)
    - при конфликте берём:
      * более "сильный" статус (live > upcoming, finished > upcoming и т.д.)
      * нормальный счёт вместо "0:0"/"-"
      * при прочих равных — последнюю запись
    """
    best: Dict[tuple, Match] = {}

    for m in matches:
        key = (
            m.match_time_msk,
            m.team1,
            m.team2,
            m.tournament,
            m.bo,
        )

        if key not in best:
            best[key] = m
            continue

        prev = best[key]

        prev_rank = _status_rank(prev.status)
        new_rank = _status_rank(m.status)

        prev_bad = _is_bad_score(prev.score)
        new_bad = _is_bad_score(m.score)

        replace = False

        if new_rank > prev_rank:
            replace = True
        elif new_rank < prev_rank:
            replace = False
        else:
            if prev_bad and not new_bad:
                replace = True
            elif not prev_bad and new_bad:
                replace = False
            else:
                replace = True

        if replace:
            best[key] = m

    result = sorted(best.values(), key=lambda mm: mm.match_time_msk)
    logger.info("Дедупликация: было %s матчей, осталось %s", len(matches), len(result))
    return result

async def fetch_matches_for_day(day: date) -> List[Match]:
    """
    Тянем матчи из API для указанного дня.
    При ошибке сети/таймауте/парсинга возвращаем
    последний успешный результат для этого дня (если он есть),
    чтобы не моргать пустыми сообщениями в Телеге.
    """
    # Простейший in-memory кэш на уровне функции:
    # { date: List[Match] }
    if not hasattr(fetch_matches_for_day, "_cache"):
        fetch_matches_for_day._cache = {}  # type: ignore[attr-defined]
    cache: Dict[date, List[Match]] = fetch_matches_for_day._cache  # type: ignore[attr-defined]

    url = build_matches_url_for_day(day)
    logger.info("Запрос матчей из API: %s для дня %s", url, day.isoformat())

    data = None

    # --- 1. Пытаемся сходить в API ---
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
    except Exception as e:
        logger.error("Ошибка API для дня %s: %s", day.isoformat(), e, exc_info=True)

        # Если у нас уже был успешный ответ на этот день — возвращаем его
        if day in cache:
            logger.info(
                "Используем закешированный список матчей для дня %s из-за ошибки API",
                day,
            )
            return cache[day]

        # Кэша нет (например, бот только что запустился) — отдаём пустой список
        return []

    # --- 2. Парсим JSON и собираем список матчей ---
    try:
        matches_raw = data.get("matches", [])

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
                # если нет поля времени, пропускаем матч
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
                    # 👉 прокидываем URL’ы команд из JSON
                    team1_url=raw.get("team1_url"),
                    team2_url=raw.get("team2_url"),
                    liquipedia_match_id=raw.get("liquipedia_match_id"),
                )
            )


        result = deduplicate_matches(result)

        # Успешно спарсили — обновляем кэш для этого дня
        cache[day] = result
        logger.info(
            "В кэше теперь %s матчей для дня %s", len(result), day.isoformat()
        )

        return result

    except Exception as e:
        logger.error("Ошибка парсинга API для дня %s: %s", day, e, exc_info=True)

        # Если парсер упал, но в кэше есть старые матчи — используем их
        if day in cache:
            logger.info(
                "Используем закешированный список матчей для дня %s из-за ошибки парсинга",
                day,
            )
            return cache[day]

        return []



def _format_match_line(m: Match, group: str) -> str:
    """
    Пример:
    ⏰ 15:00 <a href="...">AVULUS</a> vs <a href="...">Passion</a> (Bo3) [CCT S2 Series 6] 1:0
    """
    time_part = m.time_msk or m.match_time_msk.strftime("%H:%M")

    # Если есть URL — делаем кликабельное имя
    if m.team1_url:
        team1 = f'<a href="{m.team1_url}">{m.team1}</a>'
    else:
        team1 = m.team1

    if m.team2_url:
        team2 = f'<a href="{m.team2_url}">{m.team2}</a>'
    else:
        team2 = m.team2

    parts = [f"⏰ {time_part}", f"{team1} vs {team2}"]

    if m.bo:
        parts.append(f"(Bo{m.bo})")
    if m.tournament:
        parts.append(f"[{m.tournament}]")
    if m.score and group in ("live", "finished"):
        parts.append(m.score)

    return " ".join(parts)



def build_core_text(matches: List[Match], day: date) -> str:
    """
    Формирует текстовую часть сообщения по матчам.
    Категории:
      - LIVE
      - Скоро начнутся (не finished, не live и временем в будущем)
      - Завершённые
    """
    now_msk = datetime.now(MSK_TZ)

    live: List[Match] = []
    upcoming: List[Match] = []
    finished: List[Match] = []

    for m in matches:
        status = (m.status or "").lower()

        if status == "live":
            live.append(m)
        elif status == "finished":
            finished.append(m)
        else:
            # Всё, что не live и не finished:
            # считаем "скоро", только если матч ещё не начался по времени
            if m.match_time_msk > now_msk:
                upcoming.append(m)
            # если время уже прошло, а статус не finished — просто не показываем
            # (это обычно проблемы/задержки в исходных данных)

    # Сортировка по времени
    live.sort(key=lambda m: m.match_time_msk)
    upcoming.sort(key=lambda m: m.match_time_msk)
    finished.sort(key=lambda m: m.match_time_msk)

    parts: List[str] = []

    # Заголовок
    parts.append(f"📅 Матчи на {day.strftime('%d.%m.%Y')} (МСК)\n")

    # LIVE
    if live:
        lines = ["🟢 LIVE"] + [_format_match_line(m, "live") for m in live]
        parts.append("\n".join(lines))

    # Скоро начнутся
    if upcoming:
        lines = ["⏰ Скоро начнутся"] + [_format_match_line(m, "upcoming") for m in upcoming]
        parts.append("\n".join(lines))

    # Завершённые
    if finished:
        lines = ["✅ Завершённые"] + [_format_match_line(m, "finished") for m in finished]
        parts.append("\n".join(lines))

    # Итоги — считаем только то, что реально попало в список
    total = len(live) + len(upcoming) + len(finished)
    live_count = len(live)
    upcoming_count = len(upcoming)
    finished_count = len(finished)

    parts.append(
        f"\n📊 Всего матчей: {total} "
        f"(LIVE: {live_count}, скоро: {upcoming_count}, завершено: {finished_count})"
    )

    return "\n\n".join(parts)


def make_full_text(core: str, now_msk: datetime) -> str:
    return core + UPDATED_MARKER + now_msk.strftime("%H:%M")


def extract_core(text: Optional[str]) -> str:
    if not text:
        return ""
    idx = text.rfind(UPDATED_MARKER)
    if idx == -1:
        return text
    return text[:idx]


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
    Фоновый поллер:
    - периодически обновляет матчи на сегодня
    - обновляет today-сообщения во всех чатах
    - если сообщение в Телеге уже удалено, очищает состояние, чтобы не дёргать его дальше
    """
    logger.info("Старт фонового поллера матчей")

    while True:
        try:
            await asyncio.sleep(60)

            day = datetime.now(MSK_TZ).date()
            logger.info("Поллер: обновляем матчи для дня %s", day)

            # 1. Тянем матчи из API
            matches = await fetch_matches_for_day(day)
            logger.info("Поллер: из API получено матчей: %s", len(matches))

            # 2. Берём все today-состояния на этот день
            states = get_all_today_states_for_day(day)
            logger.info("Для дня %s найдено today-состояний: %s", day, len(states))

            for state in states:
                excluded = state.excluded_tournaments or set()

                # 3. Применяем фильтры турниров
                if excluded:
                    filtered_matches = [
                        m for m in matches if m.tournament not in excluded
                    ]
                else:
                    filtered_matches = matches

                now_msk = datetime.now(MSK_TZ)
                core = build_core_text(filtered_matches, day)
                new_text = make_full_text(core, now_msk)

                # 4. Клавиатура: фильтры + напоминания по будущим матчам
                keyboard = build_main_keyboard(
                    filtered_matches=filtered_matches,
                    all_matches=matches,
                    excluded=excluded,
                )

                # 4а. Если текст не изменился — обновляем только клавиатуру
                if state.last_text == new_text:
                    logger.info(
                        "Чат %s / день %s: текст не изменился, обновляем только клавиатуру",
                        state.chat_id,
                        day,
                    )
                    try:
                        await bot.edit_message_reply_markup(
                            chat_id=state.chat_id,
                            message_id=state.message_id,
                            reply_markup=keyboard,
                        )
                    except TelegramBadRequest as e:
                        logger.warning(
                            "Не удалось обновить клавиатуру today-сообщения в чате %s: %s",
                            state.chat_id,
                            e,
                        )
                        # Если сообщения больше нет — чистим состояние, чтобы не спамить лог
                        if "message to edit not found" in str(e):
                            if "delete_today_state" in globals():
                                try:
                                    delete_today_state(state.chat_id, day)
                                except Exception as e2:
                                    logger.warning(
                                        "Ошибка при удалении today-состояния для чата %s: %s",
                                        state.chat_id,
                                        e2,
                                    )
                    except Exception as e:
                        logger.warning(
                            "Не удалось обновить клавиатуру today-сообщения в чате %s: %s",
                            state.chat_id,
                            e,
                        )
                    continue

                # 4б. Текст изменился — обновляем и текст, и клавиатуру
                try:
                    await bot.edit_message_text(
                        chat_id=state.chat_id,
                        message_id=state.message_id,
                        text=new_text,
                        parse_mode="HTML",
                        reply_markup=keyboard,
                    )
                    logger.info(
                        "Поллер: обновили today-сообщение в чате %s (message_id=%s, day=%s)",
                        state.chat_id,
                        state.message_id,
                        day,
                    )
                    state.last_text = new_text
                    upsert_today_state(state)
                    logger.info(
                        "Состояние today-сообщения сохранено: chat_id=%s, day=%s, message_id=%s",
                        state.chat_id,
                        state.day,
                        state.message_id,
                    )

                except TelegramBadRequest as e:
                    logger.warning(
                        "Не удалось обновить today-сообщение в чате %s: %s",
                        state.chat_id,
                        e,
                    )
                    # Сообщение удалено / недоступно — чистим состояние
                    if "message to edit not found" in str(e):
                        if "delete_today_state" in globals():
                            try:
                                delete_today_state(state.chat_id, day)
                            except Exception as e2:
                                logger.warning(
                                    "Ошибка при удалении today-состояния для чата %s: %s",
                                    state.chat_id,
                                    e2,
                                )

                except Exception as e:
                    logger.warning(
                        "Не удалось обновить today-сообщение в чате %s: %s",
                        state.chat_id,
                        e,
                    )

        except asyncio.CancelledError:
            logger.info("Поллер матчей остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error("Ошибка в поллере матчей: %s", e, exc_info=True)
            continue


# -------------------- Telegram-бот --------------------

def delete_today_state(chat_id: int, day: date) -> None:
    """
    Удаляет состояние today-сообщения для конкретного чата и дня.
    Используем, если сообщение уже нельзя редактировать
    (message to edit not found и т.п.).
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM dota_bot_today_messages
                WHERE chat_id = %s AND day = %s;
                """,
                (chat_id, day),
            )
        conn.commit()
    logger.info("Удалили today-состояние: chat_id=%s, day=%s", chat_id, day)



# Фикс для uvloop/asyncio на Python 3.8 (uvloop + Python 3.8)
# Гарантируем, что к моменту создания Dispatcher уже есть текущий event loop.
try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

bot = Bot(token=TELEGRAM_BOT_TOKEN)
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
        "/today — матчи на сегодня\n"
        "/subscribe — подписаться на ежедневные уведомления\n"
        "/unsubscribe — отписаться от уведомлений\n"
        "/help — подробная справка."
    )

@dp.callback_query(F.data.startswith("remind:"))
async def callback_remind_match(call: CallbackQuery):
    """
    Обработчик нажатия на кнопку 'Напомнить'.
    В callback_data приходит: remind:<liquipedia_match_id>
    День берём из заголовка сообщения '📅 Матчи на DD.MM.YYYY (МСК)'.
    """
    if not call.message:
        try:
            await call.answer("Что-то пошло не так 🤔", show_alert=True)
        except TelegramBadRequest as e:
            logger.warning("Не удалось ответить на старый callback (no message): %s", e)
        return

    chat_id = call.message.chat.id
    data = call.data or ""

    # 1. Достаём liquipedia_match_id
    try:
        _, match_key = data.split(":", 1)
        match_key = match_key.strip()
    except Exception:
        try:
            await call.answer("Не понял, какой матч нужно напомнить 🤔", show_alert=True)
        except TelegramBadRequest as e:
            logger.warning("Не удалось ответить на старый callback (parse error): %s", e)
        return

    # 2. Пытаемся достать дату из заголовка 'Матчи на 07.12.2025 (МСК)'
    text = call.message.text or ""
    day = datetime.now(MSK_TZ).date()
    try:
        import re
        m = re.search(r"Матчи на (\d{2}\.\d{2}\.\d{4})", text)
        if m:
            day_str = m.group(1)
            day = datetime.strptime(day_str, "%d.%m.%Y").date()
    except Exception:
        pass

    # 3. Берём матчи на этот день и ищем нужный
    matches = await fetch_matches_for_day(day)
    match = next((m for m in matches if m.liquipedia_match_id == match_key), None)

    if not match:
        try:
            await call.answer("Не удалось найти матч для напоминания 😢", show_alert=True)
        except TelegramBadRequest as e:
            logger.warning("Не удалось ответить на старый callback (no match): %s", e)
        return

    # 4. Время и заголовок
    remind_at = match.match_time_msk

    if match.team1 and match.team2:
        title = f"{match.team1} vs {match.team2}"
    elif match.team1 or match.team2:
        title = match.team1 or match.team2
    else:
        title = match.tournament or "матч"

    # 5. Пишем в БД
    created = create_match_reminder(
        chat_id=chat_id,
        liquipedia_match_id=match_key,
        remind_at=remind_at,
        title=title,
    )

    time_str = remind_at.strftime("%H:%M")

    msg = (
        f"Ок, напомню в {time_str} про {title} 🔔"
        if created
        else "Такое напоминание уже стоит ✅"
    )

    try:
        await call.answer(msg, show_alert=True)
    except TelegramBadRequest as e:
        # Это тот самый случай "query is too old" — логируем и живём дальше
        logger.warning("Не удалось ответить на старый callback (remind): %s", e)

    logger.info(
        "Пользователь %s поставил напоминание про матч %s (%s) на %s (создано=%s)",
        chat_id,
        match_key,
        title,
        remind_at,
        created,
    )



@dp.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "Я показываю матчи Dota 2 по данным с Liquipedia.\n\n"
        "Команды:\n"
        "/today — матчи на сегодня (с возможностью скрывать турниры в inline-клавиатуре)\n"
        "/subscribe — подписаться на ежедневные уведомления (10:00 МСК)\n"
        "/unsubscribe — отписаться от уведомлений\n\n"
        "Формат сообщений:\n"
        "📅 Матчи на 02.12.2025 (МСК)\n\n"
        "🟢 LIVE\n"
        "  ⏰ 15:00 Team A vs Team B (Bo3) [Tournament] 1:0\n\n"
        "⏰ Скоро начнутся\n"
        "  ⏰ 18:00 Team C vs Team D (Bo3)\n\n"
        "✅ Завершённые\n"
        "  ⏰ 12:00 Team E vs Team F (Bo3) 2:1\n\n"
        "Матчи группируются по статусу: LIVE, скоро начнутся, завершённые.\n"
        "Дубликаты матчей схлопываются, а строка 'Обновлено' меняется только при реальных изменениях."
    )
    await message.answer(text)


@dp.message(Command("today"))
async def cmd_today(message: Message):
    """
    Команда /today:
    - отправляет сообщение с матчами на сегодня
    - под ним клавиатура: фильтры турниров + напоминания по будущим матчам
    - сохраняет это сообщение как актуальное today-сообщение
    """
    global poll_task

    user_id = message.from_user.id if message.from_user else "unknown"
    chat_id = message.chat.id
    day = datetime.now(MSK_TZ).date()

    # Автоподписка на ежедневные уведомления
    add_subscriber(chat_id)

    logger.info(
        "Команда /today от пользователя %s в чате %s для дня %s",
        user_id,
        chat_id,
        day,
    )

    # 1. Тянем матчи из API
    matches = await fetch_matches_for_day(day)
    logger.info("Команда /today: из API получено матчей: %s", len(matches))

    # 2. Достаём сохранённое состояние (для фильтров турниров)
    state = get_today_state(chat_id, day)
    if state:
        excluded = state.excluded_tournaments
        logger.info(
            "Команда /today: найдено сохранённое состояние (chat_id=%s, day=%s, excluded=%s)",
            chat_id,
            day,
            ", ".join(sorted(excluded)) if excluded else "-",
        )
    else:
        excluded = set()
        state = TodayMessageState(
            chat_id=chat_id,
            day=day,
            message_id=0,
            excluded_tournaments=excluded,
            last_text=None,
        )
        logger.info(
            "Команда /today: состояния не было, создаём новое (chat_id=%s, day=%s)",
            chat_id,
            day,
        )

    # 3. Применяем фильтр по турнирам
    if excluded:
        filtered_matches = [m for m in matches if m.tournament not in excluded]
    else:
        filtered_matches = matches

    # 4. Формируем текст
    now_msk = datetime.now(MSK_TZ)
    core = build_core_text(filtered_matches, day)
    text = make_full_text(core, now_msk)

    # 5. Клавиатура: фильтры + напоминания по будущим матчам
    keyboard = build_main_keyboard(
        filtered_matches=filtered_matches,
        all_matches=matches,
        excluded=excluded,
    )

    # 6. ВСЕГДА отправляем новое сообщение в ответ на /today
    sent: Message = await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    state.chat_id = sent.chat.id
    state.message_id = sent.message_id
    state.day = day
    logger.info(
        "Команда /today: отправили today-сообщение в чате %s (message_id=%s, day=%s)",
        sent.chat.id,
        sent.message_id,
        day,
    )

    # 7. Сохраняем текст и состояние
    state.last_text = text
    upsert_today_state(state)

    # 8. Следим, что поллер жив
    if poll_task is None or poll_task.done():
        logger.info("Поллер матчей ещё не запущен — стартуем фоновую задачу")
        poll_task = asyncio.create_task(poll_matches(message.bot))
    else:
        logger.info("Поллер матчей уже запущен, новую задачу не создаём")


@dp.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    chat_id = message.chat.id
    add_subscriber(chat_id)
    await message.answer("Вы подписаны на ежедневные уведомления о матчах (10:00 МСК).")


@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(message: Message):
    chat_id = message.chat.id
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM dota_bot_subscribers WHERE chat_id = %s;",
                (chat_id,),
            )
        conn.commit()
    await message.answer("Вы отписаны от ежедневных уведомлений.")

@dp.callback_query(F.data.startswith("filter:"))
async def callback_filter(callback: CallbackQuery):
    if not callback.message:
        return

    chat_id = callback.message.chat.id
    message_id = callback.message.message_id
    day = datetime.now(MSK_TZ).date()

    state = get_today_state(chat_id, day)
    if not state:
        state = TodayMessageState(
            chat_id=chat_id,
            day=day,
            message_id=message_id,
            excluded_tournaments=set(),
            last_text=callback.message.text,
        )

    try:
        idx = int(callback.data.split(":", 1)[1])
    except ValueError:
        try:
            await callback.answer("Некорректный фильтр", show_alert=True)
        except TelegramBadRequest as e:
            logger.warning("Не удалось ответить на старый callback: %s", e)
        return

    matches = await fetch_matches_for_day(day)
    tournaments = sorted({m.tournament for m in matches})
    if idx < 0 or idx >= len(tournaments):
        try:
            await callback.answer("Турнир не найден", show_alert=True)
        except TelegramBadRequest as e:
            logger.warning("Не удалось ответить на старый callback: %s", e)
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

    now_msk = datetime.now(MSK_TZ)
    core = build_core_text(filtered_matches, day)
    new_text = make_full_text(core, now_msk)

    # 👉 ТЕПЕРЬ: фильтры + напоминания, а не только фильтры
    keyboard = build_main_keyboard(
        filtered_matches=filtered_matches,
        all_matches=matches,
        excluded=state.excluded_tournaments,
    )

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
            "Фильтр турниров: обновили today-сообщение в чате %s (message_id=%s, day=%s, excluded=%s)",
            state.chat_id,
            state.message_id,
            state.day,
            ", ".join(sorted(state.excluded_tournaments)) if state.excluded_tournaments else "-",
        )
    except Exception as e:
        logger.warning(
            "Не удалось обновить today-сообщение по callback в чате %s: %s",
            chat_id,
            e,
        )

    # Пытаемся ответить на callback (чтобы не висел "часик")
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        logger.warning("Не удалось ответить на старый callback: %s", e)



# -------------------- Ежедневные уведомления --------------------

def was_daily_notification_sent(day: date) -> bool:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM dota_bot_daily_notifications WHERE day = %s;",
                (day,),
            )
            return cur.fetchone() is not None


def mark_daily_notification_sent(day: date):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dota_bot_daily_notifications (day)
                VALUES (%s)
                ON CONFLICT (day) DO NOTHING;
                """,
                (day,),
            )
        conn.commit()

async def reminders_notifier(bot: Bot) -> None:
    """
    Фоновый таск, который отправляет напоминания о матчах.
    Берёт из dota_bot_match_reminders записи, где sent_at IS NULL и remind_at <= now.
    """
    logger.info("Старт таска напоминаний о матчах")

    while True:
        try:
            await asyncio.sleep(20)  # частота проверки

            now_msk = datetime.now(MSK_TZ)

            # 1. Забираем napоминания, которые пора отправить
            with get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, chat_id, liquipedia_match_id, remind_at, title
                        FROM dota_bot_match_reminders
                        WHERE sent_at IS NULL
                          AND remind_at <= %s
                        ORDER BY remind_at ASC
                        LIMIT 50;
                        """,
                        (now_msk,),
                    )
                    rows = cur.fetchall()

            if not rows:
                continue

            for reminder_id, chat_id, match_key, remind_at, title in rows:
                # 2. Отправляем сообщение пользователю
                time_str = remind_at.astimezone(MSK_TZ).strftime("%H:%M")
                text = (
                    f"🔔 Не пропусти!\n"
                    f"{title}\n"
                    f"🕒 Начало в {time_str} (МСК)"
                )

                try:
                    await bot.send_message(chat_id=chat_id, text=text)
                    logger.info(
                        "Отправили напоминание %s в чат %s про %s (%s)",
                        reminder_id,
                        chat_id,
                        title,
                        match_key,
                    )
                except Exception as e:
                    logger.warning(
                        "Не удалось отправить напоминание %s в чат %s: %s",
                        reminder_id,
                        chat_id,
                        e,
                    )

                # 3. Помечаем напоминание как отправленное
                with get_db_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE dota_bot_match_reminders
                            SET sent_at = %s
                            WHERE id = %s;
                            """,
                            (now_msk, reminder_id),
                        )
                    conn.commit()

        except asyncio.CancelledError:
            logger.info("Таск напоминаний остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error("Ошибка в таске напоминаний: %s", e, exc_info=True)
            continue



async def daily_notifier(bot: Bot) -> None:
    logger.info("Старт ежедневного нотификатора (10:00 МСК, один раз в день)")

    while True:
        try:
            await asyncio.sleep(30)
            now_msk = datetime.now(MSK_TZ)
            today = now_msk.date()

            # Если на сегодня уже отправляли — просто ждём дальше
            if was_daily_notification_sent(today):
                continue

            # Разрешаем рассылку, как только наступило >= 10:00 МСК
            if now_msk.hour < 10:
                continue

            logger.info("Наступило время ежедневных уведомлений за день %s (МСК: %s)", today, now_msk)

            matches = await fetch_matches_for_day(today)
            logger.info("Ежедневный нотификатор: из API получено матчей: %s", len(matches))

            subs = get_all_subscribers()
            if not subs:
                logger.info("Подписчиков нет, рассылку за %s пропускаем", today)
                # Всё равно помечаем, что пытались/считали день обработанным,
                # чтобы не долбиться весь день без смысла
                mark_daily_notification_sent(today)
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

                if excluded:
                    filtered_matches = [m for m in matches if m.tournament not in excluded]
                else:
                    filtered_matches = matches

                core = build_core_text(filtered_matches, today)
                now_msk = datetime.now(MSK_TZ)
                text = make_full_text(core, now_msk)
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
                            today,
                        )
                    else:
                        sent: Message = await bot.send_message(
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

            # Если дошли сюда — считаем, что рассылка на сегодня отработала (или хотя бы попыталась)
            mark_daily_notification_sent(today)

        except asyncio.CancelledError:
            logger.info("Ежедневный нотификатор остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error("Ошибка в ежедневном нотификаторе: %s", e, exc_info=True)
            continue




async def main():
    global daily_task, poll_task
    logger.info("Запуск бота...")

    init_db()

    # Стартуем поллер и ежедневный нотификатор сразу
    poll_task = asyncio.create_task(poll_matches(bot))
    daily_task = asyncio.create_task(daily_notifier(bot))
    reminders_task = asyncio.create_task(reminders_notifier(bot))


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
        # Используем тот же event loop, который создали выше,
        # чтобы Dispatcher и фоновые задачи жили в одном цикле.
        loop.run_until_complete(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Завершение по сигналу KeyboardInterrupt/SystemExit")
