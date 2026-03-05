import asyncio
import contextlib
import json
import logging
import logging.handlers
import os
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Dict, Optional, List, Set, Any, Tuple
from collections import defaultdict
import html
import re
import secrets
from pathlib import Path

import aiohttp
import psycopg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python 3.8 и ниже


# -------------------- Настройки и логирование --------------------

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

# === DOTA API (backward compatible) ===
MATCHES_API_URL = os.getenv(
    "MATCHES_API_URL",
    "http://cyber-api.solar.shaneque.ru/dota/matches/today",
)

MATCHES_API_BASE_URL = os.getenv("MATCHES_API_BASE_URL")
if not MATCHES_API_BASE_URL:
    if MATCHES_API_URL.endswith("/today"):
        MATCHES_API_BASE_URL = MATCHES_API_URL.rsplit("/", 1)[0]
    else:
        MATCHES_API_BASE_URL = MATCHES_API_URL

# Рекомендуемые новые переменные:
DOTA_MATCHES_API_BASE_URL = os.getenv("DOTA_MATCHES_API_BASE_URL", MATCHES_API_BASE_URL)

# === CS2 API ===
CS2_MATCHES_API_BASE_URL = os.getenv("CS2_MATCHES_API_BASE_URL")
if not CS2_MATCHES_API_BASE_URL:
    CS2_MATCHES_API_BASE_URL = MATCHES_API_BASE_URL.replace("/dota/matches", "/cs2/matches")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

MSK_TZ = ZoneInfo("Europe/Moscow")

BASE_DIR = Path(__file__).resolve().parents[2]
LOG_DIR = str(BASE_DIR / "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = str(BASE_DIR / "logs" / "bot.log")

logger = logging.getLogger("matches_bot")
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
logger.info("DOTA_MATCHES_API_BASE_URL = %s", DOTA_MATCHES_API_BASE_URL)
logger.info("CS2_MATCHES_API_BASE_URL  = %s", CS2_MATCHES_API_BASE_URL)


# -------------------- Константы / типы --------------------

GAME_DOTA = "dota"
GAME_CS2 = "cs2"
GAMES = (GAME_DOTA, GAME_CS2)

UPDATED_MARKER = "\n\n🔄 Обновлено в "

REMIND_OFFSET_MINUTES = 0  # можно поставить 5/10 для заранее

_url_tail_re = re.compile(r"\s*\((https?://[^)]+)\)\s*$")


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
    team1_url: Optional[str] = None
    team2_url: Optional[str] = None
    liquipedia_match_id: Optional[str] = None


@dataclass
class TodayMessageState:
    chat_id: int
    day: date
    game: str
    message_id: int
    excluded_tournaments: Set[str]
    last_text: Optional[str]


poll_task: Optional[asyncio.Task] = None
daily_task: Optional[asyncio.Task] = None
reminders_task: Optional[asyncio.Task] = None

_matches_cache: Dict[Tuple[str, date], List["Match"]] = {}
_cache_lock = asyncio.Lock()


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
    """
    Универсальные таблицы (не dota_*).
    """
    logger.info("Инициализация БД...")
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            # подписчики + выбор игр
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS matches_bot_subscribers (
                    chat_id BIGINT PRIMARY KEY,
                    subscribe_dota BOOLEAN NOT NULL DEFAULT TRUE,
                    subscribe_cs2  BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            # состояния today сообщений (важно: (chat_id, day, game))
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS matches_bot_today_messages (
                    chat_id BIGINT NOT NULL,
                    day DATE NOT NULL,
                    game TEXT NOT NULL,
                    message_id BIGINT NOT NULL,
                    excluded_tournaments TEXT NOT NULL DEFAULT '',
                    last_text TEXT,
                    PRIMARY KEY (chat_id, day, game)
                );
                """
            )

            # отметка, что ежедневная рассылка по игре на день уже сделана
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS matches_bot_daily_notifications (
                    day DATE NOT NULL,
                    game TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (day, game)
                );
                """
            )

            # напоминания (универсальные)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS matches_bot_match_reminders (
                    id BIGSERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    game TEXT NOT NULL,
                    match_key TEXT NOT NULL,
                    remind_at TIMESTAMPTZ NOT NULL,
                    title TEXT NOT NULL,
                    sent_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (chat_id, game, match_key, remind_at)
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS matches_bot_match_reminders_due_idx
                ON matches_bot_match_reminders (sent_at, remind_at);
                """
            )

            # маппинг коротких callback_data -> payload (чтобы не ловить BUTTON_DATA_INVALID)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS matches_bot_callback_map (
                    id BIGSERIAL PRIMARY KEY,
                    cb_key TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS matches_bot_callback_map_created_idx
                ON matches_bot_callback_map (created_at);
                """
            )

        conn.commit()
    logger.info("БД и таблицы инициализированы.")


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
                INSERT INTO matches_bot_today_messages
                    (chat_id, day, game, message_id, excluded_tournaments, last_text)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (chat_id, day, game) DO UPDATE
                SET
                    message_id = EXCLUDED.message_id,
                    excluded_tournaments = EXCLUDED.excluded_tournaments,
                    last_text = EXCLUDED.last_text;
                """,
                (
                    state.chat_id,
                    state.day,
                    state.game,
                    state.message_id,
                    _serialize_excluded(state.excluded_tournaments),
                    state.last_text,
                ),
            )
        conn.commit()


def get_today_state(chat_id: int, day: date, game: str) -> Optional[TodayMessageState]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT message_id, excluded_tournaments, last_text
                FROM matches_bot_today_messages
                WHERE chat_id = %s AND day = %s AND game = %s;
                """,
                (chat_id, day, game),
            )
            row = cur.fetchone()
    if not row:
        return None

    message_id, excluded_raw, last_text = row
    return TodayMessageState(
        chat_id=chat_id,
        day=day,
        game=game,
        message_id=message_id,
        excluded_tournaments=_deserialize_excluded(excluded_raw),
        last_text=last_text,
    )


def get_all_today_states_for_day(day: date, game: str) -> List[TodayMessageState]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT chat_id, message_id, excluded_tournaments, last_text
                FROM matches_bot_today_messages
                WHERE day = %s AND game = %s;
                """,
                (day, game),
            )
            rows = cur.fetchall()

    result: List[TodayMessageState] = []
    for chat_id, message_id, excluded_raw, last_text in rows:
        result.append(
            TodayMessageState(
                chat_id=chat_id,
                day=day,
                game=game,
                message_id=message_id,
                excluded_tournaments=_deserialize_excluded(excluded_raw),
                last_text=last_text,
            )
        )
    return result


def delete_today_state(chat_id: int, day: date, game: str) -> None:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM matches_bot_today_messages
                WHERE chat_id = %s AND day = %s AND game = %s;
                """,
                (chat_id, day, game),
            )
        conn.commit()


def add_or_update_subscriber(chat_id: int, subscribe_dota: Optional[bool] = None, subscribe_cs2: Optional[bool] = None):
    """
    Upsert подписчика + (опционально) обновление выбора.
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO matches_bot_subscribers (chat_id)
                VALUES (%s)
                ON CONFLICT (chat_id) DO NOTHING;
                """,
                (chat_id,),
            )
            if subscribe_dota is not None:
                cur.execute(
                    "UPDATE matches_bot_subscribers SET subscribe_dota=%s WHERE chat_id=%s;",
                    (subscribe_dota, chat_id),
                )
            if subscribe_cs2 is not None:
                cur.execute(
                    "UPDATE matches_bot_subscribers SET subscribe_cs2=%s WHERE chat_id=%s;",
                    (subscribe_cs2, chat_id),
                )
        conn.commit()


def get_subscriber_prefs(chat_id: int) -> Tuple[bool, bool]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT subscribe_dota, subscribe_cs2 FROM matches_bot_subscribers WHERE chat_id=%s;",
                (chat_id,),
            )
            row = cur.fetchone()
    if not row:
        return True, False  # дефолт: Dota включена, CS2 выключен
    return bool(row[0]), bool(row[1])


def get_all_subscribers_with_prefs() -> List[Tuple[int, bool, bool]]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id, subscribe_dota, subscribe_cs2 FROM matches_bot_subscribers;")
            rows = cur.fetchall()
    return [(int(r[0]), bool(r[1]), bool(r[2])) for r in rows]


def was_daily_notification_sent(day: date, game: str) -> bool:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM matches_bot_daily_notifications WHERE day=%s AND game=%s;",
                (day, game),
            )
            return cur.fetchone() is not None


def mark_daily_notification_sent(day: date, game: str):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO matches_bot_daily_notifications (day, game)
                VALUES (%s, %s)
                ON CONFLICT (day, game) DO NOTHING;
                """,
                (day, game),
            )
        conn.commit()


def create_match_reminder(chat_id: int, game: str, match_key: str, remind_at: datetime, title: str) -> bool:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO matches_bot_match_reminders (
                        chat_id, game, match_key, remind_at, title
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (chat_id, game, match_key, remind_at) DO NOTHING;
                    """,
                    (chat_id, game, match_key, remind_at, title),
                )
                inserted = cur.rowcount > 0
            except Exception as e:
                logger.error("Ошибка при создании напоминания: %s", e, exc_info=True)
                conn.rollback()
                return False
        conn.commit()
    return inserted


# ---- callback map (короткие callback_data для remind) ----

def _gen_cb_key(prefix: str = "r_") -> str:
    # коротко и безопасно: r_xxxxxxxx...
    return prefix + secrets.token_urlsafe(8)

def save_callback_payload(cb_key: str, payload: dict) -> None:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO matches_bot_callback_map (cb_key, payload_json)
                VALUES (%s, %s)
                ON CONFLICT (cb_key) DO UPDATE SET payload_json = EXCLUDED.payload_json;
                """,
                (cb_key, json.dumps(payload, ensure_ascii=False)),
            )
        conn.commit()

def load_callback_payload(cb_key: str) -> Optional[dict]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload_json FROM matches_bot_callback_map WHERE cb_key=%s LIMIT 1;",
                (cb_key,),
            )
            row = cur.fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None


# -------------------- Вспомогательные функции матчей --------------------

def clean_team_name(s: str) -> str:
    if not s:
        return s
    return _url_tail_re.sub("", s).strip()


def _get_time_until(match_time_msk: datetime, now_msk: datetime) -> str:
    delta = match_time_msk - now_msk
    total_minutes = int(delta.total_seconds() / 60)

    if total_minutes < 60:
        return f"⚡ Через {total_minutes} мин ({match_time_msk.strftime('%H:%M')})"
    elif total_minutes < 1440:
        hours = total_minutes // 60
        mins = total_minutes % 60
        if mins > 0:
            return f"⏰ Через {hours} ч {mins} мин ({match_time_msk.strftime('%H:%M')})"
        return f"⏰ Через {hours} ч ({match_time_msk.strftime('%H:%M')})"
    else:
        return f"⏰ {match_time_msk.strftime('%H:%M')}"


def _pluralize_matches(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return f"{count} матч"
    elif 2 <= count % 10 <= 4 and (count % 100 < 10 or count % 100 >= 20):
        return f"{count} матча"
    else:
        return f"{count} матчей"


def _determine_winner(score: Optional[str]) -> int:
    if not score:
        return 0
    try:
        parts = score.replace(':', '-').split('-')
        if len(parts) == 2:
            score1, score2 = int(parts[0].strip()), int(parts[1].strip())
            if score1 > score2:
                return 1
            elif score2 > score1:
                return 2
    except Exception:
        pass
    return 0


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


def team_html(name: str, url: str | None) -> str:
    safe_name = html.escape(name or "")
    if url:
        safe_url = html.escape(url)
        return f'<a href="{safe_url}">{safe_name}</a>'
    return safe_name


# -------------------- HTTP (retry) --------------------

async def fetch_with_retry(
    url: str,
    max_retries: int = 3,
    timeout: int = 10,
    backoff_base: float = 2.0
) -> Optional[Any]:
    for attempt in range(max_retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=timeout) as resp:
                    # 404/400/401/403 — это “постоянные” ошибки, не ретраим
                    if 400 <= resp.status < 500:
                        text = await resp.text()
                        logger.error("HTTP %s (без retry) для %s. Body: %s", resp.status, url, text[:300])
                        return None

                    resp.raise_for_status()
                    return await resp.json()

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries:
                delay = backoff_base ** attempt
                logger.warning(
                    "Ошибка запроса (попытка %d/%d): %s. Повтор через %.1f сек.",
                    attempt + 1,
                    max_retries + 1,
                    e,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error("Все попытки исчерпаны для %s: %s", url, e, exc_info=True)
                return None


def build_matches_url_for_day(game: str, day: date) -> str:
    base = DOTA_MATCHES_API_BASE_URL if game == GAME_DOTA else CS2_MATCHES_API_BASE_URL
    base = (base or "").rstrip("/")  # 🔥 убираем хвостовые /
    return f"{base}/{day.strftime('%d-%m-%Y')}"


async def fetch_matches_for_day(game: str, day: date) -> List[Match]:
    """
    Потокобезопасная загрузка матчей из API с retry и кэшированием.
    При ошибке — возвращает кэш.
    """
    url = build_matches_url_for_day(game, day)
    logger.info("Запрос матчей из API: game=%s url=%s day=%s", game, url, day.isoformat())

    data = await fetch_with_retry(url, max_retries=3, timeout=10)
    if data is None:
        async with _cache_lock:
            key = (game, day)
            if key in _matches_cache:
                logger.info("Используем кэш для %s/%s", game, day)
                return _matches_cache[key]
        return []

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
                continue

            try:
                match_dt = datetime.fromisoformat(match_time_iso)
            except ValueError:
                match_dt = datetime.fromisoformat(match_time_iso.replace("Z", "+00:00"))

            result.append(
                Match(
                    match_time_msk=match_dt,
                    time_msk=raw.get("time_msk", ""),
                    team1=clean_team_name(fix_encoding(raw.get("team1", "")) or ""),
                    team2=clean_team_name(fix_encoding(raw.get("team2", "")) or ""),
                    bo=int(raw.get("bo", 0) or 0),
                    tournament=fix_encoding(raw.get("tournament", "")) or "",
                    status=raw.get("status", ""),
                    score=raw.get("score"),
                    team1_url=raw.get("team1_url"),
                    team2_url=raw.get("team2_url"),
                    liquipedia_match_id=raw.get("liquipedia_match_id"),
                )
            )

        result = deduplicate_matches(result)

        async with _cache_lock:
            _matches_cache[(game, day)] = result
            logger.info("Кэш обновлён: %s матчей для %s/%s", len(result), game, day)

        return result

    except Exception as e:
        logger.error("Ошибка парсинга для %s/%s: %s", game, day, e, exc_info=True)
        async with _cache_lock:
            key = (game, day)
            if key in _matches_cache:
                logger.info("Используем кэш после ошибки парсинга для %s/%s", game, day)
                return _matches_cache[key]
        return []


# -------------------- Форматирование сообщений (НЕ МЕНЯЛ) --------------------

def _format_match_line(m: Match, group: str, now_msk: Optional[datetime] = None) -> str:
    if m.team1_url:
        team1 = f'<a href="{m.team1_url}">{m.team1}</a>'
    else:
        team1 = m.team1 or "TBD"

    if m.team2_url:
        team2 = f'<a href="{m.team2_url}">{m.team2}</a>'
    else:
        team2 = m.team2 or "TBD"

    team1_bold = f"<b>{team1}</b>"
    team2_bold = f"<b>{team2}</b>"

    time_str = m.time_msk or m.match_time_msk.strftime("%H:%M")

    if group == "upcoming":
        if now_msk:
            time_display = _get_time_until(m.match_time_msk, now_msk)
        else:
            time_display = f"⏰ {time_str}"

        line1 = time_display
        parts = [f"{team1_bold} vs {team2_bold}"]
        if m.bo:
            parts.append(f"Bo{m.bo}")
        line2 = "   " + " • ".join(parts)

        if m.tournament:
            line3 = f"   📺 {m.tournament}"
            return f"{line1}\n{line2}\n{line3}"
        else:
            return f"{line1}\n{line2}"

    elif group == "live":
        line1 = f"🔴 {time_str}"

        if m.score:
            parts = [f"{team1_bold} {m.score} {team2_bold}"]
        else:
            parts = [f"{team1_bold} vs {team2_bold}"]

        if m.bo:
            parts.append(f"Bo{m.bo}")
        line2 = "   " + " • ".join(parts)

        if m.tournament:
            line3 = f"   📺 {m.tournament}"
            return f"{line1}\n{line2}\n{line3}"
        else:
            return f"{line1}\n{line2}"

    elif group == "finished":
        winner = _determine_winner(m.score)

        if winner == 1:
            line1 = f"🏆 {team1_bold} {m.score or '?:?'} {team2} ({time_str})"
        elif winner == 2:
            line1 = f"🏆 {team2_bold} {m.score or '?:?'} {team1} ({time_str})"
        else:
            line1 = f"⏰ {team1_bold} {m.score or '?:?'} {team2_bold} ({time_str})"

        if m.tournament:
            line2 = f"   📺 {m.tournament}"
            return f"{line1}\n{line2}"
        else:
            return line1

    else:
        parts = [f"⏰ {time_str}", f"{team1} vs {team2}"]
        if m.bo:
            parts.append(f"(Bo{m.bo})")
        if m.tournament:
            parts.append(f"[{m.tournament}]")
        if m.score:
            parts.append(m.score)
        return " ".join(parts)


def build_core_text(filtered_matches: List[Match], all_matches: List[Match], day: date, game: str) -> str:
    game_name = "Dota 2" if game == GAME_DOTA else "Counter-Strike 2" if game == GAME_CS2 else game.upper()
    game_emoji = "⚔️" if game == GAME_DOTA else "🔫" if game == GAME_CS2 else "🎮"

    now_msk = datetime.now(MSK_TZ)

    matches = filtered_matches  # use filtered

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
            if m.match_time_msk > now_msk:
                upcoming.append(m)

    live.sort(key=lambda m: m.match_time_msk)
    upcoming.sort(key=lambda m: m.match_time_msk)
    finished.sort(key=lambda m: m.match_time_msk)

    parts: List[str] = []
    separator = "━" * 14

    header = f"{game_emoji} {game_name} матчи на {day.strftime('%d.%m.%Y')} (МСК)"

    if not matches:
        parts.append(header)
        if not all_matches:
            # No matches at all for this day
            parts.append("❌ Нет матчей на этот день.")
        else:
            # Matches exist but hidden by filters
            parts.append("❌ Совпадений по выбранным фильтрам не найдено.")
            parts.append("")
            parts.append("💡 Используйте фильтры в рамках карточки ниже, чтобы настроить отображение турниров.")
        return "\n".join(parts).strip()

    parts.append(header)
    parts.append(separator)

    if live:
        live_header = f"🟢 LIVE • {_pluralize_matches(len(live))}"
        lines = [live_header] + [_format_match_line(m, "live", now_msk) for m in live]
        parts.append("\n".join(lines))

    if upcoming:
        if live:
            parts.append(separator)
        parts.append(f"⏰ Скоро начнутся • {_pluralize_matches(len(upcoming))}")
        tournaments: Dict[str, List[Match]] = defaultdict(list)
        for m in upcoming:
            tournaments[m.tournament or "Other"].append(m)

        for tournament, t_matches in sorted(tournaments.items(), key=lambda x: len(x[1]), reverse=True):
            parts.append(f"<b>{html.escape(tournament)}</b>  <i>({len(t_matches)})</i>")

            t_matches_sorted = sorted(t_matches, key=lambda mm: mm.match_time_msk)
            for m in t_matches_sorted:
                t1 = team_html(m.team1 or "TBD", m.team1_url)
                t2 = team_html(m.team2 or "TBD", m.team2_url)
                time_display = _get_time_until(m.match_time_msk, now_msk)
                bo_part = f" • Bo{m.bo}" if m.bo else ""
                parts.append(f"• {time_display} — {t1} vs {t2}{bo_part}")

            parts.append("")

    if finished:
        if live or upcoming:
            parts.append(separator)
        parts.append(f"✅ Завершённые • {_pluralize_matches(len(finished))}")
        tournaments: Dict[str, List[Match]] = defaultdict(list)
        for m in finished:
            tournaments[m.tournament or "Other"].append(m)

        for tournament, t_matches in sorted(tournaments.items(), key=lambda x: len(x[1]), reverse=True):
            parts.append(f"<b>{html.escape(tournament)}</b>  <i>({len(t_matches)})</i>")

            t_matches_sorted = sorted(t_matches, key=lambda mm: mm.match_time_msk)
            for m in t_matches_sorted:
                t1 = team_html(m.team1 or "TBD", m.team1_url)
                t2 = team_html(m.team2 or "TBD", m.team2_url)
                score = (m.score or "?:?").replace(":", "–")
                time_str = m.time_msk or m.match_time_msk.strftime("%H:%M")
                parts.append(f"• {t1} {score} {t2} <i>({html.escape(time_str)})</i>")

            parts.append("")

    total = len(live) + len(upcoming) + len(finished)
    parts.append(
        f"📊 Итого: {_pluralize_matches(total)} "
        f"(LIVE: {len(live)} • Скоро: {len(upcoming)} • Завершено: {len(finished)})"
    )

    return "\n".join(parts).strip()


def make_full_text(core: str, now_msk: datetime) -> str:
    return core + UPDATED_MARKER + now_msk.strftime("%H:%M")


def extract_core(text: Optional[str]) -> str:
    if not text:
        return ""
    idx = text.rfind(UPDATED_MARKER)
    if idx == -1:
        return text
    return text[:idx]


# -------------------- Клавиатуры: фильтры + напоминания --------------------

def build_tournaments_keyboard(matches: List[Match], excluded: Set[str], game: str) -> Optional[InlineKeyboardMarkup]:
    tournaments = sorted({m.tournament for m in matches})
    if not tournaments:
        return None

    rows = []
    for idx, t in enumerate(tournaments):
        hidden = t in excluded
        prefix = "🚫" if hidden else "✅"
        text = f"{prefix} {t}"
        rows.append([InlineKeyboardButton(text=text, callback_data=f"filter:{game}:{idx}")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_reminders_keyboard(matches: List[Match], game: str) -> InlineKeyboardMarkup:
    """
    ВАЖНО: callback_data <= 64 байт.
    Поэтому кладём в callback_data короткий cb_key, а реальные данные — в БД.
    """
    kb = InlineKeyboardBuilder()
    now_msk = datetime.now(MSK_TZ)

    for m in matches:
        status = (m.status or "").lower()
        if status in ("finished", "live"):
            continue
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

        cb_key = _gen_cb_key("r_")
        save_callback_payload(cb_key, {"game": game, "match_key": match_key})

        cb_data = f"remind:{cb_key}"  # коротко и валидно

        kb.row(InlineKeyboardButton(text=text[:64], callback_data=cb_data))

    return kb.as_markup()


def build_main_keyboard(filtered_matches: List[Match], all_matches: List[Match], excluded: Set[str], game: str) -> InlineKeyboardMarkup:
    filters_kb = build_tournaments_keyboard(all_matches, excluded, game)
    reminders_kb = build_reminders_keyboard(filtered_matches, game)

    rows: List[List[InlineKeyboardButton]] = []
    if filters_kb and filters_kb.inline_keyboard:
        rows.extend(filters_kb.inline_keyboard)
    if reminders_kb and reminders_kb.inline_keyboard:
        rows.extend(reminders_kb.inline_keyboard)

    return InlineKeyboardMarkup(inline_keyboard=rows)


# -------------------- Поллер: обновление сообщений --------------------

def _all_finished(matches: List[Match]) -> bool:
    for m in matches:
        if (m.status or "").lower() != "finished":
            return False
    return True


async def _update_today_states_for_day(bot: Bot, game: str, day: date, matches: List[Match]) -> None:
    states = get_all_today_states_for_day(day, game)
    if not states:
        return

    now_msk = datetime.now(MSK_TZ)
    today = now_msk.date()

    if day != today and _all_finished(matches):
        logger.info("День %s (%s) закрыт и не today — пропускаем апдейты", day, game)
        return

    logger.info("Поллер: обновляем %s сообщений для дня %s game=%s", len(states), day, game)

    for state in states:
        excluded = state.excluded_tournaments or set()
        filtered_matches = [m for m in matches if m.tournament not in excluded] if excluded else matches

        core = build_core_text(filtered_matches, matches, day, game)
        new_text = make_full_text(core, now_msk)

        keyboard = build_main_keyboard(
            filtered_matches=filtered_matches,
            all_matches=matches,
            excluded=excluded,
            game=game,
        )

        old_core = extract_core(state.last_text)

        if old_core == core:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=state.chat_id,
                    message_id=state.message_id,
                    reply_markup=keyboard,
                )
            except TelegramBadRequest as e:
                msg = str(e)
                if "message is not modified" in msg:
                    continue
                logger.warning("Не удалось обновить клавиатуру chat=%s day=%s game=%s: %s", state.chat_id, day, game, e)
                if "message to edit not found" in msg:
                    delete_today_state(state.chat_id, day, game)
            except Exception as e:
                logger.warning("Не удалось обновить клавиатуру chat=%s day=%s game=%s: %s", state.chat_id, day, game, e)
            continue

        try:
            await bot.edit_message_text(
                chat_id=state.chat_id,
                message_id=state.message_id,
                text=new_text,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
            state.last_text = new_text
            upsert_today_state(state)

        except TelegramBadRequest as e:
            msg = str(e)
            logger.warning("Не удалось обновить today-сообщение chat=%s day=%s game=%s: %s", state.chat_id, day, game, e)
            if "message to edit not found" in msg:
                delete_today_state(state.chat_id, day, game)
        except Exception as e:
            logger.warning("Не удалось обновить today-сообщение chat=%s day=%s game=%s: %s", state.chat_id, day, game, e)


async def poll_matches(bot: Bot) -> None:
    logger.info("Старт фонового поллера матчей")

    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

            today = datetime.now(MSK_TZ).date()
            yesterday = today - timedelta(days=1)

            for game in GAMES:
                today_matches = await fetch_matches_for_day(game, today)
                await _update_today_states_for_day(bot, game, today, today_matches)

                y_states = get_all_today_states_for_day(yesterday, game)
                if y_states:
                    y_matches = await fetch_matches_for_day(game, yesterday)
                    await _update_today_states_for_day(bot, game, yesterday, y_matches)

        except asyncio.CancelledError:
            logger.info("Поллер матчей остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error("Ошибка в поллере матчей: %s", e, exc_info=True)
            continue


# -------------------- Telegram-бот --------------------

try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()


def build_subscribe_keyboard(dota_on: bool, cs2_on: bool) -> InlineKeyboardMarkup:
    """
    Убрали кнопку "Dota2+CS2".
    Сделали нормальные галочки и toggle по нажатию.
    """
    kb = InlineKeyboardBuilder()

    dota_label = ("✅ " if dota_on else "⬜️ ") + "Dota2"
    cs2_label = ("✅ " if cs2_on else "⬜️ ") + "CS2"

    kb.add(
        InlineKeyboardButton(text=dota_label, callback_data="sub:toggle:dota"),
        InlineKeyboardButton(text=cs2_label, callback_data="sub:toggle:cs2"),
    )
    kb.add(
        InlineKeyboardButton(text="🚫 Отключить всё", callback_data="sub:none"),
    )
    return kb.as_markup()


@dp.message(Command("start"))
async def cmd_start(message: Message):
    chat_id = message.chat.id
    add_or_update_subscriber(chat_id)  # upsert

    dota_on, cs2_on = get_subscriber_prefs(chat_id)

    await message.answer(
        "👋 Привет! Я бот с расписанием матчей.\n\n"
        "Сначала выбери, на что подписаться:\n"
        "— Dota2\n"
        "— CS2\n\n"
        "Команды:\n"
        "/today — матчи на сегодня (по твоим подпискам)\n"
        "/subscribe — изменить подписку\n"
        "/unsubscribe — отписаться от всего\n"
        "/help — справка\n",
        reply_markup=build_subscribe_keyboard(dota_on, cs2_on),
        disable_web_page_preview=True,
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "Я показываю матчи Dota2 и CS2.\n\n"
        "Команды:\n"
        "/today — матчи на сегодня (по выбранным подпискам)\n"
        "/subscribe — выбрать Dota2 / CS2 / оба\n"
        "/unsubscribe — отключить всё\n\n"
        "Под сообщением:\n"
        "— фильтры турниров\n"
        "— кнопки 🔔 Напомнить для будущих матчей\n",
        disable_web_page_preview=True,
    )


@dp.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    chat_id = message.chat.id
    add_or_update_subscriber(chat_id)
    dota_on, cs2_on = get_subscriber_prefs(chat_id)

    await message.answer(
        "Выбери, куда подписываться:",
        reply_markup=build_subscribe_keyboard(dota_on, cs2_on),
    )


@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(message: Message):
    chat_id = message.chat.id
    add_or_update_subscriber(chat_id, subscribe_dota=False, subscribe_cs2=False)
    await message.answer("Ок, отключил все подписки ✅")


@dp.callback_query(F.data.startswith("sub:"))
async def callback_subscribe(call: CallbackQuery):
    if not call.message:
        return

    chat_id = call.message.chat.id
    data = call.data or ""

    add_or_update_subscriber(chat_id)  # upsert гарантированно
    dota_on, cs2_on = get_subscriber_prefs(chat_id)

    if data == "sub:none":
        dota_on, cs2_on = False, False
        add_or_update_subscriber(chat_id, subscribe_dota=False, subscribe_cs2=False)

    else:
        # sub:toggle:<game>
        try:
            _, action, which = data.split(":", 2)
            if action != "toggle":
                raise ValueError("bad action")
            if which == "dota":
                dota_on = not dota_on
                add_or_update_subscriber(chat_id, subscribe_dota=dota_on)
            elif which == "cs2":
                cs2_on = not cs2_on
                add_or_update_subscriber(chat_id, subscribe_cs2=cs2_on)
            else:
                raise ValueError("bad which")
        except Exception:
            try:
                await call.answer("Некорректная кнопка 🤔", show_alert=True)
            except TelegramBadRequest:
                pass
            return

    # обновляем клавиатуру
    try:
        await call.message.edit_reply_markup(reply_markup=build_subscribe_keyboard(dota_on, cs2_on))
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.warning("Не удалось обновить клавиатуру подписки: %s", e)

    try:
        await call.answer("Готово ✅", show_alert=False)
    except TelegramBadRequest:
        pass


@dp.message(Command("today"))
async def cmd_today(message: Message):
    """
    /today — отправляет сообщения по выбранным подпискам.
    Формат сообщений оставлен прежним.
    """
    global poll_task

    chat_id = message.chat.id
    add_or_update_subscriber(chat_id)

    dota_on, cs2_on = get_subscriber_prefs(chat_id)
    day = datetime.now(MSK_TZ).date()

    chosen_games: List[str] = []
    if dota_on:
        chosen_games.append(GAME_DOTA)
    if cs2_on:
        chosen_games.append(GAME_CS2)

    if not chosen_games:
        await message.answer(
            "У тебя выключены подписки 😅\n\nВыбери Dota2/CS2 через /subscribe",
            reply_markup=build_subscribe_keyboard(dota_on, cs2_on),
        )
        return

    for game in chosen_games:
        matches = await fetch_matches_for_day(game, day)

        state = get_today_state(chat_id, day, game)
        if state:
            excluded = state.excluded_tournaments
        else:
            excluded = set()
            state = TodayMessageState(
                chat_id=chat_id,
                day=day,
                game=game,
                message_id=0,
                excluded_tournaments=excluded,
                last_text=None,
            )

        filtered_matches = [m for m in matches if m.tournament not in excluded] if excluded else matches

        now_msk = datetime.now(MSK_TZ)
        core = build_core_text(filtered_matches, matches, day, game)
        text = make_full_text(core, now_msk)

        keyboard = build_main_keyboard(
            filtered_matches=filtered_matches,
            all_matches=matches,
            excluded=excluded,
            game=game,
        )

        sent: Message = await message.answer(
            text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )

        state.message_id = sent.message_id
        state.last_text = text
        upsert_today_state(state)

    if poll_task is None or poll_task.done():
        poll_task = asyncio.create_task(poll_matches(message.bot))


@dp.callback_query(F.data.startswith("filter:"))
async def callback_filter(callback: CallbackQuery):
    if not callback.message:
        return

    chat_id = callback.message.chat.id
    day = datetime.now(MSK_TZ).date()

    # filter:<game>:<idx>
    try:
        _, game, idx_s = (callback.data or "").split(":", 2)
        idx = int(idx_s)
        if game not in GAMES:
            raise ValueError("bad game")
    except Exception:
        try:
            await callback.answer("Некорректный фильтр", show_alert=True)
        except TelegramBadRequest:
            pass
        return

    state = get_today_state(chat_id, day, game)
    if not state:
        state = TodayMessageState(
            chat_id=chat_id,
            day=day,
            game=game,
            message_id=callback.message.message_id,
            excluded_tournaments=set(),
            last_text=callback.message.text,
        )

    matches = await fetch_matches_for_day(game, day)
    tournaments = sorted({m.tournament for m in matches})
    if idx < 0 or idx >= len(tournaments):
        try:
            await callback.answer("Турнир не найден", show_alert=True)
        except TelegramBadRequest:
            pass
        return

    tournament_name = tournaments[idx]
    if tournament_name in state.excluded_tournaments:
        state.excluded_tournaments.remove(tournament_name)
    else:
        state.excluded_tournaments.add(tournament_name)

    filtered_matches = [m for m in matches if m.tournament not in state.excluded_tournaments]

    now_msk = datetime.now(MSK_TZ)
    core = build_core_text(filtered_matches, matches, day, game)
    new_text = make_full_text(core, now_msk)

    keyboard = build_main_keyboard(
        filtered_matches=filtered_matches,
        all_matches=matches,
        excluded=state.excluded_tournaments,
        game=game,
    )

    try:
        await callback.message.edit_text(
            new_text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        state.last_text = new_text
        state.message_id = callback.message.message_id
        upsert_today_state(state)
    except Exception as e:
        logger.warning("Не удалось обновить today-сообщение по filter callback: %s", e)

    try:
        await callback.answer()
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data.startswith("remind:"))
async def callback_remind_match(call: CallbackQuery):
    """
    remind:<cb_key>
    payload в БД: {game, match_key}
    """
    if not call.message:
        try:
            await call.answer("Что-то пошло не так 🤔", show_alert=True)
        except TelegramBadRequest:
            pass
        return

    chat_id = call.message.chat.id
    data = call.data or ""

    try:
        _, cb_key = data.split(":", 1)
        cb_key = cb_key.strip()
    except Exception:
        try:
            await call.answer("Не понял кнопку 🤔", show_alert=True)
        except TelegramBadRequest:
            pass
        return

    payload = load_callback_payload(cb_key)
    if not payload:
        try:
            await call.answer("Кнопка устарела — обнови /today 🔄", show_alert=True)
        except TelegramBadRequest:
            pass
        return

    game = (payload.get("game") or "").strip()
    match_key = (payload.get("match_key") or "").strip()
    if game not in GAMES or not match_key:
        try:
            await call.answer("Кнопка кривая — обнови /today 😅", show_alert=True)
        except TelegramBadRequest:
            pass
        return

    text = call.message.text or ""
    day = datetime.now(MSK_TZ).date()
    try:
        m = re.search(r"Матчи на (\d{2}\.\d{2}\.\d{4})", text)
        if m:
            day_str = m.group(1)
            day = datetime.strptime(day_str, "%d.%m.%Y").date()
    except Exception:
        pass

    matches = await fetch_matches_for_day(game, day)
    match = next((m for m in matches if m.liquipedia_match_id == match_key), None)

    if not match:
        try:
            await call.answer("Не удалось найти матч для напоминания 😢", show_alert=True)
        except TelegramBadRequest:
            pass
        return

    remind_at = match.match_time_msk - timedelta(minutes=REMIND_OFFSET_MINUTES)

    if match.team1 and match.team2:
        title = f"{match.team1} vs {match.team2}"
    elif match.team1 or match.team2:
        title = match.team1 or match.team2
    else:
        title = match.tournament or "матч"

    created = create_match_reminder(
        chat_id=chat_id,
        game=game,
        match_key=match_key,
        remind_at=remind_at,
        title=title,
    )

    time_str = remind_at.strftime("%H:%M")
    msg = f"Ок, напомню в {time_str} про {title} 🔔" if created else "Такое напоминание уже стоит ✅"

    try:
        await call.answer(msg, show_alert=True)
    except TelegramBadRequest:
        pass


# -------------------- Напоминания --------------------

async def reminders_notifier(bot: Bot) -> None:
    logger.info("Старт таска напоминаний о матчах")

    while True:
        try:
            await asyncio.sleep(20)

            now_msk = datetime.now(MSK_TZ)

            with get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, chat_id, game, match_key, remind_at, title
                        FROM matches_bot_match_reminders
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

            for reminder_id, chat_id, game, match_key, remind_at, title in rows:
                time_str = remind_at.astimezone(MSK_TZ).strftime("%H:%M")
                text = (
                    f"🔔 Не пропусти!\n"
                    f"{title}\n"
                    f"🕒 Начало в {time_str} (МСК)"
                )

                try:
                    await bot.send_message(chat_id=chat_id, text=text)
                    logger.info("Отправили напоминание id=%s chat=%s game=%s %s", reminder_id, chat_id, game, match_key)
                except Exception as e:
                    logger.warning("Не удалось отправить напоминание %s в чат %s: %s", reminder_id, chat_id, e)

                with get_db_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE matches_bot_match_reminders
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


# -------------------- Ежедневные уведомления --------------------

async def daily_notifier(bot: Bot) -> None:
    logger.info("Старт ежедневного нотификатора (10:00 МСК, один раз в день)")

    while True:
        try:
            await asyncio.sleep(30)
            now_msk = datetime.now(MSK_TZ)
            today = now_msk.date()

            if now_msk.hour < 10:
                continue

            subs = get_all_subscribers_with_prefs()
            if not subs:
                continue

            for game in GAMES:
                if was_daily_notification_sent(today, game):
                    continue

                targets = []
                for chat_id, dota_on, cs2_on in subs:
                    if game == GAME_DOTA and dota_on:
                        targets.append(chat_id)
                    if game == GAME_CS2 and cs2_on:
                        targets.append(chat_id)

                if not targets:
                    mark_daily_notification_sent(today, game)
                    continue

                matches = await fetch_matches_for_day(game, today)

                for chat_id in targets:
                    state = get_today_state(chat_id, today, game)
                    if state:
                        excluded = state.excluded_tournaments
                    else:
                        excluded = set()
                        state = TodayMessageState(
                            chat_id=chat_id,
                            day=today,
                            game=game,
                            message_id=0,
                            excluded_tournaments=excluded,
                            last_text=None,
                        )

                    filtered_matches = [m for m in matches if m.tournament not in excluded] if excluded else matches

                    core = build_core_text(filtered_matches, matches, today, game)
                    text = make_full_text(core, datetime.now(MSK_TZ))

                    keyboard = build_main_keyboard(
                        filtered_matches=filtered_matches,
                        all_matches=matches,
                        excluded=excluded,
                        game=game,
                    )

                    try:
                        sent: Message = await bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode="HTML",
                            reply_markup=keyboard,
                            disable_web_page_preview=True,
                        )
                        state.message_id = sent.message_id
                        state.last_text = text
                        upsert_today_state(state)
                    except Exception as e:
                        logger.warning("Не удалось отправить ежедневное уведомление chat=%s game=%s: %s", chat_id, game, e)

                mark_daily_notification_sent(today, game)

        except asyncio.CancelledError:
            logger.info("Ежедневный нотификатор остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error("Ошибка в ежедневном нотификаторе: %s", e, exc_info=True)
            continue


# -------------------- main --------------------

async def main():
    global daily_task, poll_task, reminders_task
    logger.info("Запуск бота...")

    init_db()

    poll_task = asyncio.create_task(poll_matches(bot))
    daily_task = asyncio.create_task(daily_notifier(bot))
    reminders_task = asyncio.create_task(reminders_notifier(bot))

    try:
        await dp.start_polling(bot)
    finally:
        logger.info("Shutdown: cancelling background tasks...")

        tasks_to_cancel = [
            ("poll_task", poll_task),
            ("daily_task", daily_task),
            ("reminders_task", reminders_task),
        ]

        for task_name, task in tasks_to_cancel:
            if task and not task.done():
                logger.info("Cancelling %s...", task_name)
                task.cancel()

        await asyncio.sleep(0.5)

        for task_name, task in tasks_to_cancel:
            if task and not task.done():
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                    logger.info("%s stopped cleanly", task_name)
                except asyncio.TimeoutError:
                    logger.warning("%s didn't stop within timeout", task_name)
                except asyncio.CancelledError:
                    logger.info("%s cancelled", task_name)

        logger.info("Бот остановлен")


def run() -> None:
    try:
        loop.run_until_complete(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Завершение по сигналу KeyboardInterrupt/SystemExit")


if __name__ == "__main__":
    run()
