#!/usr/bin/env python3
"""
main.py — парсер матчей Liquipedia с:
- нормальным match_uid по Liquipedia Match:ID;
- миграцией старых матчей на новый UID;
- обновлением счёта по страницам матчей и вкладке Completed;
- кэшем турниров со slug / status;
- обновлением статусов матчей по времени.
"""

from __future__ import annotations
try:
    from zoneinfo import ZoneInfo  # для Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # для Python 3.8

import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple

import psycopg
from psycopg import errors
import requests
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv
from urllib.parse import urljoin

# ---------------------------------------------------------------------------
# ЛОГИ
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "parser.log")

# Настройка логирования с ротацией
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        # Console handler
        logging.StreamHandler(),
        # File handler с ротацией: макс 10MB на файл, хранить 5 бэкапов
        RotatingFileHandler(
            LOG_FILE,
            maxBytes=10_000_000,  # 10 MB
            backupCount=5,
            encoding="utf-8"
        )
    ]
)
logger = logging.getLogger(__name__)


def log_event(event: dict):
    """Log structured event as JSON."""
    event["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = json.dumps(event, ensure_ascii=False)
    logger.info(line)


# ---------------------------------------------------------------------------
# НАСТРОЙКИ И ОКРУЖЕНИЕ
# ---------------------------------------------------------------------------

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

SCRAPE_INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "600"))  # 10 минут по умолчанию

BASE_URL = "https://liquipedia.net"
MATCHES_URL = f"{BASE_URL}/dota2/Liquipedia:Matches"
MAIN_PAGE_URL = f"{BASE_URL}/dota2/Main_Page"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
}

MONTHS: dict[str, int] = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}

# Timezone mapping к IANA timezone names (автоматически обрабатывает DST)
TZ_IANA_MAP = {
    "MSK": "Europe/Moscow",      # Handles any future DST changes
    "CET": "Europe/Berlin",      # Auto-switches CET ↔ CEST
    "CEST": "Europe/Berlin",
    "EET": "Europe/Athens",      # Auto-switches EET ↔ EEST
    "EEST": "Europe/Athens",
    "SGT": "Asia/Singapore",
    "HKT": "Asia/Hong_Kong",
    "CST": "Asia/Shanghai",      # China Standard Time
    "KST": "Asia/Seoul",
    "JST": "Asia/Tokyo",
    "IST": "Asia/Kolkata",       # India (no DST)
    "PET": "America/Lima",       # Peru (no DST)
    "GST": "Asia/Dubai",         # Gulf Standard Time
    "UTC": "UTC",
    "GMT": "UTC",
}

MSK_TZ = ZoneInfo("Europe/Moscow")

# ---------------------------------------------------------------------------
# МОДЕЛИ
# ---------------------------------------------------------------------------

@dataclass
class Match:
    time_msk: Optional[datetime]
    time_raw: Optional[str]
    team1: Optional[str]
    team2: Optional[str]
    score: Optional[str]
    bo: Optional[str]
    tournament: Optional[str]
    status: Optional[str]
    match_url: Optional[str]  # URL страницы матча на Liquipedia


@dataclass
class Tournament:
    slug: str   # "/dota2/BLAST/Slam/5"
    name: str   # "BLAST Slam V"
    status: str # "upcoming" | "ongoing" | "completed" | "unknown"
    url: str    # полный URL


# кэш турниров по "очищенному" имени
KNOWN_TOURNAMENTS_BY_NAME: Dict[str, Tournament] = {}


# ---------------------------------------------------------------------------
# УТИЛИТЫ
# ---------------------------------------------------------------------------
def _strip_page_does_not_exist(name: str) -> str:
    """
    Убираем суффикс ' (page does not exist)' в конце строки, если он есть.
    """
    if not name:
        return ""
    # режем только в конце, чтобы не сносить похожие по смыслу части в середине
    return re.sub(r"\s*\(page does not exist\)\s*$", "", name).strip()


def extract_team_name_from_tag(tag: Tag) -> str:
    """
    Берём нормальное имя команды:
    - сначала из title, но без ' (page does not exist)'
    - если нет title — из текста ссылки, тоже без суффикса
    """
    if not tag:
        return ""

    title = tag.get("title")
    if title:
        clean = _strip_page_does_not_exist(title)
        if clean:
            return clean

    text = tag.get_text(strip=True)
    return _strip_page_does_not_exist(text)

def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def get_db_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def normalize_team_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    return name.strip()


def parse_time_to_msk(time_str: str) -> Optional[datetime]:
    """
    Более терпимый парсер строк вида:
      "December 4, 2025 - 14:00 CET"
      "January 8, 2026 - 13:00IST"
    (в том числе, если там были <abbr> и лишние пробелы).

    Возвращает datetime в зоне MSK (UTC+3) или None при ошибке.
    """
    if not time_str:
        return None

    # убираем HTML-теги и лишние пробелы
    cleaned = re.sub(r"<.*?>", "", time_str)
    cleaned = " ".join(cleaned.split())

    m = re.search(
        r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})\s*-\s*"
        r"(\d{1,2}):(\d{2})\s*([A-Z]{2,4})",
        cleaned,
    )
    if not m:
        logger.warning("parse_time_to_msk: не удалось распарсить '%s'", time_str)
        return None

    month_name, day, year, hour, minute, tz_abbr = m.groups()

    month = MONTHS.get(month_name)
    if not month:
        logger.warning(
            "parse_time_to_msk: неизвестный месяц '%s' в строке '%s'",
            month_name,
            time_str,
        )
        return None

    try:
        dt_naive = datetime(
            int(year),
            month,
            int(day),
            int(hour),
            int(minute),
        )
    except ValueError as e:
        logger.warning(
            "parse_time_to_msk: ошибка при создании datetime из '%s': %s",
            time_str,
            e,
        )
        return None

    # Get IANA timezone name
    tz_name = TZ_IANA_MAP.get(tz_abbr)
    if not tz_name:
        logger.warning("Неизвестная timezone '%s' в '%s', используем UTC", tz_abbr, time_str)
        tz_name = "UTC"

    # Localize to source timezone (handles DST automatically)
    try:
        src_tz = ZoneInfo(tz_name)
        dt_src = dt_naive.replace(tzinfo=src_tz)

        # Convert to MSK
        dt_msk = dt_src.astimezone(MSK_TZ)
        return dt_msk

    except Exception as e:
        logger.error("Timezone conversion failed for '%s': %s", time_str, e)
        return None


def parse_bo_int(bo: Optional[str]) -> Optional[int]:
    """
    'Bo3' -> 3
    'Bo1' -> 1
    '(Bo3)' -> 3
    None  -> None
    """
    if not bo:
        return None
    m = re.search(r"Bo\s*?(\d+)", bo)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def clean_tournament_name(tournament_name: str) -> str:
    """
    Очистка названия турнира от суффиксов:
      "CCT S2 Series 6 - Playoffs" -> "CCT S2 Series 6"
      "BLAST Slam V - November 29-A" -> "BLAST Slam V"
      "PGL Wallachia S6 - Group B" -> "PGL Wallachia S6"
    """
    if not tournament_name:
        return tournament_name

    cleaned = re.split(
        r"\s*-\s*(?:Playoffs?|Groups?|Group\s+[A-Z]|November\s+\d+-[A-Z]|December\s+\d+-[A-Z]|Play-In|Qualifier[s]?)",
        tournament_name,
        maxsplit=1,
    )[0]

    return cleaned.strip()

def auto_repair_matches() -> None:
    """
    Автоматический ремонт таблицы dota_matches.

    Делает:
      1) Удаляет строки без match_uid (старый/битый мусор).
      2) Удаляет TBD-плейсхолдеры, если в том же слоте уже есть матч с реальными командами.
      3) Чинит странные finished-матчи без команд или с мусорным счётом.
      4) Проставляет liquipedia_match_id там, где его можно вывести из match_uid / match_url.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 1) Удаляем строки без match_uid (то, что ты уже делал руками)
            cur.execute(
                """
                DELETE FROM dota_matches
                WHERE match_uid IS NULL OR match_uid = '';
                """
            )
            deleted_no_uid = cur.rowcount

            # 2) Удаляем TBD-плейсхолдеры,
            #    если в этом же слоте (время+турнир) уже есть матч с нормальными командами
            cur.execute(
                """
                DELETE FROM dota_matches d
                WHERE (d.team1 = 'TBD' OR d.team2 = 'TBD')
                  AND EXISTS (
                      SELECT 1
                      FROM dota_matches d2
                      WHERE d2.id <> d.id
                        AND d2.match_time_msk = d.match_time_msk
                        AND COALESCE(LOWER(d2.tournament), '') = COALESCE(LOWER(d.tournament), '')
                        AND d2.team1 <> 'TBD'
                        AND d2.team2 <> 'TBD'
                  );
                """
            )
            deleted_tbd = cur.rowcount

            # 3а) Нормализуем законченные матчи без команд: считаем их ещё не валидными
            cur.execute(
                """
                UPDATE dota_matches
                SET status = 'unknown',
                    score  = NULL,
                    updated_at = now()
                WHERE status = 'finished'
                  AND (team1 IS NULL OR team1 = '')
                  AND (team2 IS NULL OR team2 = '');
                """
            )
            fixed_finished_no_teams = cur.rowcount

            # 3б) Finished + счёт 0:0 тоже считаем подозрительным
            cur.execute(
                """
                UPDATE dota_matches
                SET status = 'unknown',
                    score  = NULL,
                    updated_at = now()
                WHERE status = 'finished'
                  AND score = '0:0';
                """
            )
            fixed_finished_zero_zero = cur.rowcount

            # 4а) Проставляем liquipedia_match_id из match_uid формата "lp:ID_xxx"
            cur.execute(
                """
                UPDATE dota_matches
                SET liquipedia_match_id = substring(match_uid FROM '^lp:(ID_[^|]+)')
                WHERE liquipedia_match_id IS NULL
                  AND match_uid LIKE 'lp:ID_%';
                """
            )
            updated_from_uid = cur.rowcount

            # 4б) Проставляем liquipedia_match_id из match_url (если есть Match:ID_xxx)
            cur.execute(
                """
                UPDATE dota_matches
                SET liquipedia_match_id = substring(match_url FROM 'Match:(ID_[^&#/?]+)')
                WHERE liquipedia_match_id IS NULL
                  AND match_url LIKE '%Match:ID_%';
                """
            )
            updated_from_url = cur.rowcount

        conn.commit()

    print(
        f"[AUTO-REPAIR] deleted_no_uid={deleted_no_uid}, "
        f"deleted_tbd={deleted_tbd}, "
        f"fixed_finished_no_teams={fixed_finished_no_teams}, "
        f"fixed_finished_zero_zero={fixed_finished_zero_zero}, "
        f"liqui_from_uid={updated_from_uid}, "
        f"liqui_from_url={updated_from_url}"
    )

import re
from bs4 import BeautifulSoup, Tag

SCORE_RE = re.compile(r'(\d+)\s*[:\-]\s*(\d+)')
BO_RE = re.compile(r'\(Bo\s*([0-9]+)\)', re.IGNORECASE)


def parse_score_and_bo_from_container(container: Tag) -> tuple[Optional[str], Optional[str]]:
    """
    Универсальный парсер счёта и Bo по тексту всего контейнера матча.

    Примеры входного текста:
      "Bsb 0 : 1 (Bo3) L1GA"
      "Nemiga 2:0 Lynx (Bo3)"
      "Team A 1-2 Team B (bo5)"
    Возвращает:
      score: строка вида "0:1"
      bo: строка вида "Bo3" (дальше превратится в int через parse_bo_int)
    """
    text = " ".join(container.stripped_strings)
    if not text:
        return None, None

    score: Optional[str] = None
    bo_text: Optional[str] = None

    # Ищем счёт
    m_score = SCORE_RE.search(text)
    if m_score:
        try:
            left = int(m_score.group(1))
            right = int(m_score.group(2))
        except ValueError:
            left = right = None
        else:
            # Dota-ограничение: нормальный счёт не бывает 2025:14 и т.п.
            if 0 <= left <= 10 and 0 <= right <= 10:
                score = f"{left}:{right}"

    # Ищем Bo
    m_bo = BO_RE.search(text)
    if m_bo:
        try:
            bo_num = int(m_bo.group(1))
            bo_text = f"Bo{bo_num}"
        except ValueError:
            bo_text = None

    return score, bo_text


TZ_OFFSETS = {
    "CET":  1 * 60,   # UTC+1
    "CEST": 2 * 60,   # UTC+2
    "MSK":  3 * 60,   # UTC+3
    "CST":  8 * 60,   # тут именно China Standard Time
    "SGT":  8 * 60,   # Singapore Time
    "IST":  5 * 60 + 30,  # India
}

MSK_OFFSET_MIN = 3 * 60  # UTC+3


TZ_MAP = {
    "CET": ZoneInfo("Europe/Berlin"),     # Liquipedia так любит
    "CEST": ZoneInfo("Europe/Berlin"),
    "CST": ZoneInfo("Asia/Shanghai"),    # для Kaixi Cup, China
    "SGT": ZoneInfo("Asia/Singapore"),
    "MSK": ZoneInfo("Europe/Moscow"),
}

MSK_TZ = ZoneInfo("Europe/Moscow")
UTC_TZ = ZoneInfo("UTC")

TIME_RE = re.compile(
    r"^(?P<month>[A-Za-z]+)\s+"
    r"(?P<day>\d{1,2}),\s+"
    r"(?P<year>\d{4})\s*-\s*"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*"
    r"(?P<tz>[A-Z]{2,4})$"
)



def parse_liquipedia_time(raw: str) -> tuple[datetime | None, datetime | None]:
    """
    Разбор строки времени с Liquipedia вида:
      'December 6, 2025 - 13:40CET'
      'January 15, 2026 - 10:00BRT'
      'January 8, 2026 - 13:00IST'

    Возвращает:
      (dt_utc, dt_msk) или (None, None) при ошибке.
    """
    raw = (raw or "").strip()
    if not raw:
        return None, None

    # Пример строки: "December 6, 2025 - 13:40CET"
    m = re.match(r"^(.*?\d{4})\s*-\s*(\d{2}:\d{2})([A-Z]+)$", raw)
    if not m:
        logger.warning("parse_liquipedia_time: не смогли распарсить строку '%s'", raw)
        return None, None

    date_part, time_part, tz_abbr = m.groups()
    tz_abbr = tz_abbr.strip()

    # Маппинг аббревиатур в реальные таймзоны
    tz_map = {
        "CET": "Europe/Berlin",
        "CEST": "Europe/Berlin",
        "MSK": "Europe/Moscow",
        "SGT": "Asia/Singapore",
        "CST": "Asia/Shanghai",       # Liquipedia часто так помечает китайское время
        "EET": "Europe/Bucharest",
        "BRT": "America/Sao_Paulo",   # Brazil Time, UTC-3
        "IST": "Asia/Kolkata",        # India Standard Time, UTC+5:30
        "GST": "Asia/Dubai",          # Gulf Standard Time, UTC+4
    }

    tz_name = tz_map.get(tz_abbr)
    if not tz_name:
        logger.warning(
            "parse_liquipedia_time: неизвестный таймзон '%s' в строке '%s'",
            tz_abbr,
            raw,
        )
        return None, None

    try:
        naive = datetime.strptime(f"{date_part} {time_part}", "%B %d, %Y %H:%M")
    except ValueError:
        logger.warning("parse_liquipedia_time: ошибка парсинга даты/времени в '%s'", raw)
        return None, None

    # локальное время в таймзоне матча
    dt_local = naive.replace(tzinfo=ZoneInfo(tz_name))

    # время в UTC
    dt_utc = dt_local.astimezone(ZoneInfo("UTC"))

    # время в MSK (для match_time_msk)
    dt_msk = dt_local.astimezone(ZoneInfo("Europe/Moscow"))

    return dt_utc, dt_msk

# ---------------------------------------------------------------------------
# ТУРНИРЫ
# ---------------------------------------------------------------------------

def parse_tournaments_from_main(html: str) -> List[Tournament]:
    """
    Примерная логика:
    - на главной странице турниры сгруппированы под заголовками:
      "Ongoing", "Upcoming & Qualifiers", "Recent Results" и т.п.
    - мы пытаемся по тексту заголовков понять статус, затем забрать <ul> ниже.
    """
    soup = BeautifulSoup(html, "html.parser")
    result: List[Tournament] = []

    status_map: Dict[str, str] = {
        "ongoing": "ongoing",
        "upcoming": "upcoming",
        "qualifier": "upcoming",
        "recent": "completed",
        "completed": "completed",
    }

    for header in soup.find_all(["h2", "h3"]):
        htext = header.get_text(strip=True).lower()
        status = None
        for key, val in status_map.items():
            if key in htext:
                status = val
                break
        if not status:
            continue

        ul = header.find_next("ul")
        if not ul:
            continue

        for a in ul.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("/dota2/"):
                continue
            name = a.get_text(strip=True)
            if not name:
                continue

            slug = href
            url = urljoin(BASE_URL, href)
            result.append(
                Tournament(
                    slug=slug,
                    name=name,
                    status=status,
                    url=url,
                )
            )

    return result


def sync_tournaments_from_main_page() -> None:
    """
    Подтягиваем актуальные турниры с главной страницы и обновляем кэш
    KNOWN_TOURNAMENTS_BY_NAME по очищенному имени.
    """
    global KNOWN_TOURNAMENTS_BY_NAME

    try:
        html = fetch_html(MAIN_PAGE_URL)
    except Exception as e:
        log_event(
            {
                "level": "error",
                "msg": "fetch_main_page_failed",
                "error": str(e),
            }
        )
        return

    tournaments = parse_tournaments_from_main(html)
    mapping: Dict[str, Tournament] = {}

    for t in tournaments:
        cleaned_name = clean_tournament_name(t.name).lower()
        mapping[cleaned_name] = t

    KNOWN_TOURNAMENTS_BY_NAME = mapping
    logger.info("Синхронизировано турниров: %s", len(KNOWN_TOURNAMENTS_BY_NAME))

def parse_score_tuple(score: Optional[str]) -> Optional[tuple[int, int]]:
    """
    Безопасно парсим строку счёта вида '2:1' -> (2, 1).
    Любая странность -> None.
    """
    if not score:
        return None
    m = re.match(r"^\s*(\d+)\s*[:\-]\s*(\d+)\s*$", score)
    if not m:
        return None
    left = int(m.group(1))
    right = int(m.group(2))
    # sanity-check от дичи типа 2025:14
    if left < 0 or right < 0 or left > 10 or right > 10:
        return None
    return left, right



_SCORE_RE = re.compile(r"^\s*(\d+)\s*[:\-]\s*(\d+)\s*$")


def _clean_str(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    return s or None


def normalize_match(m: Match) -> Match:
    """
    Применяем бизнес-валидацию:
    - 'live'/'finished' не могут быть без двух команд;
    - мусорный счёт вычищаем;
    - для BoX проверяем, что победитель набрал достаточно карт;
    - не допускаем finished + 0:0 (такого счёта реально не бывает).
    """

    # 1. 'live' или 'finished' без двух команд — это ещё не реальный матч
    if m.status in ("live", "finished") and (not m.team1 or not m.team2):
        m.status = "unknown"
        m.score = None
        return m

    # 2. Разбираем счёт, выкидываем мусор вроде "0:" или "2025:14"
    score_tuple = parse_score_tuple(m.score)
    if score_tuple is None:
        m.score = None
        # если у нас "finished", но счёта нет — тоже подозрительно
        if m.status == "finished":
            m.status = None
        return m

    left, right = score_tuple

    # 2а. Жёстко отсекаем finished + 0:0, даже если не знаем Bo
    if m.status == "finished" and left == 0 and right == 0:
        m.status = None
        m.score = None
        return m

    # 3. Проверяем консистентность с Bo
    bo_int = parse_bo_int(m.bo)
    if bo_int is not None and bo_int >= 1:
        needed_to_win = bo_int // 2 + 1  # Bo3 -> 2, Bo5 -> 3 и т.п.

        if m.status == "finished":
            # победитель должен набрать минимум needed_to_win
            if max(left, right) < needed_to_win:
                # пример: Bo3 + 1:0 → не финальный результат
                m.status = "unknown"
                m.score = None
                return m

    # дошли сюда — матч выглядит разумным
    return m

# ---------------------------------------------------------------------------
# ПАРСИНГ МАТЧЕЙ С Liquipedia:Matches
# ---------------------------------------------------------------------------


def parse_matches_from_html(html: str) -> List[Match]:
    soup = BeautifulSoup(html, "html.parser")

    containers = soup.select(".match-info")
    print(f"[DEBUG] Найдено контейнеров .match-info: {len(containers)}")

    matches: List[Match] = []

    for container in containers:
        # --- Время ---
        time_el = container.select_one(".timer-object-date, .timer-object")
        time_raw: Optional[str] = time_el.get_text(strip=True) if time_el else None

        if not time_raw:
            # Fallback: пробуем вытащить время из общего текста контейнера
            text_block = " ".join(container.stripped_strings)
            m_time = re.search(
                r"[A-Za-z]+\s+\d{1,2},\s+\d{4}\s*-\s*\d{1,2}:\d{2}\s*[A-Z]{2,4}",
                text_block,
            )
            if m_time:
                time_raw = m_time.group(0)

        time_msk: Optional[datetime] = None
        if time_raw:
            dt_utc, parsed_msk = parse_liquipedia_time(time_raw)
            if parsed_msk is not None:
                time_msk = parsed_msk
            else:
                # более терпимый парсер (умеет жить с <abbr> и прочим)
                time_msk = parse_time_to_msk(time_raw)


        # --- Команды ---
        teams = container.select(
            ".team-template-text a, .team-template-image-icon + span.name a"
        )
        team1 = (
            normalize_team_name(extract_team_name_from_tag(teams[0]))
            if len(teams) >= 1
            else None
        )
        team2 = (
            normalize_team_name(extract_team_name_from_tag(teams[1]))
            if len(teams) >= 2
            else None
        )

        # --- Счёт и Bo ---
        score_el = container.select_one(".match-info-header-scoreholder-scorewrapper")
        score: Optional[str] = None
        bo_text: Optional[str] = None

        if score_el:
            upper = score_el.select_one(".match-info-header-scoreholder-upper")
            lower = score_el.select_one(".match-info-header-scoreholder-lower")

            if upper:
                raw_score_text = upper.get_text(strip=True)
                m_sc = re.match(r"^(\d+)\s*[:\-]\s*(\d+)$", raw_score_text)
                if m_sc:
                    left = int(m_sc.group(1))
                    right = int(m_sc.group(2))
                    # лёгкий sanity-check: не даём ерунду вроде 2025:14
                    if 0 <= left <= 10 and 0 <= right <= 10:
                        score = f"{left}:{right}"
            if lower:
                bo_text = lower.get_text(strip=True)

        need_fallback = score is None or score == "0:0" or bo_text is None
        if need_fallback:
            fallback_score, fallback_bo = parse_score_and_bo_from_container(container)

            if fallback_score and (score is None or score == "0:0"):
                score = fallback_score

            if fallback_bo and bo_text is None:
                bo_text = fallback_bo

        # --- Турнир ---
        tournament_el = container.select_one(".match-info-tournament a span")
        tournament = tournament_el.get_text(strip=True) if tournament_el else None

        # --- Статус ---
        # --- Статус ---
        status: Optional[str] = None
        status_el = container.select_one(".match-status")
        if status_el:
            txt = status_el.get_text(strip=True).lower()
            if "live" in txt:
                status = "live"
            elif "upcoming" in txt or "scheduled" in txt:
                status = "upcoming"
            elif "completed" in txt or "finished" in txt:
                status = "finished"
            else:
                status = None  # <-- было "unknown"
        else:
            status = None  # <-- было None/unknown; фиксируем явно



        # --- URL матча (канонический, без action=edit&redlink=1) ---
        match_url: Optional[str] = None

        # пытаемся вытащить Match:ID из кнопки матча
        match_page_link = container.select_one(".match-page-button a")
        combined = ""
        if match_page_link:
            href = match_page_link.get("href") or ""
            title_attr = match_page_link.get("title") or ""
            combined = " ".join([href, title_attr])

        # если в кнопке нет ID — пробуем вытащить из текста всего контейнера
        m_id = re.search(r"Match:(ID_[^ \t&#/?]+)", combined)
        if not m_id:
            text_block = " ".join(container.stripped_strings)
            m_id = re.search(r"Match:(ID_[^ \t&#/?]+)", text_block)

        # если нашли ID — строим канонический URL
        if m_id:
            liqui_id = m_id.group(1)
            match_url = urljoin(BASE_URL, f"/dota2/index.php?title=Match:{liqui_id}")
        else:
            match_url = None


        m_obj = Match(
            time_msk=time_msk,
            time_raw=time_raw,
            team1=team1,
            team2=team2,
            score=score,
            bo=bo_text,
            tournament=tournament,
            status=status,
            match_url=match_url,
        )

        m_obj = normalize_match(m_obj)
        matches.append(m_obj)

    print(f"[DEBUG] parse_matches_from_html: итоговых матчей: {len(matches)}")
    return matches


# ---------------------------------------------------------------------------
# UID МАТЧА (Liquipedia Match:ID + fallback)
# ---------------------------------------------------------------------------

def build_match_identifier(m: Match) -> str:
    """
    Пытаемся вытащить liquipedia Match:ID_* из match_url.

    Поддерживаем оба варианта URL:
      - /dota2/Match:ID_XXXX
      - /dota2/index.php?title=Match:ID_XXXX&...

    Возвращаем строку вида:
      "ID_DDNks4MTOD_0002"
    или "" если ID найти не удалось.
    """
    if not m.match_url:
        return ""

    url = m.match_url

    # Вариант 1: классический path: /Match:ID_...
    m1 = re.search(r"Match:(ID_[^&#/?]+)", url)
    if m1:
        return m1.group(1)

    # Вариант 2: просто ID_... где-то в query
    m2 = re.search(r"(ID_[A-Za-z0-9]+(?:_[0-9A-Za-z\-]+)?)", url)
    if m2:
        return m2.group(1)

    return ""


def build_match_uid(m: Match) -> Optional[str]:
    """
    Строим UID только из Liquipedia Match:ID_*
    Формат: "lp:ID_xxx".
    Если ID не нашли — возвращаем None.
    """
    liqui_id = build_match_identifier(m)
    if not liqui_id:
        return None
    return f"lp:{liqui_id}"




from typing import List, Set, Tuple

def deduplicate_matches(matches: List[Match]) -> List[Match]:
    """
    Убирает дубли матчей внутри одного прохода парсера.

    Простая и предсказуемая логика:
    1) Пытаемся вытащить liquipedia Match ID через build_match_identifier(m).
       Если получилось — считаем ключом "lp:ID_xxx" и по нему дедупим.
    2) Если ID нет — матч НЕ дедупим (лучше лишний дубль, чем потерянный матч).
    """

    seen_lp_ids: Set[str] = set()
    result: List[Match] = []

    for m in matches:
        uid = ""

        try:
            liqui_id = build_match_identifier(m)
        except Exception:
            liqui_id = ""

        if liqui_id:
            uid = f"lp:{liqui_id}"

        if uid:
            if uid in seen_lp_ids:
                # дубль того же матча — пропускаем
                continue
            seen_lp_ids.add(uid)

        # если uid пустой, вообще не пытаемся дедупить — просто добавляем
        result.append(m)

    logger.info(
        "Дедупликация матчей: было %s, стало %s",
        len(matches),
        len(result),
    )
    return result

# ---------------------------------------------------------------------------
# СОХРАНЕНИЕ МАТЧЕЙ В БД (с миграцией UID)
# ---------------------------------------------------------------------------

def save_matches_to_db(matches: List[Match], max_retries: int = 3) -> None:
    if not matches:
        print("Нет матчей для сохранения")
        return

    attempt = 1
    while True:
        try:
            _save_matches_to_db_impl(matches)
            auto_repair_matches()
            return
        except errors.DeadlockDetected as e:
            logger.warning(
                "Deadlock при сохранении матчей (попытка %s/%s): %s",
                attempt,
                max_retries,
                e,
            )
            if attempt >= max_retries:
                raise
            attempt += 1
            time.sleep(1)


import re
from typing import Optional


def _normalize_bo(value) -> int | None:
    """
    Приводит значение Bo к int или None.

    Поддерживает варианты:
      - 3, 5 (уже int)
      - "3"
      - "Bo3", "(Bo3)", "BO5", "best of 3" и подобное

    Если распарсить не удалось — возвращает None.
    """
    if value is None:
        return None

    # если уже int — просто возвращаем
    if isinstance(value, int):
        return value

    s = str(value).strip().lower()
    if not s:
        return None

    # убираем текстовый мусор
    for token in ("best of", "bo", "(", ")", ":"):
        s = s.replace(token, "")
    s = s.strip()

    if not s:
        return None

    try:
        num = int(s)
    except ValueError:
        return None

    # 0 нам фактически не нужен — считаем как "нет данных"
    return num or None


def _build_match_uid(m) -> str:
    """
    Строим стабильный идентификатор матча.

    Приоритет:
      1. Если у объекта есть атрибут match_uid и он не пустой — используем его.
      2. Если есть liquipedia_match_id — uid = "lp:{id}".
      3. Если есть match_url с куском "Match:ID_xxx" — uid = "lp:ID_xxx".
      4. Fallback: время + команды + турнир + bo, в нижнем регистре.
    """
    # 1. Уже готовый match_uid в объекте (на всякий пожарный)
    existing = getattr(m, "match_uid", None)
    if isinstance(existing, str) and existing.strip():
        return existing.strip()

    liqui_id: Optional[str] = getattr(m, "liquipedia_match_id", None)
    if liqui_id:
        liqui_id = liqui_id.strip()

    # 2. Если liquipedia_match_id ещё нет, пытаемся вытащить его из match_url
    if not liqui_id:
        url = (getattr(m, "match_url", "") or "").strip()
        m_url = re.search(r"Match:(ID_[^&#/?]+)", url)
        if m_url:
            liqui_id = m_url.group(1)

    # 3. Если удалось получить Liquipedia ID — делаем lp:ID_xxx
    if liqui_id:
        return f"lp:{liqui_id}"

    # 4. Fallback-строка, чтобы матч всё равно имел детерминированный uid
    time_part = ""
    match_time_msk = getattr(m, "match_time_msk", None)
    if match_time_msk is not None:
        try:
            time_part = match_time_msk.isoformat()
        except Exception:
            time_part = str(match_time_msk)

    pieces = [
        time_part,
        (getattr(m, "team1", "") or "").strip().lower(),
        (getattr(m, "team2", "") or "").strip().lower(),
        (getattr(m, "tournament", "") or "").strip().lower(),
        f"bo{getattr(m, 'bo', 0) or 0}",
    ]
    return "|".join(pieces)



def _save_matches_to_db_impl(matches: List[Match]) -> None:
    if not matches:
        return

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for m in matches:
                bo_int = parse_bo_int(m.bo)

                # --- выбор / миграция match_uid ---
                match_uid: Optional[str] = None
                new_uid = build_match_uid(m)

                if new_uid:
                    # 1. уже есть запись с таким UID?
                    cur.execute(
                        """
                        SELECT id, match_uid
                        FROM dota_matches
                        WHERE match_uid = %(match_uid)s
                        LIMIT 1;
                        """,
                        {"match_uid": new_uid},
                    )
                    row = cur.fetchone()
                    if row:
                        match_uid = new_uid
                    else:
                        # 2. ищем старую запись, чтобы мигрировать на новый UID
                        existing_row = None

                        # 2а) по match_url (если есть)
                        if m.match_url:
                            cur.execute(
                                """
                                SELECT id, match_uid
                                FROM dota_matches
                                WHERE match_url = %(match_url)s
                                ORDER BY match_time_msk DESC NULLS LAST
                                LIMIT 1;
                                """,
                                {"match_url": m.match_url},
                            )
                            existing_row = cur.fetchone()

                        # 2б) по (team1, team2, tournament, время ±15 мин) — только если есть time_msk
                        if (
                            existing_row is None
                            and m.time_msk is not None
                            and m.team1 and m.team2 and m.tournament
                        ):
                            cleaned_tournament = (
                                clean_tournament_name(m.tournament) or m.tournament
                            )
                            cur.execute(
                                """
                                SELECT id, match_uid
                                FROM dota_matches
                                WHERE
                                    team1 = %(team1)s
                                    AND team2 = %(team2)s
                                    AND lower(tournament) LIKE lower(%(tournament_prefix)s)
                                    AND match_time_msk IS NOT NULL
                                    AND ABS(
                                        EXTRACT(EPOCH FROM (match_time_msk - %(match_time_msk)s))
                                    ) <= 900
                                ORDER BY match_time_msk DESC
                                LIMIT 1;
                                """,
                                {
                                    "team1": m.team1,
                                    "team2": m.team2,
                                    "tournament_prefix": cleaned_tournament + "%",
                                    "match_time_msk": m.time_msk,
                                },
                            )
                            existing_row = cur.fetchone()

                        if existing_row:
                            old_id, old_uid = existing_row
                            # мигрируем старый UID на новый
                            cur.execute(
                                """
                                UPDATE dota_matches
                                SET match_uid = %(new_uid)s,
                                    updated_at = now()
                                WHERE id = %(id)s;
                                """,
                                {"new_uid": new_uid, "id": old_id},
                            )
                            match_uid = new_uid
                        else:
                            # это новый матч
                            match_uid = new_uid

                if not match_uid:
                    # fallback: пытаемся найти уже существующий матч по старой схеме только если есть время
                    existing_uid = None
                    if (
                        m.time_msk is not None
                        and m.team1 and m.team2 and m.tournament
                    ):
                        cleaned_tournament = (
                            clean_tournament_name(m.tournament) or m.tournament
                        )
                        cur.execute(
                            """
                            SELECT id, match_uid
                            FROM dota_matches
                            WHERE
                                team1 = %(team1)s
                                AND team2 = %(team2)s
                                AND lower(tournament) LIKE lower(%(tournament_prefix)s)
                                AND match_time_msk IS NOT NULL
                                AND ABS(
                                    EXTRACT(EPOCH FROM (match_time_msk - %(match_time_msk)s))
                                ) <= 900
                            ORDER BY match_time_msk DESC
                            LIMIT 1;
                            """,
                            {
                                "team1": m.team1,
                                "team2": m.team2,
                                "tournament_prefix": cleaned_tournament + "%",
                                "match_time_msk": m.time_msk,
                            },
                        )
                        row = cur.fetchone()
                        if row:
                            existing_uid = row[1]

                    match_uid = existing_uid or build_fallback_match_uid(m)

                # Вставка / апдейт
                cur.execute(
                    """
                    INSERT INTO dota_matches (
                        match_time_msk,
                        match_time_raw,
                        team1,
                        team2,
                        score,
                        bo,
                        tournament,
                        status,
                        match_uid,
                        match_url
                    )
                    VALUES (
                        %(match_time_msk)s,
                        %(match_time_raw)s,
                        %(team1)s,
                        %(team2)s,
                        %(score)s,
                        %(bo)s,
                        %(tournament)s,
                        %(status)s,
                        %(match_uid)s,
                        %(match_url)s
                    )
                    ON CONFLICT (match_uid) DO UPDATE SET
                        match_time_msk = COALESCE(EXCLUDED.match_time_msk, dota_matches.match_time_msk),
                        score          = COALESCE(EXCLUDED.score, dota_matches.score),
                        bo             = COALESCE(EXCLUDED.bo, dota_matches.bo),
                        match_time_raw = COALESCE(EXCLUDED.match_time_raw, dota_matches.match_time_raw),
                        team1          = COALESCE(EXCLUDED.team1, dota_matches.team1),
                        team2          = COALESCE(EXCLUDED.team2, dota_matches.team2),
                        tournament     = COALESCE(EXCLUDED.tournament, dota_matches.tournament),
                        status = CASE
                            WHEN EXCLUDED.status IS NULL THEN dota_matches.status
                            WHEN EXCLUDED.status = 'unknown' THEN dota_matches.status
                            ELSE EXCLUDED.status
                        END,                        match_url      = COALESCE(EXCLUDED.match_url, dota_matches.match_url),
                        updated_at     = now();
                                        """,
                    {
                        "match_time_msk": m.time_msk,
                        "match_time_raw": m.time_raw,
                        "team1": m.team1,
                        "team2": m.team2,
                        "score": m.score,
                        "bo": bo_int,
                        "tournament": m.tournament,
                        "status": m.status,
                        "match_uid": match_uid,
                        "match_url": m.match_url,
                    },
                )

            conn.commit()

    print(f"Сохранили/обновили {len(matches)} матчей в БД")



# ---------------------------------------------------------------------------
# ОБНОВЛЕНИЕ СЧЁТА МАТЧЕЙ
# ---------------------------------------------------------------------------

def extract_liquipedia_id_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"Match:(ID_[^&#/?]+)", url)
    return m.group(1) if m else None


def fetch_score_from_completed_by_id(liqui_id: str) -> tuple[Optional[str], Optional[str]]:
    """
    Ищем матч во вкладке Completed по liquipedia Match ID (ID_xxx).
    Возвращаем (score, bo_text).
    """
    url = MATCHES_URL + "?status=completed"

    try:
        html = fetch_html(url)
    except Exception as e:
        log_event({"level": "error", "msg": "fetch_completed_failed", "error": str(e)})
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    containers = soup.select(".match-info")
    if not containers:
        return None, None

    for c in containers:
        # Пытаемся найти Match:ID_... где угодно внутри контейнера
        text_block = " ".join(c.stripped_strings)
        m = re.search(r"Match:(ID_[^ \t&#/?]+)", text_block)
        if not m:
            # иногда ID встречается в href/title
            m = re.search(r"Match:(ID_[^ \t&#/?]+)", str(c))
        if not m:
            continue

        cid = m.group(1)
        if cid != liqui_id:
            continue

        # Нашли нужный матч — берём score/bo
        score, bo_text = None, None

        score_el = c.select_one(".match-info-header-scoreholder-scorewrapper")
        if score_el:
            upper = score_el.select_one(".match-info-header-scoreholder-upper")
            lower = score_el.select_one(".match-info-header-scoreholder-lower")

            if upper:
                raw = upper.get_text(strip=True)
                mm = re.match(r"^(\d+)\s*[:\-]\s*(\d+)$", raw)
                if mm:
                    a, b = int(mm.group(1)), int(mm.group(2))
                    if 0 <= a <= 10 and 0 <= b <= 10:
                        score = f"{a}:{b}"

            if lower:
                bo_text = lower.get_text(strip=True) or None

        # fallback: если верстка странная — берём “универсальным” парсером
        if not score or not bo_text:
            f_score, f_bo = parse_score_and_bo_from_container(c)
            if not score and f_score:
                score = f_score
            if not bo_text and f_bo:
                bo_text = f_bo

        return score, bo_text

    return None, None



def _parse_score_block_from_soup(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    """
    Общая логика вытаскивания score + Bo из HTML блока матча.
    Возвращает (score, bo_text).
    """
    score_el = soup.select_one(".match-info-header-scoreholder-scorewrapper")
    if not score_el:
        return None, None

    score = None
    bo_text = None

    upper = score_el.select_one(".match-info-header-scoreholder-upper")
    lower = score_el.select_one(".match-info-header-scoreholder-lower")

    if upper:
        parts = upper.get_text(strip=True).split(":")
        if len(parts) == 2:
            score = f"{parts[0]}:{parts[1]}"

    if lower:
        bo_text = lower.get_text(strip=True)

    return score, bo_text


def fetch_score_from_matches_by_id(liqui_id: str, url: str) -> tuple[Optional[str], Optional[str]]:
    """
    Ищем матч по liquipedia Match ID (ID_xxx) на странице url
    (MATCHES_URL или MATCHES_URL?status=completed).
    Возвращаем (score, bo_text).

    Эта версия строит индекс ID->контейнер и логирует диагностическую инфу,
    если нужного ID не найдено.
    """
    try:
        html = fetch_html(url)
    except Exception as e:
        log_event({"level": "error", "msg": "fetch_matches_by_id_failed", "url": url, "error": str(e)})
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    containers = soup.select(".match-info")
    if not containers:
        logger.info("[SCORE_ID] no .match-info on %s", url)
        return None, None

    ID_RE = re.compile(r"(ID_[A-Za-z0-9]+(?:_[0-9A-Za-z\-]+)*)")

    def _extract_ids_from_container(c: Tag) -> list[str]:
        ids: list[str] = []

        # 1) смотрим кнопку матча
        a_btn = c.select_one(".match-page-button a")
        if a_btn:
            combined = f"{a_btn.get('href','')} {a_btn.get('title','')}"
            ids += ID_RE.findall(combined)

        # 2) смотрим все ссылки внутри
        for a in c.find_all("a", href=True):
            combined = f"{a.get('href','')} {a.get('title','')}"
            ids += ID_RE.findall(combined)

        # 3) fallback по сырому html контейнера
        ids += ID_RE.findall(str(c))

        # уникализируем сохранив порядок
        seen = set()
        out = []
        for x in ids:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    # строим индекс id -> container
    index: dict[str, Tag] = {}
    for c in containers:
        for cid in _extract_ids_from_container(c):
            # берём первый найденный контейнер для id
            if cid not in index:
                index[cid] = c

    if liqui_id not in index:
        # диагностика: покажем, какие ID вообще видим на странице
        sample = list(index.keys())[:10]
        logger.info(
            "[SCORE_ID] id not found on page. target=%s url=%s containers=%d indexed_ids=%d sample=%s",
            liqui_id, url, len(containers), len(index), sample
        )
        return None, None

    c = index[liqui_id]

    # парсим score/bo
    score: Optional[str] = None
    bo_text: Optional[str] = None

    score_el = c.select_one(".match-info-header-scoreholder-scorewrapper")
    if score_el:
        upper = score_el.select_one(".match-info-header-scoreholder-upper")
        lower = score_el.select_one(".match-info-header-scoreholder-lower")

        if upper:
            raw = upper.get_text(strip=True)
            mm = re.match(r"^(\d+)\s*[:\-]\s*(\d+)$", raw)
            if mm:
                a, b = int(mm.group(1)), int(mm.group(2))
                if 0 <= a <= 10 and 0 <= b <= 10:
                    score = f"{a}:{b}"

        if lower:
            bo_text = lower.get_text(strip=True) or None

    if not score or not bo_text:
        f_score, f_bo = parse_score_and_bo_from_container(c)
        if not score and f_score:
            score = f_score
        if not bo_text and f_bo:
            bo_text = f_bo

    logger.info("[SCORE_ID] found target=%s url=%s score=%s bo=%s", liqui_id, url, score, bo_text)
    return score, bo_text


def fetch_score_from_match_page(match_url: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        html = fetch_html(match_url)
    except requests .HTTPError as e:
        if getattr(e, "response", None) is not None and e.response.status_code == 404:
            # страницы матча не существует — это норма
            logger.info("Match page not found (404), skip: %s", match_url)
            return None, None
        log_event({"level":"error","msg":"fetch_score_from_match_page_failed","match_url":match_url,"error":str(e)})
        return None, None
    except Exception as e:
        log_event({"level":"error","msg":"fetch_score_from_match_page_failed","match_url":match_url,"error":str(e)})
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    return _parse_score_block_from_soup(soup)



def fetch_score_from_main_completed(team1: str, team2: str, tournament_clean: str) -> Optional[str]:
    url = MATCHES_URL + "?status=completed"
    try:
        html = fetch_html(url)
    except Exception as e:
        log_event({"level":"error","msg":"fetch_score_from_main_completed_failed","error":str(e)})
        return None

    matches = parse_matches_from_html(html)

    team1_norm = team1.strip().lower()
    team2_norm = team2.strip().lower()
    tournament_norm = tournament_clean.strip().lower()

    for m in matches:
        if not m.team1 or not m.team2:
            continue
        if m.team1.strip().lower() != team1_norm:
            continue
        if m.team2.strip().lower() != team2_norm:
            continue

        t = clean_tournament_name(m.tournament or "").strip().lower()
        if tournament_norm and tournament_norm not in t:
            continue

        if m.score:
            print(f"[SCORE_MAIN] Нашли счёт в completed: {team1} vs {team2} -> {m.score}")
            return m.score

    return None


def update_scores_from_match_pages() -> None:
    def _parse_score_tuple(score_str: str) -> Optional[tuple[int, int]]:
        try:
            a_str, b_str = score_str.strip().split(":")
            return int(a_str), int(b_str)
        except Exception:
            return None

    def _is_final_score(score_str: str, bo_value: Optional[int]) -> bool:
        if not bo_value or bo_value <= 0:
            return False
        st = _parse_score_tuple(score_str)
        if not st:
            return False
        a, b = st
        needed = bo_value // 2 + 1
        return max(a, b) >= needed

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    match_url,
                    liquipedia_match_id,
                    score,
                    status,
                    bo
                FROM dota_matches
                WHERE
                    (status = 'live' OR status = 'upcoming' OR status IS NULL)
                    AND match_time_msk IS NOT NULL
                    AND match_time_msk < (now() AT TIME ZONE 'Europe/Moscow') - INTERVAL '10 minutes'
                ORDER BY match_time_msk
                LIMIT 200;
                """
            )

            rows = cur.fetchall()
            if not rows:
                print("[SCORE] Нет матчей, требующих обновления счёта")
                return

            print(f"[SCORE] Обновляем счёт для {len(rows)} матчей")

            for (match_id, match_url, liqui_id_db, score_db, status_db, bo_db) in rows:
                # если уже финальный — пропускаем
                if score_db and bo_db and _is_final_score(score_db, bo_db):
                    continue

                liqui_id = (liqui_id_db or "").strip() or extract_liquipedia_id_from_url(match_url)
                if not liqui_id:
                    cur.execute(
                        "UPDATE dota_matches SET last_score_check_at = now() WHERE id = %(id)s;",
                        {"id": match_id},
                    )
                    continue

                logger.info("[SCORE_ID] try match_id=%s liqui_id=%s", match_id, liqui_id)

                new_score: Optional[str] = None
                new_bo: Optional[int] = None

                # 1) matches (live/finished)
                s, bo_text = fetch_score_from_matches_by_id(liqui_id, MATCHES_URL)
                if s:
                    new_score = s
                if bo_text:
                    new_bo = parse_bo_int(bo_text)

                # 2) completed
                if not new_score:
                    s, bo_text = fetch_score_from_matches_by_id(liqui_id, MATCHES_URL + "?status=completed")
                    if s:
                        new_score = s
                    if bo_text and new_bo is None:
                        new_bo = parse_bo_int(bo_text)

                # 3) match page (optional)
                if not new_score and match_url:
                    s, bo_text = fetch_score_from_match_page(match_url)
                    if s:
                        new_score = s
                    if bo_text and new_bo is None:
                        new_bo = parse_bo_int(bo_text)

                if not new_score:
                    cur.execute(
                        "UPDATE dota_matches SET last_score_check_at = now() WHERE id = %(id)s;",
                        {"id": match_id},
                    )
                    continue

                bo_effective = new_bo if new_bo is not None else bo_db
                is_final = _is_final_score(new_score, bo_effective)
                new_status = "finished" if is_final else "live"

                cur.execute(
                    """
                    UPDATE dota_matches
                    SET
                        score = %(score)s,
                        bo = COALESCE(%(bo)s, bo),
                        status = %(status)s,
                        last_score_check_at = now(),
                        score_last_updated_at = now(),
                        updated_at = now()
                    WHERE id = %(id)s;
                    """,
                    {"id": match_id, "score": new_score, "bo": new_bo, "status": new_status},
                )

                logger.info(
                    "[SCORE_DB] updated id=%s rowcount=%s score=%s bo=%s status=%s",
                    match_id, cur.rowcount, new_score, new_bo, new_status
                )

        conn.commit()

    print("[SCORE] Обновление счёта завершено")

# ---------------------------------------------------------------------------
# ОБНОВЛЕНИЕ СТАТУСОВ МАТЧЕЙ ПО ВРЕМЕНИ
# ---------------------------------------------------------------------------

def refresh_statuses_in_db() -> None:
    """
    Обновляем status матчей по времени и данным в БД.
    finished ставим ТОЛЬКО если счёт финальный относительно bo.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dota_matches
                SET
                    status = CASE
                        -- 1) финализация по bo+score
                        WHEN bo IS NOT NULL
                             AND score IS NOT NULL AND score <> ''
                             AND score ~ '^[0-9]+:[0-9]+$'
                             AND GREATEST(
                                 split_part(score, ':', 1)::int,
                                 split_part(score, ':', 2)::int
                             ) >= ((bo / 2)::int + 1)
                        THEN 'finished'

                        -- 2) ещё не начался
                        WHEN match_time_msk > now() + INTERVAL '5 minutes'
                        THEN 'upcoming'

                        -- 3) должен идти (в пределах 4 часов)
                        WHEN match_time_msk <= now() - INTERVAL '5 minutes'
                             AND match_time_msk >= now() - INTERVAL '4 hours'
                             AND (status IS NULL OR status IN ('unknown', 'upcoming'))
                        THEN 'live'

                        -- иначе не трогаем
                        ELSE status
                    END,
                    updated_at = now()
                WHERE match_time_msk IS NOT NULL;
                """
            )
        conn.commit()

    print("Статусы матчей обновлены по времени/BO")



# ---------------------------------------------------------------------------
# WORKER
# ---------------------------------------------------------------------------

def worker_once() -> None:
    """
    Один проход:
      1) синк турниров;
      2) парсинг матчей;
      3) дедуп;
      4) сохранение в БД (с миграцией UID);
      5) дообновление счёта;
      6) обновление статусов.
    """
    log_event({"level": "info", "msg": "worker_once_start"})
    start_ts = time.time()

    metrics = {
        "parsed_matches": 0,
        "deduped_matches": 0,
    }

    # 1. турниры
    try:
        sync_tournaments_from_main_page()
    except Exception as e:
        logger.warning("Не удалось синхронизировать турниры: %s", e)

    # 2. матчи
    try:
        html = fetch_html(MATCHES_URL)
    except Exception as e:
        log_event(
            {
                "level": "error",
                "msg": "fetch_matches_failed",
                "error": str(e),
            }
        )
        return

    matches = parse_matches_from_html(html)
    metrics["parsed_matches"] = len(matches)

    matches = deduplicate_matches(matches)
    metrics["deduped_matches"] = len(matches)

    save_matches_to_db(matches)

    # 5. добиваем счёт
    update_scores_from_match_pages()

    # 6. статусы
    refresh_statuses_in_db()

    elapsed = time.time() - start_ts
    metrics["elapsed_sec"] = round(elapsed, 2)

    log_event(
        {
            "level": "info",
            "msg": "worker_once_finished",
            "metrics": metrics,
        }
    )

    print(
        f"Проход завершён: {metrics['parsed_matches']} матчей (после дедупа: {metrics['deduped_matches']}), "
        f"за {metrics['elapsed_sec']} сек."
    )


def worker_loop() -> None:
    """
    Бесконечный цикл.
    """
    while True:
        try:
            worker_once()
        except Exception as e:
            log_event(
                {
                    "level": "error",
                    "msg": "worker_loop_exception",
                    "error": str(e),
                }
            )
            logger.exception("Ошибка в worker_loop: %s", e)
        time.sleep(SCRAPE_INTERVAL_SECONDS)


if __name__ == "__main__":
    # Можно запускать либо один раз, либо переключиться на loop, если нужно
    worker_once()
    # worker_loop()
