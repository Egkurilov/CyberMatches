#!/usr/bin/env python3
"""
cs2_main.py — Liquipedia Counter-Strike/CS2 parser (Liquipedia:Matches)
Логика как в твоём dota main.py, но с "правильной" моделью команд:

- match_uid только по Liquipedia Match:ID_* -> "lp:ID_xxx"
- миграция старых матчей на новый UID (по match_url или time+teams+tournament±15min)
- таблица cs2_teams + апсерты команд (unique по liquipedia_path)
- в cs2_matches сохраняем team1_id/team2_id, а team1/team2 остаются как кеш-строки
- score обновляем через:
    1) /counterstrike/Liquipedia:Matches
    2) /counterstrike/Liquipedia:Matches?status=completed
    3) match page /counterstrike/index.php?title=Match:ID_xxx
- статусы обновляем по времени (finished ставим только если score финальный относительно bo — для series-score)
"""

from __future__ import annotations

try:
    from zoneinfo import ZoneInfo  # py3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # py3.8

import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List, Tuple, Set

import psycopg
from psycopg import errors
import requests
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv
from urllib.parse import urljoin

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "cs2_parser.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5, encoding="utf-8"),
    ],
)
logger = logging.getLogger("cs2_parser")


def log_event(event: dict) -> None:
    event["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(json.dumps(event, ensure_ascii=False))


# ---------------------------------------------------------------------------
# ENV / SETTINGS
# ---------------------------------------------------------------------------

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

SCRAPE_INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "600"))
TARGET_TIMEZONE = os.getenv("TARGET_TIMEZONE", "Europe/Moscow")

BASE_URL = "https://liquipedia.net"
MATCHES_URL = f"{BASE_URL}/counterstrike/Liquipedia:Matches"
MAIN_PAGE_URL = f"{BASE_URL}/counterstrike/Main_Page"

# Таблицы (можно переопределять env)
TEAMS_TABLE = os.getenv("CS2_TEAMS_TABLE", "cs2_teams")
MATCHES_TABLE = os.getenv("CS2_MATCHES_TABLE", "cs2_matches")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
}

MONTHS: Dict[str, int] = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}

# Liquipedia на CS часто показывает PST/PDT и т.п.
TZ_IANA_MAP = {
    "UTC": "UTC",
    "GMT": "UTC",

    # Europe
    "CET": "Europe/Berlin",
    "CEST": "Europe/Berlin",
    "EET": "Europe/Athens",
    "EEST": "Europe/Athens",
    "MSK": "Europe/Moscow",
    "WET": "Europe/Lisbon",   # важный кейс из твоего вывода

    # Americas
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "BRT": "America/Sao_Paulo",

    # Asia
    "CST": "Asia/Shanghai",      # China Standard Time
    "HKT": "Asia/Hong_Kong",
    "SGT": "Asia/Singapore",
    "JST": "Asia/Tokyo",
    "KST": "Asia/Seoul",
    "IST": "Asia/Kolkata",
    "GST": "Asia/Dubai",
}

TARGET_TZ = ZoneInfo(TARGET_TIMEZONE)

# ---------------------------------------------------------------------------
# MODELS
# ---------------------------------------------------------------------------

@dataclass
class Match:
    time_msk: Optional[datetime]
    time_raw: Optional[str]

    team1: Optional[str]
    team2: Optional[str]
    team1_url: Optional[str]
    team2_url: Optional[str]
    team1_path: Optional[str]
    team2_path: Optional[str]

    score: Optional[str]
    bo: Optional[str]
    tournament: Optional[str]
    status: Optional[str]
    match_url: Optional[str]  # canonical match url /index.php?title=Match:ID_...


@dataclass
class Tournament:
    slug: str
    name: str
    status: str
    url: str


KNOWN_TOURNAMENTS_BY_NAME: Dict[str, Tournament] = {}

# ---------------------------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------------------------

def get_db_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def ensure_cs2_teams_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS public.{TEAMS_TABLE} (
                id BIGSERIAL PRIMARY KEY,

                liquipedia_path TEXT NOT NULL,
                liquipedia_url  TEXT NOT NULL,
                name            TEXT NOT NULL,

                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

                CONSTRAINT {TEAMS_TABLE}_liquipedia_path_uq UNIQUE (liquipedia_path)
            );
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS {TEAMS_TABLE}_name_idx ON public.{TEAMS_TABLE}(lower(name));")
    conn.commit()


def ensure_cs2_matches_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        # 1) Таблица (если её нет)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS public.{MATCHES_TABLE} (
                id BIGSERIAL PRIMARY KEY,

                match_time_msk TIMESTAMPTZ,
                match_time_raw TEXT,

                team1 TEXT,
                team2 TEXT,

                score TEXT,
                bo INTEGER,

                tournament TEXT,
                status TEXT,

                match_uid TEXT NOT NULL,
                match_url TEXT,
                liquipedia_match_id TEXT,

                last_score_check_at TIMESTAMPTZ,
                score_last_updated_at TIMESTAMPTZ,

                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

                CONSTRAINT {MATCHES_TABLE}_match_uid_uq UNIQUE (match_uid)
            );
        """)

        # 2) Добавляем новые колонки, если таблица уже существовала (ключевой фикс)
        cur.execute(f"ALTER TABLE public.{MATCHES_TABLE} ADD COLUMN IF NOT EXISTS team1_id BIGINT;")
        cur.execute(f"ALTER TABLE public.{MATCHES_TABLE} ADD COLUMN IF NOT EXISTS team2_id BIGINT;")
        cur.execute(f"ALTER TABLE public.{MATCHES_TABLE} ADD COLUMN IF NOT EXISTS team1_url TEXT;")
        cur.execute(f"ALTER TABLE public.{MATCHES_TABLE} ADD COLUMN IF NOT EXISTS team2_url TEXT;")

        # 3) Индексы
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_time_idx ON public.{MATCHES_TABLE}(match_time_msk);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_url_idx ON public.{MATCHES_TABLE}(match_url);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_teams_idx ON public.{MATCHES_TABLE}(team1, team2);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_team_ids_idx ON public.{MATCHES_TABLE}(team1_id, team2_id);")

        # 4) FK — только после того, как колонки гарантированно есть
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = '{MATCHES_TABLE}_team1_fk'
                ) THEN
                    ALTER TABLE public.{MATCHES_TABLE}
                    ADD CONSTRAINT {MATCHES_TABLE}_team1_fk
                    FOREIGN KEY (team1_id) REFERENCES public.{TEAMS_TABLE}(id)
                    ON DELETE SET NULL;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = '{MATCHES_TABLE}_team2_fk'
                ) THEN
                    ALTER TABLE public.{MATCHES_TABLE}
                    ADD CONSTRAINT {MATCHES_TABLE}_team2_fk
                    FOREIGN KEY (team2_id) REFERENCES public.{TEAMS_TABLE}(id)
                    ON DELETE SET NULL;
                END IF;
            END $$;
        """)

    conn.commit()


# ---------------------------------------------------------------------------
# NETWORK / PARSING UTILS
# ---------------------------------------------------------------------------
def _extract_scoreholder_score_and_bo(container: Optional[Tag]) -> Tuple[Optional[str], Optional[str]]:
    if not container:
        return None, None

    # ищем внутри контейнера матча/блока любые score spans
    parts = container.select(".match-info-header-scoreholder-score")
    score = None
    if len(parts) >= 2:
        left = parts[0].get_text(strip=True)
        right = parts[1].get_text(strip=True)
        if left.isdigit() and right.isdigit():
            a, b = int(left), int(right)
            if 0 <= a <= 50 and 0 <= b <= 50:
                score = f"{a}:{b}"

    bo_text = None
    lower = container.select_one(".match-info-header-scoreholder-lower")
    if lower:
        bo_text = lower.get_text(strip=True) or None

    return score, bo_text



def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _norm_key(s: Optional[str]) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def _tour_key(s: Optional[str]) -> str:
    base = clean_tournament_name(s or "") or (s or "")
    base = base.strip().lower()
    base = re.sub(r"\s+", " ", base)
    return base

def fetch_completed_match_by_fallback(
    completed_matches: List[Match],
    team1: str,
    team2: str,
    tournament: str,
    match_time_msk: Optional[datetime],
    time_window_minutes: int = 6 * 60,   # лучше сузить: 6 часов
) -> Optional[Match]:
    pair = frozenset([_norm_key(team1), _norm_key(team2)])
    tkey = _tour_key(tournament)

    pair_candidates: List[Match] = []
    tour_candidates: List[Match] = []

    for m in completed_matches:
        if not m.team1 or not m.team2 or not m.score:
            continue

        mpair = frozenset([_norm_key(m.team1), _norm_key(m.team2)])
        if mpair != pair:
            continue

        pair_candidates.append(m)

        mtkey = _tour_key(m.tournament)
        if tkey and mtkey and (tkey == mtkey or tkey in mtkey or mtkey in tkey):
            tour_candidates.append(m)

    candidates = tour_candidates or pair_candidates
    if not candidates:
        return None

    if match_time_msk is None:
        return candidates[0]

    best = None
    best_delta = None
    for m in candidates:
        if m.time_msk is None:
            continue
        delta = abs((m.time_msk - match_time_msk).total_seconds())
        if delta > time_window_minutes * 60:
            continue
        if best is None or best_delta is None or delta < best_delta:
            best = m
            best_delta = delta

    # если время задано — матч обязан попасть в окно, иначе это почти гарантированно другой матч
    if match_time_msk is not None:
        return best  # может быть None

    if match_time_msk is not None and best is None:
        # покажем ближайшего вообще (для диагностики)
        nearest = None
        nearest_delta = None
        for m in candidates:
            if m.time_msk is None:
                continue
            d = abs((m.time_msk - match_time_msk).total_seconds())
            if nearest is None or d < (nearest_delta or 10 ** 18):
                nearest, nearest_delta = m, d
        logger.info("[SCORE][DBG] fallback_no_hit_in_window nearest_delta_sec=%s nearest_time=%r nearest_tour=%r",
                    nearest_delta, getattr(nearest, "time_msk", None), getattr(nearest, "tournament", None))


    # если времени нет — ок, берём что есть
    return candidates[0]


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.text


def _strip_page_does_not_exist(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s*\(page does not exist\)\s*$", "", name).strip()


def extract_team_name_from_tag(tag: Tag) -> str:
    if not tag:
        return ""
    title = tag.get("title")
    if title:
        clean = _strip_page_does_not_exist(title)
        if clean:
            return clean
    return _strip_page_does_not_exist(tag.get_text(strip=True))


def normalize_team_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    name = name.strip()
    return name or None


def clean_tournament_name(name: str) -> str:
    if not name:
        return name
    cleaned = re.split(
        r"\s*-\s*(?:Playoffs?|Groups?|Group\s+[A-Z]|Swiss|Stage\s+\d+|Qualifier[s]?|Finals?)",
        name,
        maxsplit=1,
    )[0]
    return cleaned.strip()


def parse_time_to_target_tz(time_str: str) -> Optional[datetime]:
    if not time_str:
        return None

    cleaned = re.sub(r"<.*?>", "", time_str)
    cleaned = " ".join(cleaned.split())

    m = re.search(
        r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})\s*-\s*"
        r"(\d{1,2}):(\d{2})\s*([A-Z]{2,4})",
        cleaned,
    )
    if not m:
        logger.warning("parse_time: cannot parse '%s'", time_str)
        return None

    month_name, day, year, hour, minute, tz_abbr = m.groups()
    month = MONTHS.get(month_name)
    if not month:
        logger.warning("parse_time: unknown month '%s' in '%s'", month_name, time_str)
        return None

    try:
        dt_naive = datetime(int(year), month, int(day), int(hour), int(minute))
    except ValueError as e:
        logger.warning("parse_time: invalid datetime '%s': %s", time_str, e)
        return None

    tz_name = TZ_IANA_MAP.get(tz_abbr, "UTC")
    try:
        src_tz = ZoneInfo(tz_name)
        dt_src = dt_naive.replace(tzinfo=src_tz)
        return dt_src.astimezone(TARGET_TZ)
    except Exception as e:
        logger.error("parse_time: tz convert failed '%s': %s", time_str, e)
        return None


def parse_bo_int(bo: Optional[str]) -> Optional[int]:
    if not bo:
        return None
    m = re.search(r"Bo\s*?(\d+)", bo, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


# Score: в CS в Completed нередко 13:11 (карта) — это не series-score.
SCORE_RE = re.compile(r"(\d+)\s*[:\-]\s*(\d+)")
BO_RE = re.compile(r"\(Bo\s*([0-9]+)\)", re.IGNORECASE)


def parse_score_and_bo_from_container(container: Tag) -> Tuple[Optional[str], Optional[str]]:
    text = " ".join(container.stripped_strings)
    if not text:
        return None, None

    score: Optional[str] = None
    bo_text: Optional[str] = None

    m_score = SCORE_RE.search(text)
    if m_score:
        try:
            left = int(m_score.group(1))
            right = int(m_score.group(2))
            if 0 <= left <= 50 and 0 <= right <= 50:
                score = f"{left}:{right}"
        except ValueError:
            pass

    m_bo = BO_RE.search(text)
    if m_bo:
        try:
            bo_num = int(m_bo.group(1))
            bo_text = f"Bo{bo_num}"
        except ValueError:
            pass

    return score, bo_text


def parse_score_tuple(score: Optional[str]) -> Optional[Tuple[int, int]]:
    if not score:
        return None
    m = re.match(r"^\s*(\d+)\s*[:\-]\s*(\d+)\s*$", score)
    if not m:
        return None
    a, b = int(m.group(1)), int(m.group(2))
    if a < 0 or b < 0 or a > 50 or b > 50:
        return None
    return a, b


def _is_series_score(score: str, bo: Optional[int]) -> bool:
    """
    Эвристика "это счёт серии", а не карты:
    - bo известен
    - max(score) <= bo
    - max(score) <= 10 (серия 2:1, 1:0, 3:2 и т.п.)
    """
    if not bo or bo <= 0:
        return False
    st = parse_score_tuple(score)
    if not st:
        return False
    a, b = st
    return max(a, b) <= bo and max(a, b) <= 10


def normalize_match(m: Match) -> Match:
    # live/finished без команд -> мусор
    if m.status in ("live", "finished") and (not m.team1 or not m.team2):
        m.status = "unknown"
        m.score = None
        return m

    # мусорный score -> чистим
    st = parse_score_tuple(m.score)
    if st is None:
        if m.status == "finished":
            m.status = None
        m.score = None
        return m

    a, b = st

    # finished + 0:0 — мусор
    if m.status == "finished" and a == 0 and b == 0:
        m.status = None
        m.score = None
        return m

    # bo-консистентность только для series-score
    bo_int = parse_bo_int(m.bo)
    if bo_int and m.score and _is_series_score(m.score, bo_int) and m.status == "finished":
        needed = bo_int // 2 + 1
        if max(a, b) < needed:
            m.status = "unknown"
            m.score = None
            return m

    return m


# ---------------------------------------------------------------------------
# UID: Liquipedia Match:ID_*
# ---------------------------------------------------------------------------

def build_match_identifier(m: Match) -> str:
    if not m.match_url:
        return ""
    url = m.match_url
    mm = re.search(r"Match:(ID_[^&#/?]+)", url)
    if mm:
        return mm.group(1)
    mm2 = re.search(r"(ID_[A-Za-z0-9]+(?:_[0-9A-Za-z\-]+)?)", url)
    return mm2.group(1) if mm2 else ""


def build_match_uid(m: Match) -> Optional[str]:
    liqui_id = build_match_identifier(m)
    if not liqui_id:
        return None
    return f"lp:{liqui_id}"


def build_fallback_match_uid(m: Match) -> str:
    time_part = m.time_msk.isoformat() if m.time_msk else ""
    return "|".join([
        time_part,
        (m.team1 or "").strip().lower(),
        (m.team2 or "").strip().lower(),
        (m.tournament or "").strip().lower(),
        f"bo{parse_bo_int(m.bo) or 0}",
    ])


def deduplicate_matches(matches: List[Match]) -> List[Match]:
    seen: Set[str] = set()
    out: List[Match] = []

    for m in matches:
        liqui_id = build_match_identifier(m)
        uid = f"lp:{liqui_id}" if liqui_id else ""
        if uid:
            if uid in seen:
                continue
            seen.add(uid)
        out.append(m)

    logger.info("Дедупликация матчей: было %s, стало %s", len(matches), len(out))
    return out


# ---------------------------------------------------------------------------
# TEAM PARSING / UPSERT
# ---------------------------------------------------------------------------

from urllib.parse import urlparse, parse_qs, unquote

def _extract_team_path_and_url(a_tag: Optional[Tag]) -> Tuple[Optional[str], Optional[str]]:
    """
    Возвращает (team_path, team_url) максимально стабильно.

    Поддерживает:
      - /counterstrike/Fnatic
      - /counterstrike/index.php?title=Team_Vitality
      - /counterstrike/index.php?title=XI_Esport&action=edit&redlink=1  (redlink)

    Правила:
      - если это redlink -> НЕ возвращаем path/url (чтобы не засорять cs2_teams)
      - если можно канонизировать к /counterstrike/<Title> -> возвращаем
    """
    if not a_tag:
        return None, None

    href = (a_tag.get("href") or "").strip()
    if not href:
        return None, None

    # 1) Нормальный путь
    if href.startswith("/counterstrike/") and "index.php" not in href:
        path = href.split("#", 1)[0].split("?", 1)[0].rstrip("/")
        if path == "/counterstrike":
            return None, None
        return path, urljoin(BASE_URL, path)

    # 2) index.php?title=...
    if href.startswith("/counterstrike/index.php"):
        # если redlink=1 — это "страница не существует", не сохраняем в teams
        if "redlink=1" in href:
            return None, None

        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        title = (qs.get("title") or [None])[0]
        if not title:
            return None, None

        # title бывает с пробелами/кодировкой
        title = unquote(title).strip()
        if not title:
            return None, None

        # На Liquipedia canonical team page обычно /counterstrike/<Title>
        canonical_path = f"/counterstrike/{title}".rstrip("/")
        return canonical_path, urljoin(BASE_URL, canonical_path)

    return None, None


def upsert_team(cur: psycopg.Cursor, name: str, path: str, url: str) -> int:
    """
    Апсерт команды по liquipedia_path, возвращает team_id.
    """
    cur.execute(
        f"""
        INSERT INTO public.{TEAMS_TABLE} (liquipedia_path, liquipedia_url, name)
        VALUES (%(path)s, %(url)s, %(name)s)
        ON CONFLICT (liquipedia_path) DO UPDATE SET
            liquipedia_url = COALESCE(EXCLUDED.liquipedia_url, public.{TEAMS_TABLE}.liquipedia_url),
            name = COALESCE(EXCLUDED.name, public.{TEAMS_TABLE}.name),
            updated_at = now()
        RETURNING id;
        """,
        {"path": path, "url": url, "name": name},
    )
    return int(cur.fetchone()[0])


# ---------------------------------------------------------------------------
# TOURNAMENTS (Main Page)
# ---------------------------------------------------------------------------

def parse_tournaments_from_main(html: str) -> List[Tournament]:
    soup = BeautifulSoup(html, "html.parser")
    result: List[Tournament] = []

    status_map: Dict[str, str] = {
        "ongoing": "ongoing",
        "upcoming": "upcoming",
        "qualifier": "upcoming",
        "recent": "completed",
        "completed": "completed",
        "results": "completed",
    }

    for header in soup.find_all(["h2", "h3"]):
        htext = header.get_text(strip=True).lower()
        status = None
        for k, v in status_map.items():
            if k in htext:
                status = v
                break
        if not status:
            continue

        ul = header.find_next("ul")
        if not ul:
            continue

        for a in ul.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("/counterstrike/"):
                continue
            name = a.get_text(strip=True)
            if not name:
                continue
            result.append(Tournament(
                slug=href,
                name=name,
                status=status,
                url=urljoin(BASE_URL, href),
            ))

    return result


def sync_tournaments_from_main_page() -> None:
    global KNOWN_TOURNAMENTS_BY_NAME
    try:
        html = fetch_html(MAIN_PAGE_URL)
    except Exception as e:
        log_event({"level": "error", "msg": "fetch_cs_main_failed", "error": str(e)})
        return

    tournaments = parse_tournaments_from_main(html)
    mapping: Dict[str, Tournament] = {}
    for t in tournaments:
        mapping[clean_tournament_name(t.name).lower()] = t

    KNOWN_TOURNAMENTS_BY_NAME = mapping
    logger.info("Синхронизировано турниров (CS): %s", len(mapping))


# ---------------------------------------------------------------------------
# PARSE MATCHES (Liquipedia:Matches)
# ---------------------------------------------------------------------------

def parse_matches_from_html(html: str) -> List[Match]:
    soup = BeautifulSoup(html, "html.parser")
    containers = soup.select(".match-info")
    logger.info("[DEBUG] .match-info containers: %d", len(containers))

    matches: List[Match] = []

    for c in containers:
        # time
        time_el = c.select_one(".timer-object-date, .timer-object")
        time_raw = time_el.get_text(strip=True) if time_el else None

        if not time_raw:
            text_block = " ".join(c.stripped_strings)
            m_time = re.search(
                r"[A-Za-z]+\s+\d{1,2},\s+\d{4}\s*-\s*\d{1,2}:\d{2}\s*[A-Z]{2,4}",
                text_block,
            )
            if m_time:
                time_raw = m_time.group(0)

        time_msk = parse_time_to_target_tz(time_raw) if time_raw else None

        # teams (+ urls/paths)
        teams = c.select(".team-template-text a, .team-template-image-icon + span.name a")

        team1 = normalize_team_name(extract_team_name_from_tag(teams[0])) if len(teams) >= 1 else None
        team2 = normalize_team_name(extract_team_name_from_tag(teams[1])) if len(teams) >= 2 else None

        team1_path, team1_url = _extract_team_path_and_url(teams[0] if len(teams) >= 1 else None)
        team2_path, team2_url = _extract_team_path_and_url(teams[1] if len(teams) >= 2 else None)

        # score + bo
        score_el = c.select_one(".match-info-header-scoreholder-scorewrapper")
        score, bo_text = _extract_scoreholder_score_and_bo(c)

        # если не нашли — fallback через общий текст
        if (score is None) or (score == "0:0") or (bo_text is None):
            f_score, f_bo = parse_score_and_bo_from_container(c)
            if f_score and (score is None or score == "0:0"):
                score = f_score
            if f_bo and bo_text is None:
                bo_text = f_bo


        need_fallback = (score is None) or (score == "0:0") or (bo_text is None)
        if need_fallback:
            f_score, f_bo = parse_score_and_bo_from_container(c)
            if f_score and (score is None or score == "0:0"):
                score = f_score
            if f_bo and bo_text is None:
                bo_text = f_bo

        # tournament
        tournament = None
        t_el = c.select_one(".match-info-tournament-name span")
        if t_el:
            tournament = t_el.get_text(strip=True) or None
        else:
            # fallback
            a = c.select_one(".match-info-tournament a")
            if a:
                tournament = a.get_text(" ", strip=True) or None

        # status
        status = None
        status_el = c.select_one(".match-status")
        if status_el:
            txt = status_el.get_text(strip=True).lower()
            if "live" in txt:
                status = "live"
            elif "upcoming" in txt or "scheduled" in txt:
                status = "upcoming"
            elif "completed" in txt or "finished" in txt:
                status = "finished"
            else:
                status = None

        # match_url canonical from Match:ID_*
        match_url = None
        match_page_link = c.select_one(".match-page-button a")

        # пробуем достать ID из href/title кнопки
        combined = ""
        if match_page_link:
            href = match_page_link.get("href") or ""
            title_attr = match_page_link.get("title") or ""
            combined = " ".join([href, title_attr])

        m_id = re.search(r"Match:(ID_[^ \t&#/?]+)", combined)
        if not m_id:
            # иногда Match:ID есть в других ссылках
            for a in c.find_all("a", href=True):
                comb = f"{a.get('href','')} {a.get('title','')}"
                m_id = re.search(r"Match:(ID_[^ \t&#/?]+)", comb)
                if m_id:
                    break

        if not m_id:
            text_block = " ".join(c.stripped_strings)
            m_id = re.search(r"Match:(ID_[^ \t&#/?]+)", text_block)

        if not m_id:
            # brute-force по HTML контейнера
            raw_html = str(c)
            m_id = re.search(r"Match:(ID_[A-Za-z0-9]+(?:_[0-9A-Za-z\-]+)*)", raw_html)


        if m_id:
            liqui_id = m_id.group(1)
            match_url = urljoin(BASE_URL, f"/counterstrike/index.php?title=Match:{liqui_id}")

        m_obj = Match(
            time_msk=time_msk,
            time_raw=time_raw,

            team1=team1,
            team2=team2,
            team1_url=team1_url,
            team2_url=team2_url,
            team1_path=team1_path,
            team2_path=team2_path,

            score=score,
            bo=bo_text,
            tournament=tournament,
            status=status,
            match_url=match_url,
        )
        matches.append(normalize_match(m_obj))

    logger.info("[DEBUG] parsed matches: %d", len(matches))
    return matches


# ---------------------------------------------------------------------------
# AUTO-REPAIR
# ---------------------------------------------------------------------------

def auto_repair_matches() -> None:
    with get_db_connection() as conn:
        ensure_cs2_teams_table(conn)
        ensure_cs2_matches_table(conn)

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM public.{MATCHES_TABLE} WHERE match_uid IS NULL OR match_uid = '';")
            deleted_no_uid = cur.rowcount

            cur.execute(f"""
                DELETE FROM public.{MATCHES_TABLE} d
                WHERE (d.team1 = 'TBD' OR d.team2 = 'TBD')
                  AND EXISTS (
                      SELECT 1
                      FROM public.{MATCHES_TABLE} d2
                      WHERE d2.id <> d.id
                        AND d2.match_time_msk = d.match_time_msk
                        AND COALESCE(LOWER(d2.tournament), '') = COALESCE(LOWER(d.tournament), '')
                        AND d2.team1 <> 'TBD'
                        AND d2.team2 <> 'TBD'
                  );
            """)
            deleted_tbd = cur.rowcount

            cur.execute(f"""
                UPDATE public.{MATCHES_TABLE}
                SET status = 'unknown', score = NULL, updated_at = now()
                WHERE status = 'finished'
                  AND (team1 IS NULL OR team1 = '')
                  AND (team2 IS NULL OR team2 = '');
            """)
            fixed_finished_no_teams = cur.rowcount

            cur.execute(f"""
                UPDATE public.{MATCHES_TABLE}
                SET status = 'unknown', score = NULL, updated_at = now()
                WHERE status = 'finished' AND score = '0:0';
            """)
            fixed_finished_zero_zero = cur.rowcount

            cur.execute(f"""
                UPDATE public.{MATCHES_TABLE}
                SET liquipedia_match_id = substring(match_uid FROM '^lp:(ID_[^|]+)')
                WHERE liquipedia_match_id IS NULL AND match_uid LIKE 'lp:ID_%';
            """)
            updated_from_uid = cur.rowcount

            cur.execute(f"""
                UPDATE public.{MATCHES_TABLE}
                SET liquipedia_match_id = substring(match_url FROM 'Match:(ID_[^&#/?]+)')
                WHERE liquipedia_match_id IS NULL
                  AND match_url LIKE '%Match:ID_%';
            """)
            updated_from_url = cur.rowcount

        conn.commit()

    logger.info(
        "[AUTO-REPAIR] deleted_no_uid=%s deleted_tbd=%s fixed_finished_no_teams=%s fixed_finished_zero_zero=%s liqui_from_uid=%s liqui_from_url=%s",
        deleted_no_uid, deleted_tbd, fixed_finished_no_teams, fixed_finished_zero_zero, updated_from_uid, updated_from_url
    )


# ---------------------------------------------------------------------------
# SAVE MATCHES (with team upserts + uid migration)
# ---------------------------------------------------------------------------

def save_matches_to_db(matches: List[Match], max_retries: int = 3) -> None:
    if not matches:
        logger.info("Нет матчей для сохранения")
        return

    attempt = 1
    while True:
        try:
            _save_matches_to_db_impl(matches)
            auto_repair_matches()
            return
        except errors.DeadlockDetected as e:
            logger.warning("Deadlock (attempt %s/%s): %s", attempt, max_retries, e)
            if attempt >= max_retries:
                raise
            attempt += 1
            time.sleep(1)


from urllib.parse import urlparse, parse_qs, unquote

def _save_matches_to_db_impl(matches: List[Match]) -> None:
    def _canon_team_url(u: Optional[str]) -> Optional[str]:
        """
        Канонизирует URL команды к стабильному виду:
          https://liquipedia.net/counterstrike/Team_Vitality

        - режет #...
        - если redlink=1 -> None
        - если index.php?title=... -> /counterstrike/<title>
        - режет ?... у обычных ссылок
        """
        if not u:
            return None
        u = u.strip()
        if not u:
            return None

        # fragment off
        u = u.split("#", 1)[0]

        # redlink => мусор
        if "redlink=1" in u:
            return None

        # index.php?title=...
        if "/counterstrike/index.php" in u and "title=" in u:
            parsed = urlparse(u)
            qs = parse_qs(parsed.query)
            title = (qs.get("title") or [None])[0]
            if not title:
                return None
            title = unquote(title).strip()
            if not title:
                return None
            return f"{BASE_URL}/counterstrike/{title}".rstrip("/")

        # обычная ссылка
        u = u.split("?", 1)[0].rstrip("/")
        return u

    def _url_to_team_path(u: Optional[str]) -> Optional[str]:
        """
        По каноничному team_url строим team_path вида:
          /counterstrike/Fnatic
        """
        u = _canon_team_url(u)
        if not u:
            return None
        # BASE_URL = https://liquipedia.net
        if u.startswith(BASE_URL):
            path = u[len(BASE_URL):]
        else:
            # на всякий случай
            parsed = urlparse(u)
            path = parsed.path
        path = path.rstrip("/")
        return path if path.startswith("/counterstrike/") else None

    with get_db_connection() as conn:
        ensure_cs2_teams_table(conn)
        ensure_cs2_matches_table(conn)

        with conn.cursor() as cur:
            for m in matches:
                bo_int = parse_bo_int(m.bo)

                # --- Нормализуем ссылки команд ДО апсерта и ДО сохранения ---
                team1_url_db = _canon_team_url(m.team1_url)
                team2_url_db = _canon_team_url(m.team2_url)

                team1_path_db = m.team1_path or _url_to_team_path(team1_url_db)
                team2_path_db = m.team2_path or _url_to_team_path(team2_url_db)

                # --- Upsert teams (если есть path/url) ---
                team1_id: Optional[int] = None
                team2_id: Optional[int] = None

                if m.team1 and team1_path_db and team1_url_db:
                    try:
                        team1_id = upsert_team(cur, m.team1, team1_path_db, team1_url_db)
                    except Exception as e:
                        logger.warning("team1 upsert failed name=%s path=%s: %s", m.team1, team1_path_db, e)

                if m.team2 and team2_path_db and team2_url_db:
                    try:
                        team2_id = upsert_team(cur, m.team2, team2_path_db, team2_url_db)
                    except Exception as e:
                        logger.warning("team2 upsert failed name=%s path=%s: %s", m.team2, team2_path_db, e)

                # --- match_uid selection/migration ---
                match_uid: Optional[str] = None
                new_uid = build_match_uid(m)

                if new_uid:
                    # 1) уже есть запись с таким UID?
                    cur.execute(
                        f"SELECT id, match_uid FROM public.{MATCHES_TABLE} WHERE match_uid = %(uid)s LIMIT 1;",
                        {"uid": new_uid},
                    )
                    row = cur.fetchone()
                    if row:
                        match_uid = new_uid
                    else:
                        existing_row = None

                        # 2а) по match_url
                        if m.match_url:
                            cur.execute(
                                f"""
                                SELECT id, match_uid
                                FROM public.{MATCHES_TABLE}
                                WHERE match_url = %(match_url)s
                                ORDER BY match_time_msk DESC NULLS LAST
                                LIMIT 1;
                                """,
                                {"match_url": m.match_url},
                            )
                            existing_row = cur.fetchone()

                        # 2б) по team1/team2/tournament/time ±15min
                        if existing_row is None and m.time_msk and m.team1 and m.team2 and m.tournament:
                            cleaned_tournament = clean_tournament_name(m.tournament) or m.tournament
                            cur.execute(
                                f"""
                                SELECT id, match_uid
                                FROM public.{MATCHES_TABLE}
                                WHERE team1 = %(team1)s
                                  AND team2 = %(team2)s
                                  AND lower(tournament) LIKE lower(%(tournament_prefix)s)
                                  AND match_time_msk IS NOT NULL
                                  AND ABS(EXTRACT(EPOCH FROM (match_time_msk - %(ts)s))) <= 900
                                ORDER BY match_time_msk DESC
                                LIMIT 1;
                                """,
                                {
                                    "team1": m.team1,
                                    "team2": m.team2,
                                    "tournament_prefix": cleaned_tournament + "%",
                                    "ts": m.time_msk,
                                },
                            )
                            existing_row = cur.fetchone()

                        if existing_row:
                            old_id, _old_uid = existing_row
                            cur.execute(
                                f"""
                                UPDATE public.{MATCHES_TABLE}
                                SET match_uid = %(new_uid)s,
                                    updated_at = now()
                                WHERE id = %(id)s;
                                """,
                                {"new_uid": new_uid, "id": old_id},
                            )
                            match_uid = new_uid
                        else:
                            match_uid = new_uid

                if not match_uid:
                    match_uid = build_fallback_match_uid(m)

                cur.execute(
                    f"""
                    INSERT INTO public.{MATCHES_TABLE} (
                        match_time_msk,
                        match_time_raw,

                        team1,
                        team2,

                        team1_id,
                        team2_id,

                        team1_url,
                        team2_url,

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

                        %(team1_id)s,
                        %(team2_id)s,

                        %(team1_url)s,
                        %(team2_url)s,

                        %(score)s,
                        %(bo)s,

                        %(tournament)s,
                        %(status)s,

                        %(match_uid)s,
                        %(match_url)s
                    )
                    ON CONFLICT (match_uid) DO UPDATE SET
                        match_time_msk = COALESCE(EXCLUDED.match_time_msk, public.{MATCHES_TABLE}.match_time_msk),
                        match_time_raw = COALESCE(EXCLUDED.match_time_raw, public.{MATCHES_TABLE}.match_time_raw),

                        team1 = COALESCE(EXCLUDED.team1, public.{MATCHES_TABLE}.team1),
                        team2 = COALESCE(EXCLUDED.team2, public.{MATCHES_TABLE}.team2),

                        team1_id = COALESCE(EXCLUDED.team1_id, public.{MATCHES_TABLE}.team1_id),
                        team2_id = COALESCE(EXCLUDED.team2_id, public.{MATCHES_TABLE}.team2_id),

                        team1_url = COALESCE(EXCLUDED.team1_url, public.{MATCHES_TABLE}.team1_url),
                        team2_url = COALESCE(EXCLUDED.team2_url, public.{MATCHES_TABLE}.team2_url),

                        score = COALESCE(EXCLUDED.score, public.{MATCHES_TABLE}.score),
                        bo    = COALESCE(EXCLUDED.bo, public.{MATCHES_TABLE}.bo),

                        tournament = COALESCE(EXCLUDED.tournament, public.{MATCHES_TABLE}.tournament),

                        -- 🔥 ГЛАВНЫЙ ФИКС: не даунгрейдим finished обратно
                        status = CASE
                            WHEN public.{MATCHES_TABLE}.status = 'finished' THEN public.{MATCHES_TABLE}.status
                            WHEN EXCLUDED.status IS NULL THEN public.{MATCHES_TABLE}.status
                            WHEN EXCLUDED.status = 'unknown' THEN public.{MATCHES_TABLE}.status
                            ELSE EXCLUDED.status
                        END,

                        match_url = COALESCE(EXCLUDED.match_url, public.{MATCHES_TABLE}.match_url),

                        updated_at = now();
                    """,
                    {
                        "match_time_msk": m.time_msk,
                        "match_time_raw": m.time_raw,

                        "team1": m.team1,
                        "team2": m.team2,

                        "team1_id": team1_id,
                        "team2_id": team2_id,

                        # ✅ сохраняем уже канонизированные URL
                        "team1_url": team1_url_db,
                        "team2_url": team2_url_db,

                        "score": m.score,
                        "bo": bo_int,

                        "tournament": m.tournament,
                        "status": m.status,

                        "match_uid": match_uid,
                        "match_url": m.match_url,
                    },
                )

        conn.commit()

    logger.info("Сохранили/обновили %d матчей", len(matches))


# ---------------------------------------------------------------------------
# SCORE UPDATES
# ---------------------------------------------------------------------------

def _liqui_id_from_uid(match_uid: Optional[str]) -> Optional[str]:
    if not match_uid:
        return None
    m = re.search(r"^lp:(ID_[^|]+)", match_uid.strip())
    return m.group(1) if m else None


from urllib.parse import urlparse, parse_qs, unquote

from urllib.parse import urlparse, parse_qs, unquote
from typing import Optional, Tuple
from bs4 import Tag

def _canon_team_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None

    # убираем якоря
    u = u.split("#", 1)[0]

    # redlink — это не страница команды, а "создать страницу"
    if "redlink=1" in u:
        return None

    # абсолют/относительный → абсолют
    if u.startswith("/"):
        u = urljoin(BASE_URL, u)

    # index.php?title=...
    if "/counterstrike/index.php" in u and "title=" in u:
        parsed = urlparse(u)
        qs = parse_qs(parsed.query)
        title = (qs.get("title") or [None])[0]
        if not title:
            return None
        title = unquote(title).strip()
        if not title:
            return None
        # нормальная форма
        return f"{BASE_URL}/counterstrike/{title}".rstrip("/")

    # обычная ссылка: режем query
    return u.split("?", 1)[0].rstrip("/")


def _url_to_team_path(u: Optional[str]) -> Optional[str]:
    u = _canon_team_url(u)
    if not u:
        return None
    if u.startswith(BASE_URL):
        path = u[len(BASE_URL):]
    else:
        path = urlparse(u).path
    path = path.rstrip("/")
    return path if path.startswith("/counterstrike/") else None


def _extract_team_path_and_url(a: Optional[Tag]) -> Tuple[Optional[str], Optional[str]]:
    """
    Возвращает (team_path, team_url) или (None, None) если ссылка плохая.
    """
    if not a:
        return None, None

    href = a.get("href") or ""
    if not href:
        return None, None

    url = _canon_team_url(href)
    if not url:
        return None, None

    path = _url_to_team_path(url)
    return path, url


def _team_pair_key(u1: Optional[str], u2: Optional[str]) -> Optional[frozenset[str]]:
    c1 = _canon_team_url(u1)
    c2 = _canon_team_url(u2)
    if not c1 or not c2:
        return None
    return frozenset([c1.lower(), c2.lower()])


def extract_liquipedia_id_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"Match:(ID_[^&#/?]+)", url)
    return m.group(1) if m else None


def _parse_score_block_from_soup(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    score_el = soup.select_one(".match-info-header-scoreholder-scorewrapper")
    return _extract_scoreholder_score_and_bo(score_el)


    score = None
    bo_text = None

    upper = score_el.select_one(".match-info-header-scoreholder-upper")
    lower = score_el.select_one(".match-info-header-scoreholder-lower")

    if upper:
        raw = upper.get_text(strip=True)
        mm = re.match(r"^(\d+)\s*[:\-]\s*(\d+)$", raw)
        if mm:
            a, b = int(mm.group(1)), int(mm.group(2))
            if 0 <= a <= 50 and 0 <= b <= 50:
                score = f"{a}:{b}"

    if lower:
        bo_text = lower.get_text(strip=True) or None

    return score, bo_text


def fetch_score_from_match_page(match_url: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        html = fetch_html(match_url)
    except requests.HTTPError as e:
        if getattr(e, "response", None) is not None and e.response.status_code == 404:
            logger.info("Match page 404, skip: %s", match_url)
            return None, None
        log_event({"level": "error", "msg": "fetch_match_page_failed", "url": match_url, "error": str(e)})
        return None, None
    except Exception as e:
        log_event({"level": "error", "msg": "fetch_match_page_failed", "url": match_url, "error": str(e)})
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    return _parse_score_block_from_soup(soup)


def fetch_score_from_matches_by_id(liqui_id: str, url: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        html = fetch_html(url)
    except Exception as e:
        log_event({"level": "error", "msg": "fetch_matches_failed", "url": url, "error": str(e)})
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    containers = soup.select(".match-info")
    if not containers:
        return None, None

    ID_RE = re.compile(r"(ID_[A-Za-z0-9]+(?:_[0-9A-Za-z\-]+)*)")

    def extract_ids(c: Tag) -> List[str]:
        ids: List[str] = []
        a_btn = c.select_one(".match-page-button a")
        if a_btn:
            combined = f"{a_btn.get('href','')} {a_btn.get('title','')}"
            ids += ID_RE.findall(combined)
        for a in c.find_all("a", href=True):
            combined = f"{a.get('href','')} {a.get('title','')}"
            ids += ID_RE.findall(combined)
        ids += ID_RE.findall(str(c))
        seen = set()
        out = []
        for x in ids:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    index: Dict[str, Tag] = {}
    for c in containers:
        for cid in extract_ids(c):
            if cid not in index:
                index[cid] = c

    if liqui_id not in index:
        return None, None

    c = index[liqui_id]

    score_el = c.select_one(".match-info-header-scoreholder-scorewrapper")
    score, bo_text = _extract_scoreholder_score_and_bo(score_el)
    return score, bo_text


def update_scores_from_match_pages() -> None:
    """
    FIX:
    - если нет liquipedia_match_id / match_url и/или нет team_url (redlink) —
      всё равно можем найти матч на completed по team1/team2/tournament/time и обновить score.
    """

    def is_final_score_series(score_str: str, bo_value: Optional[int]) -> bool:
        if not score_str or not bo_value:
            return False
        if not _is_series_score(score_str, bo_value):
            return False
        st = parse_score_tuple(score_str)
        if not st:
            return False
        a, b = st
        needed = bo_value // 2 + 1
        return max(a, b) >= needed

    def _find_completed_by_team_urls(
        completed_matches: List[Match],
        team1_url: Optional[str],
        team2_url: Optional[str],
        match_time_msk: Optional[datetime],
    ) -> Optional[Match]:
        key = _team_pair_key(team1_url, team2_url)
        if not key:
            return None

        best = None
        best_delta = None

        for m in completed_matches:
            mk = _team_pair_key(getattr(m, "team1_url", None), getattr(m, "team2_url", None))
            if not mk or mk != key:
                continue

            if match_time_msk is None or m.time_msk is None:
                return m

            delta = abs((m.time_msk - match_time_msk).total_seconds())
            if best is None or best_delta is None or delta < best_delta:
                best = m
                best_delta = delta

        return best

    # ---------- Prefetch completed (1 раз) ----------
    completed_matches: List[Match] = []
    completed_index: Dict[str, Match] = {}

    completed_urls = [
        MATCHES_URL + "?status=completed",
        MATCHES_URL + "?status=finished",
        MATCHES_URL + "?status=recent",
        MATCHES_URL + "?status=results",
    ]

    seen_uids: set[str] = set()

    for u in completed_urls:
        try:
            html = fetch_html(u)
            ms = parse_matches_from_html(html)
            added = 0
            for m in ms:
                # берём только то, где есть счёт
                if not m.score:
                    continue
                # стабильный ключ (если нет Match:ID — хотя бы fallback)
                key = build_match_uid(m) or build_fallback_match_uid(m)
                if key in seen_uids:
                    continue
                seen_uids.add(key)
                completed_matches.append(m)
                mid = extract_liquipedia_id_from_url(getattr(m, "match_url", None))
                if mid and mid not in completed_index:
                    completed_index[mid] = m
                added += 1

            logger.info("[SCORE] Prefetch %s: parsed=%d added_with_score=%d", u, len(ms), added)
        except Exception as e:
            logger.warning("[SCORE] Prefetch failed url=%s err=%s", u, e)

    logger.info("[SCORE] Prefetched completed (merged) matches_with_score: %d", len(completed_matches))

    # DEBUG: покажем, есть ли там нужные пары
    wanted = [
        frozenset(["9ine", "big"]),
        frozenset(["xi esport", "natus vincere junior"]),
        frozenset(["team vitality", "faze clan"]),
    ]
    hits = 0
    for m in completed_matches:
        if not m.team1 or not m.team2:
            continue
        k = frozenset([_norm(m.team1), _norm(m.team2)])
        if k in wanted:
            logger.info("[SCORE][DBG] completed_has_pair=%r tour=%r score=%r url=%r",
                        k, m.tournament, m.score, m.match_url)
            hits += 1
    logger.info("[SCORE][DBG] completed_pairs_hits=%d", hits)


    with get_db_connection() as conn:
        ensure_cs2_teams_table(conn)
        ensure_cs2_matches_table(conn)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id,
                    match_uid,
                    match_url,
                    liquipedia_match_id,
                    score,
                    status,
                    bo,
                    match_time_msk,
                    team1_url,
                    team2_url,
                    team1,
                    team2,
                    tournament
                FROM public.{MATCHES_TABLE}
                WHERE (status = 'live' OR status = 'upcoming' OR status IS NULL OR status = 'unknown')
                  AND match_time_msk IS NOT NULL
                  AND match_time_msk < now() - INTERVAL '10 minutes'
                ORDER BY match_time_msk
                LIMIT 300;
                """
            )
            rows = cur.fetchall()
            if not rows:
                logger.info("[SCORE] Нет матчей для обновления")
                return

            logger.info("[SCORE] Обновляем счёт для %d матчей", len(rows))

            for (
                match_id,
                match_uid,
                match_url,
                liqui_id_db,
                score_db,
                _status_db,
                bo_db,
                match_time_msk_db,
                team1_url_db,
                team2_url_db,
                team1_name_db,
                team2_name_db,
                tournament_db,
            ) in rows:

                # если уже финальная СЕРИЯ (2:1 в Bo3) — можно не трогать
                if score_db and bo_db and is_final_score_series(score_db, bo_db):
                    continue

                liqui_id = (liqui_id_db or "").strip() \
                           or _liqui_id_from_uid(match_uid) \
                           or extract_liquipedia_id_from_url(match_url)

                # ---------- 1) Если ID есть и он в completed -> finished ----------
                if liqui_id and liqui_id in completed_index:
                    found = completed_index[liqui_id]
                    new_score = found.score
                    new_bo = parse_bo_int(found.bo)

                    cur.execute(
                        f"""
                        UPDATE public.{MATCHES_TABLE}
                        SET score = COALESCE(%(score)s, score),
                            bo = COALESCE(%(bo)s, bo),
                            status = 'finished',
                            match_url = COALESCE(%(match_url)s, match_url),
                            liquipedia_match_id = COALESCE(%(liqui_id)s, liquipedia_match_id),
                            last_score_check_at = now(),
                            score_last_updated_at = now(),
                            updated_at = now()
                        WHERE id = %(id)s;
                        """,
                        {
                            "id": match_id,
                            "score": new_score,
                            "bo": new_bo,
                            "match_url": getattr(found, "match_url", None),
                            "liqui_id": liqui_id,
                        },
                    )
                    continue

                # ---------- 2) completed по team_url (если обе ссылки есть) ----------
                if completed_matches:
                    found = _find_completed_by_team_urls(
                        completed_matches=completed_matches,
                        team1_url=team1_url_db,
                        team2_url=team2_url_db,
                        match_time_msk=match_time_msk_db,
                    )
                    if found and found.score:
                        new_score = found.score
                        new_bo = parse_bo_int(found.bo)
                        new_liqui_id = extract_liquipedia_id_from_url(getattr(found, "match_url", None))

                        cur.execute(
                            f"""
                            UPDATE public.{MATCHES_TABLE}
                            SET score = COALESCE(%(score)s, score),
                                bo = COALESCE(%(bo)s, bo),
                                status = 'finished',
                                match_url = COALESCE(%(match_url)s, match_url),
                                liquipedia_match_id = COALESCE(%(liqui_id)s, liquipedia_match_id),
                                last_score_check_at = now(),
                                score_last_updated_at = now(),
                                updated_at = now()
                            WHERE id = %(id)s;
                            """,
                            {
                                "id": match_id,
                                "score": new_score,
                                "bo": new_bo,
                                "match_url": getattr(found, "match_url", None),
                                "liqui_id": new_liqui_id,
                            },
                        )
                        continue

                logger.info(
                    "[SCORE][DBG] id=%s team1=%r team2=%r tour=%r time=%r t1url=%r t2url=%r liqui_id=%r",
                    match_id, team1_name_db, team2_name_db, tournament_db, match_time_msk_db,
                    team1_url_db, team2_url_db, liqui_id
                )


                # ---------- 3) NEW: completed fallback по именам команд + турниру + времени ----------
                if completed_matches and team1_name_db and team2_name_db and tournament_db:
                    found = fetch_completed_match_by_fallback(
                        completed_matches=completed_matches,
                        team1=team1_name_db,
                        team2=team2_name_db,
                        tournament=tournament_db,
                        match_time_msk=match_time_msk_db,
                        time_window_minutes=12 * 60,
                    )
                    if found:
                        logger.info(
                            "[SCORE][DBG] FOUND completed: score=%r tour=%r time=%r t1=%r t2=%r url=%r",
                            found.score, found.tournament, found.time_msk, found.team1, found.team2, found.match_url
                        )
                    else:
                        logger.info("[SCORE][DBG] NOT FOUND in completed by fallback")

                    if found and found.score:
                        new_score = found.score
                        new_bo = parse_bo_int(found.bo)
                        new_liqui_id = extract_liquipedia_id_from_url(getattr(found, "match_url", None))

                        cur.execute(
                            f"""
                            UPDATE public.{MATCHES_TABLE}
                            SET score = COALESCE(%(score)s, score),
                                bo = COALESCE(%(bo)s, bo),
                                status = 'finished',
                                match_url = COALESCE(%(match_url)s, match_url),
                                liquipedia_match_id = COALESCE(%(liqui_id)s, liquipedia_match_id),
                                last_score_check_at = now(),
                                score_last_updated_at = now(),
                                updated_at = now()
                            WHERE id = %(id)s;
                            """,
                            {
                                "id": match_id,
                                "score": new_score,
                                "bo": new_bo,
                                "match_url": getattr(found, "match_url", None),
                                "liqui_id": new_liqui_id,
                            },
                        )
                        continue

                # ---------- 4) last resort: match page ----------
                if match_url:
                    s, bo_text = fetch_score_from_match_page(match_url)
                    if s:
                        new_bo = parse_bo_int(bo_text) if bo_text else None
                        cur.execute(
                            f"""
                            UPDATE public.{MATCHES_TABLE}
                            SET score = %(score)s,
                                bo = COALESCE(%(bo)s, bo),
                                status = CASE WHEN status = 'finished' THEN 'finished' ELSE 'live' END,
                                last_score_check_at = now(),
                                score_last_updated_at = now(),
                                updated_at = now()
                            WHERE id = %(id)s;
                            """,
                            {"id": match_id, "score": s, "bo": new_bo},
                        )
                        continue

                # ничего не нашли — отметим проверку
                cur.execute(
                    f"UPDATE public.{MATCHES_TABLE} SET last_score_check_at = now() WHERE id = %(id)s;",
                    {"id": match_id},
                )

        conn.commit()

    logger.info("[SCORE] Обновление счёта завершено")


# ---------------------------------------------------------------------------
# STATUS REFRESH
# ---------------------------------------------------------------------------

def refresh_statuses_in_db() -> None:
    """
    CS2:
    - Не откатываем finished назад.
    - finished по series-score финалу (2:1 в Bo3) можно проставлять, но это дополнительный путь.
    - upcoming/live — по времени.
    """
    with get_db_connection() as conn:
        ensure_cs2_teams_table(conn)
        ensure_cs2_matches_table(conn)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE public.{MATCHES_TABLE}
                SET status = CASE
                    WHEN status = 'finished' THEN 'finished'

                    -- Серийный финал (редко, но бывает)
                    WHEN bo IS NOT NULL
                         AND score IS NOT NULL AND score <> ''
                         AND score ~ '^[0-9]+:[0-9]+$'
                         AND (
                             GREATEST(split_part(score, ':', 1)::int, split_part(score, ':', 2)::int) <= bo
                             AND GREATEST(split_part(score, ':', 1)::int, split_part(score, ':', 2)::int) <= 10
                         )
                         AND GREATEST(split_part(score, ':', 1)::int, split_part(score, ':', 2)::int) >= ((bo / 2)::int + 1)
                    THEN 'finished'

                    WHEN match_time_msk > now() + INTERVAL '5 minutes'
                    THEN 'upcoming'

                    WHEN match_time_msk <= now() - INTERVAL '5 minutes'
                         AND (status IS NULL OR status IN ('unknown', 'upcoming'))
                    THEN 'live'

                    ELSE status
                END,
                updated_at = now()
                WHERE match_time_msk IS NOT NULL;
                """
            )

        conn.commit()

    logger.info("Статусы матчей обновлены по времени/BO")


# ---------------------------------------------------------------------------
# WORKER
# ---------------------------------------------------------------------------

def worker_once() -> None:
    log_event({"level": "info", "msg": "cs2_worker_once_start"})
    start_ts = time.time()

    metrics = {"parsed_matches": 0, "deduped_matches": 0}

    # tournaments cache
    try:
        sync_tournaments_from_main_page()
    except Exception as e:
        logger.warning("Tournament sync failed: %s", e)

    # fetch matches
    try:
        html = fetch_html(MATCHES_URL)
    except Exception as e:
        log_event({"level": "error", "msg": "fetch_cs_matches_failed", "error": str(e)})
        return

    matches = parse_matches_from_html(html)
    metrics["parsed_matches"] = len(matches)

    matches = deduplicate_matches(matches)
    metrics["deduped_matches"] = len(matches)

    save_matches_to_db(matches)
    update_scores_from_match_pages()
    refresh_statuses_in_db()

    elapsed = round(time.time() - start_ts, 2)
    metrics["elapsed_sec"] = elapsed

    log_event({"level": "info", "msg": "cs2_worker_once_finished", "metrics": metrics})
    logger.info(
        "Проход завершён: parsed=%s dedup=%s elapsed=%ss",
        metrics["parsed_matches"], metrics["deduped_matches"], metrics["elapsed_sec"]
    )


def worker_loop() -> None:
    while True:
        try:
            worker_once()
        except Exception as e:
            log_event({"level": "error", "msg": "cs2_worker_loop_exception", "error": str(e)})
            logger.exception("Ошибка в worker_loop: %s", e)
        time.sleep(SCRAPE_INTERVAL_SECONDS)


if __name__ == "__main__":
    worker_once()
    # worker_loop()
