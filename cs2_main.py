#!/usr/bin/env python3
"""
cs2_main.py — Liquipedia Counter-Strike/CS2 parser (Liquipedia:Matches)

Цель фикса: корректно парсить score/bo из реального HTML Liquipedia CS2
и стабильно обновлять счёт в БД, даже когда у матчей нет Match:ID_* в блоке.

Основные изменения (по твоему cs2.html):
- Время: берём из .timer-object[data-timestamp] (unix epoch) + fallback по строке/abbr.
- Команды: берём из .match-info-header-opponent .name a (title/text), url — из href.
- Score/Bo: берём из .match-info-header-scoreholder (две цифры + (BoX)).
- Статус:
    - upcoming если upper == "vs"
    - finished если data-finished="finished" ИЛИ если series-score уже финальный по Bo
    - live если есть score, но ещё не финальный (или Bo неизвестен)
- Fallback match_uid теперь стабилен (без “обрезания” турнира), чтобы не плодить дубликаты.
- Апдейтер счёта матчит по паре team paths (предпочтительно) / именам + ближнему времени.
"""

from __future__ import annotations

try:
    from zoneinfo import ZoneInfo  # py3.9+
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # py3.8

import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Set, Iterable
from urllib.parse import urljoin, urlparse, parse_qs, unquote

import psycopg
from psycopg import errors
import requests
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv


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

TZ_IANA_MAP = {
    "UTC": "UTC",
    "GMT": "UTC",

    "CET": "Europe/Berlin",
    "CEST": "Europe/Berlin",
    "EET": "Europe/Athens",
    "EEST": "Europe/Athens",
    "MSK": "Europe/Moscow",
    "WET": "Europe/Lisbon",

    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "EST": "America/New_York",
    "EDT": "America/New_York",

    "CST": "Asia/Shanghai",
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
    match_url: Optional[str]  # /index.php?title=Match:ID_... (обычно отсутствует в CS2 блоке)


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

                team1_id BIGINT,
                team2_id BIGINT,
                team1_url TEXT,
                team2_url TEXT,

                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

                CONSTRAINT {MATCHES_TABLE}_match_uid_uq UNIQUE (match_uid)
            );
        """)

        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_time_idx ON public.{MATCHES_TABLE}(match_time_msk);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_url_idx ON public.{MATCHES_TABLE}(match_url);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_teams_idx ON public.{MATCHES_TABLE}(team1, team2);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_team_ids_idx ON public.{MATCHES_TABLE}(team1_id, team2_id);")

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

def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    return resp.text


def _strip_page_does_not_exist(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s*\(page does not exist\)\s*$", "", name).strip()


def extract_team_name_from_tag(tag: Optional[Tag]) -> str:
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
    """
    ВАЖНО: больше НЕ “обрезаем” хвосты типа "- Playoffs", "- Group D".
    Иначе match_uid меняется и ты получаешь дубликаты (что ты уже наблюдаешь).
    Тут только косметика: тире/пробелы.
    """
    if not name:
        return name
    s = name.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _norm_key(s: Optional[str]) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _tour_key(s: Optional[str]) -> str:
    base = clean_tournament_name(s or "")
    base = base.strip().lower()
    base = re.sub(r"\s+", " ", base)
    return base


# --- TIME parsing (fallback) ---

_TIME_RE = re.compile(
    r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})\s*-\s*(\d{1,2}):(\d{2})\s*([A-Z]{2,6})?"
)


def parse_time_to_target_tz(time_str: str, container: Optional[Tag] = None) -> Optional[datetime]:
    if not time_str:
        return None

    cleaned = re.sub(r"<.*?>", "", time_str)
    cleaned = " ".join(cleaned.split())

    m = _TIME_RE.search(cleaned)
    if not m:
        return None

    month_name, day, year, hour, minute, tz_abbr = m.groups()
    month = MONTHS.get(month_name)
    if not month:
        return None

    try:
        dt_naive = datetime(int(year), month, int(day), int(hour), int(minute))
    except ValueError:
        return None

    # 1) offset from HTML: <abbr data-tz="+3:00">
    offset = None
    if container:
        ab = container.select_one(".timer-object-date abbr[data-tz]")
        if ab:
            offset = (ab.get("data-tz") or "").strip() or None

    if offset and re.match(r"^[\+\-]\d{1,2}:\d{2}$", offset):
        sign = 1 if offset.startswith("+") else -1
        hh, mm = offset[1:].split(":")
        delta = timedelta(hours=int(hh) * sign, minutes=int(mm) * sign)
        dt_utc = (dt_naive - delta).replace(tzinfo=ZoneInfo("UTC"))
        return dt_utc.astimezone(TARGET_TZ)

    # 2) tz abbr mapping
    tz_abbr = (tz_abbr or "").strip()
    tz_name = TZ_IANA_MAP.get(tz_abbr, "UTC")
    try:
        src_tz = ZoneInfo(tz_name)
        dt_src = dt_naive.replace(tzinfo=src_tz)
        return dt_src.astimezone(TARGET_TZ)
    except Exception:
        return None


def parse_bo_int(bo: Optional[str]) -> Optional[int]:
    if not bo:
        return None
    m = re.search(r"(?:\(|\b)bo\s*([0-9]+)", bo, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


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
    if not bo or bo <= 0:
        return False
    st = parse_score_tuple(score)
    if not st:
        return False
    a, b = st
    # на LP series-score обычно маленький (не 13:10), поэтому ограничим “разумно”
    return 0 <= a <= bo and 0 <= b <= bo and max(a, b) <= 10


def _is_final_series(score: Optional[str], bo: Optional[int]) -> bool:
    if not score or not bo:
        return False
    if not _is_series_score(score, bo):
        return False
    st = parse_score_tuple(score)
    if not st:
        return False
    a, b = st
    needed = bo // 2 + 1
    return max(a, b) >= needed


# --- TEAM URL / PATH ---

def _canon_team_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None

    u = u.split("#", 1)[0]
    if "redlink=1" in u:
        return None

    if u.startswith("/"):
        u = urljoin(BASE_URL, u)

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

    return u.split("?", 1)[0].rstrip("/")


def _url_to_team_path(u: Optional[str]) -> Optional[str]:
    u = _canon_team_url(u)
    if not u:
        return None
    path = urlparse(u).path
    path = path.rstrip("/")
    return path if path.startswith("/counterstrike/") else None


def _extract_team_path_and_url(a_tag: Optional[Tag]) -> Tuple[Optional[str], Optional[str]]:
    if not a_tag:
        return None, None
    href = (a_tag.get("href") or "").strip()
    if not href:
        return None, None
    url = _canon_team_url(href)
    if not url:
        return None, None
    path = _url_to_team_path(url)
    return path, url


_SLUG_SAFE_RE = re.compile(r"[^a-z0-9_]+")

def _slug_from_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace("&", "and")
    s = s.replace(" ", "_")
    s = re.sub(r"\s+", "_", s)
    s = _SLUG_SAFE_RE.sub("", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _team_uid_token(name: Optional[str], path: Optional[str], url: Optional[str]) -> str:
    # 1) реальный path/url
    ref = _norm_team_ref(path) or _norm_team_ref(url)
    if ref:
        return ref  # "/counterstrike/..."

    # 2) TBD/пусто
    if not name or name.strip() == "" or name.strip().lower() == "tbd":
        return "tbd"

    # 3) детерминированный guess по имени (стабильно между запусками)
    slug = _slug_from_name(name)
    return f"/counterstrike/{slug}" if slug else "tbd"


def _norm_team_ref(ref: Optional[str]) -> Optional[str]:
    """
    Нормализуем ссылку/путь команды к виду "/counterstrike/team_vitality".
    """
    if not ref:
        return None
    ref = ref.strip()
    if not ref:
        return None

    try:
        if ref.startswith("http://") or ref.startswith("https://"):
            ref = urlparse(ref).path
    except Exception:
        pass

    ref = ref.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    ref = ref.replace(" ", "_")
    ref = ref.lower()
    if not ref.startswith("/counterstrike/"):
        return None
    return ref


def _team_pair_key_by_paths(team1_url: Optional[str], team2_url: Optional[str],
                           team1_path: Optional[str] = None, team2_path: Optional[str] = None) -> Optional[frozenset[str]]:
    a = _norm_team_ref(team1_path or _url_to_team_path(team1_url) or team1_url)
    b = _norm_team_ref(team2_path or _url_to_team_path(team2_url) or team2_url)
    if not a or not b:
        return None
    return frozenset([a, b])


def upsert_team(cur: psycopg.Cursor, name: str, path: str, url: str) -> int:
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
# TOURNAMENTS (Main Page) — optional
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
# PARSE MATCHES (Liquipedia:Matches) — aligned with cs2.html
# ---------------------------------------------------------------------------

def _extract_score_and_bo(container: Tag) -> Tuple[Optional[str], Optional[str]]:
    """
    Структура из твоего HTML:
      <div class="match-info-header-scoreholder">
        <span class="match-info-header-scoreholder-scorewrapper">
          <span class="match-info-header-scoreholder-upper">
            <span class="match-info-header-scoreholder-score">3</span>
            <span class="match-info-header-scoreholder-divider">:</span>
            <span class="match-info-header-scoreholder-score">1</span>
          </span>
          <span class="match-info-header-scoreholder-lower">(Bo5)</span>
        </span>
      </div>
    """
    sh = container.select_one(".match-info-header-scoreholder")
    if not sh:
        return None, None

    upper = sh.select_one(".match-info-header-scoreholder-upper")
    upper_txt = upper.get_text(" ", strip=True).lower() if upper else ""
    # upcoming => "vs"
    if upper_txt.strip() == "vs":
        bo_txt = None
        lower = sh.select_one(".match-info-header-scoreholder-lower")
        if lower:
            bo_txt = lower.get_text(" ", strip=True) or None
        return None, bo_txt

    nums = [s.get_text(strip=True) for s in sh.select(".match-info-header-scoreholder-score")]
    score = None
    if len(nums) >= 2 and nums[0].isdigit() and nums[1].isdigit():
        score = f"{int(nums[0])}:{int(nums[1])}"

    bo_txt = None
    lower = sh.select_one(".match-info-header-scoreholder-lower")
    if lower:
        bo_txt = lower.get_text(" ", strip=True) or None

    return score, bo_txt


def normalize_match(m: Match) -> Match:
    """
    Чистим мусорные состояния, чтобы:
    - не хранить finished без команд
    - не хранить 0:0 как finished
    - не оставлять кривой score
    """
    # если live/finished, но команд нет — это мусор
    if m.status in ("live", "finished") and (not m.team1 or not m.team2):
        m.status = "unknown"
        m.score = None
        return m

    # если score не парсится — выкидываем score
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

    # если есть Bo и это series-score, но до победы не дошли — не считаем finished
    bo_int = parse_bo_int(m.bo)
    if bo_int and m.score and _is_series_score(m.score, bo_int) and m.status == "finished":
        needed = bo_int // 2 + 1
        if max(a, b) < needed:
            m.status = "live" if (a > 0 or b > 0) else "unknown"
            return m

    return m


def parse_matches_from_html(html: str) -> List[Match]:
    soup = BeautifulSoup(html, "html.parser")
    containers = soup.select(".match-info")
    logger.info("[DEBUG] .match-info containers: %d", len(containers))

    matches: List[Match] = []

    for c in containers:
        # -------------------- TIME --------------------
        time_raw: Optional[str] = None
        time_msk: Optional[datetime] = None

        timer = c.select_one(".timer-object")
        if timer:
            ts = timer.get("data-timestamp")
            if ts and str(ts).isdigit():
                try:
                    dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    time_msk = dt_utc.astimezone(TARGET_TZ)
                except Exception:
                    time_msk = None

            time_el = c.select_one(".timer-object-date")
            time_raw = time_el.get_text(" ", strip=True) if time_el else None

        if time_msk is None:
            # fallback: парсим строку + abbr tz
            time_el = c.select_one(".timer-object-date, .timer-object")
            time_raw = time_el.get_text(" ", strip=True) if time_el else None
            time_msk = parse_time_to_target_tz(time_raw or "", container=c)

        # -------------------- TEAMS (+ URL/PATH) --------------------
        team_links = c.select(".match-info-header-opponent .name a")
        t1_tag = team_links[0] if len(team_links) >= 1 else None
        t2_tag = team_links[1] if len(team_links) >= 2 else None

        team1 = normalize_team_name(extract_team_name_from_tag(t1_tag)) if t1_tag else None
        team2 = normalize_team_name(extract_team_name_from_tag(t2_tag)) if t2_tag else None

        team1_path, team1_url = _extract_team_path_and_url(t1_tag)
        team2_path, team2_url = _extract_team_path_and_url(t2_tag)

        # -------------------- SCORE + BO --------------------
        score, bo_text = _extract_score_and_bo(c)

        # приводим Bo к единому виду (Bo5)
        if bo_text:
            m = re.search(r"bo\s*([0-9]+)", bo_text, flags=re.IGNORECASE)
            if m:
                bo_text = f"Bo{m.group(1)}"

        # -------------------- TOURNAMENT --------------------
        tournament = None
        t_el = c.select_one(".match-info-tournament-name span")
        if t_el:
            tournament = t_el.get_text(strip=True) or None
        else:
            a = c.select_one(".match-info-tournament a")
            if a:
                tournament = a.get_text(" ", strip=True) or None

        if tournament:
            tournament = clean_tournament_name(tournament)

        # -------------------- STATUS --------------------
        status = None
        finished_flag = None
        if timer:
            finished_flag = (timer.get("data-finished") or "").strip().lower()

        sh = c.select_one(".match-info-header-scoreholder")
        upper = sh.select_one(".match-info-header-scoreholder-upper") if sh else None
        upper_txt = upper.get_text(" ", strip=True).lower() if upper else ""

        bo_int = parse_bo_int(bo_text)
        if upper_txt.strip() == "vs":
            status = "upcoming"
        elif finished_flag == "finished":
            status = "finished"
        elif _is_final_series(score, bo_int):
            status = "finished"
        elif score:
            status = "live"
        else:
            status = None

        # -------------------- MATCH URL --------------------
        match_url = None  # в CS2 блоке чаще нет Match:ID_...

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
        matches.append(m_obj)

    logger.info("[DEBUG] parsed matches: %d", len(matches))
    return matches


# ---------------------------------------------------------------------------
# UID (CS2 fallback mostly)
# ---------------------------------------------------------------------------

def build_match_uid(m: Match) -> Optional[str]:
    if not m.match_url:
        return None
    mm = re.search(r"Match:(ID_[^&#/?]+)", m.match_url)
    if mm:
        return f"lp:{mm.group(1)}"
    return None


def _uid_team_part(m: Match, which: int) -> str:
    """
    Стабильная часть uid: предпочитаем team path (если есть), иначе url path, иначе имя.
    """
    if which == 1:
        ref = m.team1_path or _url_to_team_path(m.team1_url) or (m.team1 or "")
    else:
        ref = m.team2_path or _url_to_team_path(m.team2_url) or (m.team2 or "")
    ref = (ref or "").strip().lower()
    ref = ref.replace(" ", "_")
    ref = re.sub(r"\s+", "_", ref)
    return ref


def build_fallback_match_uid(m: Match) -> str:
    # Используем только дату для стабильности, без точного времени, чтобы избежать изменения UID при коррекции времени на LP
    time_part = m.time_msk.date().isoformat() if m.time_msk else ""

    left  = _team_uid_token(m.team1, m.team1_path, m.team1_url)
    right = _team_uid_token(m.team2, m.team2_path, m.team2_url)

    # Сортируем ключи команд, чтобы порядок не влиял на UID
    teams_key = "+".join(sorted([left, right])) if left and right else f"{left or ''}<>{right or ''}"

    tour = _tour_key(m.tournament)
    bo = parse_bo_int(m.bo) or 0

    return "|".join([time_part, f"teams={teams_key}", tour, f"bo{bo}"])


def deduplicate_matches(matches: List[Match]) -> List[Match]:
    seen: Set[str] = set()
    out: List[Match] = []
    for m in matches:
        uid = build_match_uid(m) or build_fallback_match_uid(m)
        if uid in seen:
            logger.warning(
                "Дубликат в parsed матчах - UID: %s для %s vs %s в %s турнир=%s статус=%s счёт=%s",
                uid, m.team1 or '', m.team2 or '', m.time_msk, m.tournament, m.status, m.score
            )
            continue
        seen.add(uid)
        out.append(m)
    logger.info("Дедупликация матчей: было %s, стало %s", len(matches), len(out))
    return out


# ---------------------------------------------------------------------------
# AUTO-REPAIR (minimal)
# ---------------------------------------------------------------------------

def deduplicate_duplicates_in_db() -> None:
    """
    Удаляет дубликаты матчей по комбинации tournament, team1, team2, bo.
    Оставляет самый ранний id для каждой группы дубликатов.
    """
    with get_db_connection() as conn:
        ensure_cs2_teams_table(conn)
        ensure_cs2_matches_table(conn)

        with conn.cursor() as cur:
            cur.execute(f"""
                WITH duplicates AS (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY LOWER(tournament), LOWER(team1), LOWER(team2), bo
                               ORDER BY id
                           ) AS rn
                    FROM public.{MATCHES_TABLE}
                    WHERE tournament IS NOT NULL AND team1 IS NOT NULL AND team2 IS NOT NULL AND bo IS NOT NULL
                )
                DELETE FROM public.{MATCHES_TABLE}
                WHERE id IN (SELECT id FROM duplicates WHERE rn > 1);
            """)
            deleted = cur.rowcount

        conn.commit()

    logger.info("[DEDUPE-DB] удалено дубликатов: %d", deleted)


def auto_repair_matches() -> None:
    with get_db_connection() as conn:
        ensure_cs2_teams_table(conn)
        ensure_cs2_matches_table(conn)

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM public.{MATCHES_TABLE} WHERE match_uid IS NULL OR match_uid = '';")
            deleted_no_uid = cur.rowcount

        conn.commit()

    logger.info("[AUTO-REPAIR] deleted_no_uid=%s", deleted_no_uid)


# ---------------------------------------------------------------------------
# SAVE MATCHES
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


def _save_matches_to_db_impl(matches: List[Match]) -> None:
    with get_db_connection() as conn:
        ensure_cs2_teams_table(conn)
        ensure_cs2_matches_table(conn)

        with conn.cursor() as cur:
            for m in matches:
                bo_int = parse_bo_int(m.bo)

                team1_url_db = _canon_team_url(m.team1_url)
                team2_url_db = _canon_team_url(m.team2_url)
                team1_path_db = m.team1_path or _url_to_team_path(team1_url_db)
                team2_path_db = m.team2_path or _url_to_team_path(team2_url_db)

                # upsert teams
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

                match_uid = build_match_uid(m) or build_fallback_match_uid(m)

                liqui_id = None
                mm = re.search(r"^lp:(ID_[^|]+)$", match_uid)
                if mm:
                    liqui_id = mm.group(1)

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
                        match_url,
                        liquipedia_match_id
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
                        %(match_url)s,
                        %(liqui_id)s
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

                        -- ВАЖНО: score должен обновляться, когда он появился.
                        score = COALESCE(EXCLUDED.score, public.{MATCHES_TABLE}.score),
                        bo    = COALESCE(EXCLUDED.bo, public.{MATCHES_TABLE}.bo),

                        tournament = COALESCE(EXCLUDED.tournament, public.{MATCHES_TABLE}.tournament),

                        -- не даунгрейдим finished обратно
                        status = CASE
                            WHEN public.{MATCHES_TABLE}.status = 'finished' THEN public.{MATCHES_TABLE}.status
                            WHEN EXCLUDED.status IS NULL THEN public.{MATCHES_TABLE}.status
                            ELSE EXCLUDED.status
                        END,

                        match_url = COALESCE(EXCLUDED.match_url, public.{MATCHES_TABLE}.match_url),
                        liquipedia_match_id = COALESCE(EXCLUDED.liquipedia_match_id, public.{MATCHES_TABLE}.liquipedia_match_id),

                        updated_at = now();
                    """,
                    {
                        "match_time_msk": m.time_msk,
                        "match_time_raw": m.time_raw,
                        "team1": m.team1,
                        "team2": m.team2,
                        "team1_id": team1_id,
                        "team2_id": team2_id,
                        "team1_url": team1_url_db,
                        "team2_url": team2_url_db,
                        "score": m.score,
                        "bo": bo_int,
                        "tournament": m.tournament,
                        "status": m.status,
                        "match_uid": match_uid,
                        "match_url": m.match_url,
                        "liqui_id": liqui_id,
                    },
                )

        conn.commit()

    logger.info("Сохранили/обновили %d матчей", len(matches))


# ---------------------------------------------------------------------------
# SCORE UPDATES (match by team pair + nearest time)
# ---------------------------------------------------------------------------

def _team_pair_key(team1_url: Optional[str], team2_url: Optional[str]) -> Optional[frozenset[str]]:
    """
    Совместимость со старым кодом: возвращает ключ пары команд.
    Используем нормализацию до /counterstrike/... через _team_pair_key_by_paths().
    """
    return _team_pair_key_by_paths(team1_url, team2_url)


def _index_completed_matches(completed: List[Match]) -> Tuple[Dict[frozenset[str], List[Match]], Dict[frozenset[str], List[Match]]]:
    """
    Индексы:
      - by team paths (лучше всего): frozenset({"/counterstrike/a", "/counterstrike/b"}) -> [Match...]
      - by team names (fallback):  frozenset({"team vitality", "faze clan"}) -> [Match...]
    """
    by_paths: Dict[frozenset[str], List[Match]] = {}
    by_names: Dict[frozenset[str], List[Match]] = {}

    for m in completed:
        if not m.score:
            continue
        k_path = _team_pair_key_by_paths(m.team1_url, m.team2_url, m.team1_path, m.team2_path)
        if k_path:
            by_paths.setdefault(k_path, []).append(m)

        if m.team1 and m.team2:
            k_name = frozenset([_norm_key(m.team1), _norm_key(m.team2)])
            by_names.setdefault(k_name, []).append(m)

    # сортируем по времени для удобства выбора ближайшего
    def _sort(v: List[Match]) -> None:
        v.sort(key=lambda x: x.time_msk or datetime.min.replace(tzinfo=TARGET_TZ))

    for v in by_paths.values():
        _sort(v)
    for v in by_names.values():
        _sort(v)

    return by_paths, by_names


def _pick_nearest(candidates: List[Match], match_time_msk: Optional[datetime], time_window_minutes: int) -> Optional[Match]:
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
    return best


def update_scores_from_match_pages() -> None:
    """
    Надёжный апдейт счёта для CS2:
    - Prefetch MATCHES_URL?status=completed
    - Индексы:
        1) по паре team PATH (из team_url или team_path)
        2) fallback по паре team NAMES
    - Выбор кандидата: ближайший по времени (окно 48ч, чтобы пережить TZ/плавающие таймстампы)
    """

    def is_final_score_series(score_str: str, bo_value: Optional[int]) -> bool:
        if not score_str or not bo_value:
            return False
        st = parse_score_tuple(score_str)
        if not st:
            return False
        a, b = st
        needed = bo_value // 2 + 1
        return max(a, b) >= needed

    def norm_team_ref(url_or_path: Optional[str]) -> Optional[str]:
        """
        Приводим ссылку/путь к каноничному виду:
        - всегда path вида /counterstrike/xxx
        - lower()
        - без query/fragment
        - пробелы -> _
        """
        if not url_or_path:
            return None
        s = url_or_path.strip()
        if not s:
            return None

        # если полный URL -> берём только path
        try:
            if s.startswith("http://") or s.startswith("https://"):
                p = urlparse(s)
                s = p.path or s
            else:
                # если дали уже path, оставляем как есть
                s = s.split("?", 1)[0].split("#", 1)[0]
        except Exception:
            s = s.split("?", 1)[0].split("#", 1)[0]

        s = s.strip()
        if not s:
            return None

        # привести к виду /counterstrike/...
        if not s.startswith("/counterstrike/"):
            # иногда у тебя проскакивает "xi_esport" без префикса
            # это НЕ path, это "слаг по имени" -> для URL-матчинга не годится
            return None

        s = s.replace(" ", "_")
        s = re.sub(r"/+$", "", s)
        return s.lower()

    def pair_key_from_match(m: Match) -> Optional[frozenset[str]]:
        a = norm_team_ref(m.team1_url) or norm_team_ref(m.team1_path)
        b = norm_team_ref(m.team2_url) or norm_team_ref(m.team2_path)
        if not a or not b:
            return None
        return frozenset([a, b])

    def pair_key_from_db(team1_url: Optional[str], team2_url: Optional[str]) -> Optional[frozenset[str]]:
        a = norm_team_ref(team1_url)
        b = norm_team_ref(team2_url)
        if not a or not b:
            return None
        return frozenset([a, b])

    def name_pair(team1: Optional[str], team2: Optional[str]) -> Optional[frozenset[str]]:
        if not team1 or not team2:
            return None
        return frozenset([_norm_key(team1), _norm_key(team2)])

    def best_by_time(cands: List[Match], target: Optional[datetime], window_hours: int = 48) -> Optional[Match]:
        if not cands:
            return None
        if target is None:
            return cands[0]
        best = None
        best_delta = None
        for m in cands:
            if m.time_msk is None:
                continue
            delta = abs((m.time_msk - target).total_seconds())
            if delta > window_hours * 3600:
                continue
            if best is None or best_delta is None or delta < best_delta:
                best = m
                best_delta = delta
        return best or cands[0]

    # ---------- Prefetch completed ----------
    try:
        html = fetch_html(MATCHES_URL + "?status=completed")
        ms = parse_matches_from_html(html)
        completed = [m for m in ms if m.score and m.bo]
        logger.info("[SCORE] Prefetch completed: parsed=%d with_score=%d", len(ms), len(completed))
    except Exception as e:
        logger.warning("[SCORE] Prefetch completed failed: %s", e)
        return

    if not completed:
        logger.info("[SCORE] completed пустой — нечего матчить")
        return

    # ---------- Build indices ----------
    by_pair: Dict[frozenset[str], List[Match]] = {}
    by_names: Dict[frozenset[str], List[Match]] = {}

    for m in completed:
        pk = pair_key_from_match(m)
        if pk:
            by_pair.setdefault(pk, []).append(m)

        nk = name_pair(m.team1, m.team2)
        if nk:
            by_names.setdefault(nk, []).append(m)

    logger.info("[SCORE] completed index: by_pair=%d by_names=%d", len(by_pair), len(by_names))

    with get_db_connection() as conn:
        ensure_cs2_teams_table(conn)
        ensure_cs2_matches_table(conn)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id,
                    score,
                    status,
                    bo,
                    match_time_msk,
                    team1_url,
                    team2_url,
                    team1,
                    team2
                FROM public.{MATCHES_TABLE}
                WHERE (status = 'live' OR status = 'upcoming' OR status IS NULL OR status = 'unknown')
                  AND match_time_msk IS NOT NULL
                  AND match_time_msk < now() - INTERVAL '10 minutes'
                ORDER BY match_time_msk
                LIMIT 400;
                """
            )
            rows = cur.fetchall()
            if not rows:
                logger.info("[SCORE] Нет матчей для обновления")
                return

            logger.info("[SCORE] Обновляем счёт для %d матчей", len(rows))

            updated = 0
            checked = 0

            for (
                match_id,
                score_db,
                status_db,
                bo_db,
                match_time_msk_db,
                team1_url_db,
                team2_url_db,
                team1_name_db,
                team2_name_db,
            ) in rows:
                checked += 1

                # уже финальная серия — не трогаем
                if score_db and bo_db and is_final_score_series(score_db, bo_db):
                    continue

                found: Optional[Match] = None

                # 1) пробуем по team path pair
                pk = pair_key_from_db(team1_url_db, team2_url_db)
                if pk and pk in by_pair:
                    found = best_by_time(by_pair[pk], match_time_msk_db)

                # 2) fallback по именам
                if not found:
                    nk = name_pair(team1_name_db, team2_name_db)
                    if nk and nk in by_names:
                        found = best_by_time(by_names[nk], match_time_msk_db)

                # DEBUG если не нашли (чтобы ты увидел почему)
                if not found:
                    logger.info(
                        "[SCORE][MISS] id=%s time=%s urls=(%s,%s) names=(%s,%s) pk=%s nk=%s",
                        match_id,
                        match_time_msk_db,
                        team1_url_db,
                        team2_url_db,
                        team1_name_db,
                        team2_name_db,
                        pk,
                        name_pair(team1_name_db, team2_name_db),
                    )

                if found and found.score:
                    new_bo = parse_bo_int(found.bo)

                    # финальный счёт => finished
                    new_status = "finished" if (new_bo and is_final_score_series(found.score, new_bo)) else "live"

                    cur.execute(
                        f"""
                        UPDATE public.{MATCHES_TABLE}
                        SET score = %(score)s,
                            bo = COALESCE(%(bo)s, bo),
                            status = %(status)s,
                            last_score_check_at = now(),
                            score_last_updated_at = now(),
                            updated_at = now()
                        WHERE id = %(id)s;
                        """,
                        {"id": match_id, "score": found.score, "bo": new_bo, "status": new_status},
                    )
                    updated += cur.rowcount
                else:
                    cur.execute(
                        f"UPDATE public.{MATCHES_TABLE} SET last_score_check_at = now() WHERE id = %(id)s;",
                        {"id": match_id},
                    )

        conn.commit()

    logger.info("[SCORE] Обновление счёта завершено: checked=%d updated=%d", checked, updated)


# ---------------------------------------------------------------------------
# STATUS REFRESH (не ломаем finished)
# ---------------------------------------------------------------------------

def refresh_statuses_in_db() -> None:
    with get_db_connection() as conn:
        ensure_cs2_teams_table(conn)
        ensure_cs2_matches_table(conn)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE public.{MATCHES_TABLE}
                SET status = CASE
                    WHEN status = 'finished' THEN 'finished'

                    WHEN bo IS NOT NULL
                         AND score IS NOT NULL AND score <> ''
                         AND score ~ '^[0-9]+:[0-9]+$'
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

    try:
        sync_tournaments_from_main_page()
    except Exception as e:
        logger.warning("Tournament sync failed: %s", e)

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
    deduplicate_duplicates_in_db()
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
