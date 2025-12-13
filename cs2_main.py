#!/usr/bin/env python3
"""
cs2_main.py — HLTV CS2 scraper (fixed MSK time)

Что делает:
- Читает .env (DATABASE_URL или DB_*), HLTV_BASE_URL
- Берёт эвенты из cs2_events (или синкает их с /events, если таблица пустая)
- Отбирает эвенты для окна [сегодня; завтра] по date range, вытащенному с event page
- Парсит матчи:
    1) /events/{id}/matches  -> upcoming/live/finished (если доступно)
    2) /events/{id}/results  -> finished only (fallback)
    3) (опционально) /matches (global) -> upcoming/live (fallback), фильтр по event_id
- Пишет в БД

FIX TIME (ВАЖНО):
- Всегда пытаемся получить unix ms (data-unix) и конвертим в UTC + MSK.
- Если unix нет: трактуем HH:MM как MSK-time на section_date и строим when_utc из этого.
- В БД:
    - when_utc (TIMESTAMPTZ) — реальный instant
    - when_msk (TIMESTAMPTZ) — тот же instant, но в MSK
    - match_time_raw (TEXT)  — строка UTC таймстемпа: "YYYY-mm-dd HH:MM:SS.ffffff +00:00"
    - match_time_msk (TEXT)  — строка MSK таймстемпа: "YYYY-mm-dd HH:MM:SS.ffffff +03:00"
"""

from __future__ import annotations

from zoneinfo import ZoneInfo
import os
import re
import time
import random
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date, timezone
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag
import psycopg
from dotenv import load_dotenv


# -----------------------------
# CONFIG / ENV
# -----------------------------

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

HLTV_BASE_URL = os.getenv("HLTV_BASE_URL", "https://www.hltv.org").rstrip("/")
EVENTS_TABLE = os.getenv("CS2_EVENTS_TABLE", "cs2_events")
MATCHES_TABLE = os.getenv("CS2_MATCHES_TABLE", "cs2_matches")
TEAMS_TABLE = os.getenv("CS2_TEAMS_TABLE", "cs2_teams")

SCRAPE_INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "600"))
EVENTS_SCHEMA = os.getenv("CS2_EVENTS_SCHEMA", "").strip()  # если пусто — авто-детект

REQUEST_TIMEOUT = float(os.getenv("HLTV_TIMEOUT_SECONDS", "15"))
REQUEST_RETRIES = int(os.getenv("HLTV_RETRIES", "3"))
REQUEST_RETRY_SLEEP = float(os.getenv("HLTV_RETRY_SLEEP", "1.2"))
HLTV_BASE_SLEEP_SECONDS = float(os.getenv("HLTV_BASE_SLEEP_SECONDS", "0.35"))
HLTV_MAX_SLEEP_SECONDS = float(os.getenv("HLTV_MAX_SLEEP_SECONDS", "12.0"))

HLTV_MAX_EVENT_PAGES_PER_RUN = int(os.getenv("HLTV_MAX_EVENT_PAGES_PER_RUN", "40"))
HLTV_EVENT_PAGE_DELAY_SECONDS = float(os.getenv("HLTV_EVENT_PAGE_DELAY_SECONDS", "0.25"))

HLTV_EVENTS_SYNC_LIMIT = int(os.getenv("HLTV_EVENTS_SYNC_LIMIT", "200"))
HLTV_EVENTS_LIST_URL = os.getenv("HLTV_EVENTS_LIST_URL", HLTV_BASE_URL + "/events")

# опциональный глобальный fallback
HLTV_GLOBAL_MATCHES_FALLBACK = os.getenv("HLTV_GLOBAL_MATCHES_FALLBACK", "1").strip() not in ("0", "false", "False", "")

# ВАЖНО: это не "где HLTV", а то, что ты хочешь получить в БД
TARGET_TIMEZONE = os.getenv("TARGET_TIMEZONE", "Europe/Moscow").strip()
TZ_MSK = ZoneInfo(TARGET_TIMEZONE)

HLTV_HEADERS = {
    "User-Agent": os.getenv(
        "HLTV_USER_AGENT",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
    "Connection": "keep-alive",
    "Referer": HLTV_BASE_URL + "/",
    "Upgrade-Insecure-Requests": "1",
}


def init_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.info("Logging initialized (level=%s)", LOG_LEVEL)


def resolve_database_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")

    if not all([host, port, name, user, pwd]):
        raise RuntimeError(
            "DATABASE_URL is not set and DB_* variables are incomplete. "
            "Set DATABASE_URL or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD"
        )

    return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"


DATABASE_URL = resolve_database_url()


def db_connect():
    return psycopg.connect(DATABASE_URL)


# -----------------------------
# DATA MODELS
# -----------------------------

@dataclass
class EventsSchema:
    pk: str
    title: str
    slug: str
    url: str
    status: str


@dataclass
class EventRow:
    event_id: int
    title: str
    slug: str
    url: str
    status: str


@dataclass
class MatchItem:
    match_id: int
    url: str
    event_id: int
    event_title: str
    status: str                  # upcoming/live/finished
    when_utc: Optional[datetime] # instant
    when_msk: Optional[datetime] # instant, but in tz MSK (для удобства)
    match_time_raw: Optional[str]  # UTC timestamp string "YYYY-mm-dd HH:MM:SS.ffffff +00:00"
    match_time_msk: Optional[str]  # MSK timestamp string "YYYY-mm-dd HH:MM:SS.ffffff +03:00"
    team1: Optional[str]
    team2: Optional[str]
    bo: Optional[int]


# -----------------------------
# URL / REGEX
# -----------------------------

MATCH_LINK_RE = re.compile(r"^/matches/(\d+)(?:/|$)", re.IGNORECASE)
EVENT_LINK_RE = re.compile(r"^/events/(\d+)(?:/|$)", re.IGNORECASE)
TEAM_LINK_RE = re.compile(r"^/team/(\d+)/([^/?#]+)")

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


# -----------------------------
# HLTV FETCH
# -----------------------------

def _build_headers_for_url(url: str) -> dict:
    base = HLTV_HEADERS.copy()

    langs = [
        "en-US,en;q=0.9",
        "en-GB,en;q=0.9",
        "en-US,en;q=0.8,ru;q=0.6",
        "en-US,en;q=0.7",
    ]
    base["Accept-Language"] = random.choice(langs)

    parsed = urlparse(url)
    base["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"

    base.setdefault("DNT", "1")
    base.setdefault("Sec-Fetch-Dest", "document")
    base.setdefault("Sec-Fetch-Mode", "navigate")
    base.setdefault("Sec-Fetch-Site", "same-origin")
    base.setdefault("Sec-Fetch-User", "?1")

    return base


def fetch_html(url: str, session: requests.Session) -> Optional[str]:
    is_event_matches = "/events/" in url and url.endswith("/matches")

    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            time.sleep(HLTV_BASE_SLEEP_SECONDS + random.random() * 0.25)
            headers = _build_headers_for_url(url)
            r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            logging.info("HLTV GET %s -> %s", url, r.status_code)

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra and ra.isdigit():
                    sleep_s = min(HLTV_MAX_SLEEP_SECONDS, float(ra))
                else:
                    sleep_s = min(HLTV_MAX_SLEEP_SECONDS, (REQUEST_RETRY_SLEEP * (2 ** (attempt - 1))) + random.random())
                logging.error("HLTV 429 for %s (attempt %s/%s), sleep=%.2fs", url, attempt, REQUEST_RETRIES, sleep_s)
                time.sleep(sleep_s)
                continue

            if r.status_code == 403:
                logging.error("HLTV 403 for %s (attempt %s/%s)", url, attempt, REQUEST_RETRIES)
                if is_event_matches:
                    return None
                time.sleep(min(HLTV_MAX_SLEEP_SECONDS, REQUEST_RETRY_SLEEP * attempt + random.random()))
                continue

            r.raise_for_status()

            html = r.text or ""
            if len(html) < 200:
                logging.error("HLTV returned too short html for %s (len=%s)", url, len(html))
                time.sleep(min(HLTV_MAX_SLEEP_SECONDS, REQUEST_RETRY_SLEEP * attempt + random.random()))
                continue

            return html

        except requests.RequestException as e:
            sleep_s = min(HLTV_MAX_SLEEP_SECONDS, REQUEST_RETRY_SLEEP * attempt + random.random())
            logging.error("fetch_html failed url=%s err=%s (attempt %s/%s), sleep=%.2fs",
                          url, e, attempt, REQUEST_RETRIES, sleep_s)
            time.sleep(sleep_s)
        except Exception as e:
            sleep_s = min(HLTV_MAX_SLEEP_SECONDS, REQUEST_RETRY_SLEEP * attempt + random.random())
            logging.error("fetch_html unexpected error url=%s err=%s (attempt %s/%s), sleep=%.2fs",
                          url, e, attempt, REQUEST_RETRIES, sleep_s)
            time.sleep(sleep_s)

    return None


def abs_url(path: str) -> str:
    if path.startswith("http"):
        return path
    return HLTV_BASE_URL + path


# -----------------------------
# TIME HELPERS (MSK FIX)
# -----------------------------
def unix_ms_from_same_match_row(a: Tag) -> Optional[int]:
    """
    Ищем data-unix ТОЛЬКО внутри строки/карточки конкретного матча.
    Никаких подъёмов до body и find_all по всему дереву.
    """
    # 1) match-time внутри ссылки
    mt = a.select_one("div.match-time[data-unix]")
    if mt:
        v = mt.get("data-unix")
        if v and str(v).isdigit():
            return int(v)

    # 2) поднимаемся до ближайшего контейнера матча и ищем match-time там
    row = a
    for _ in range(8):
        row = row.parent
        if not row:
            return None

        # эвристика: “строка матча” обычно содержит match-time и две команды
        mt = row.select_one("div.match-time[data-unix]")
        if mt:
            v = mt.get("data-unix")
            if v and str(v).isdigit():
                return int(v)

        # если мы уже поднялись до какого-то "секционного" контейнера — дальше нельзя
        cls = " ".join(row.get("class", [])).lower()
        if "matches" in cls and "day" in cls:
            break

    return None


def _ensure_tz_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_msk(when_utc: Optional[datetime]) -> Optional[datetime]:
    if not when_utc:
        return None
    when_utc = _ensure_tz_utc(when_utc)
    return when_utc.astimezone(TZ_MSK)


def _parse_hhmm(s: Optional[str]) -> Optional[Tuple[int, int]]:
    if not s:
        return None
    m = re.search(r"\b(\d{1,2}):(\d{2})\b", s.strip())
    if not m:
        return None
    h = int(m.group(1))
    mi = int(m.group(2))
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        return None
    return h, mi


def _fmt_ts(dt: Optional[datetime]) -> Optional[str]:
    """
    Вернёт: "YYYY-mm-dd HH:MM:SS.ffffff +03:00" / "+00:00" (с двоеточием в offset).
    """
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # isoformat даёт offset с двоеточием и поддерживает microseconds.
    return dt.isoformat(sep=" ", timespec="microseconds")


# -----------------------------
# PARSING HELPERS
# -----------------------------

def _parse_unix_ms_from_attrs(tag: Tag) -> Optional[int]:
    for k in ("data-unix", "data-time", "data-zonedgrouping-entry-unix"):
        v = tag.get(k)
        if v and str(v).isdigit():
            try:
                return int(v)
            except Exception:
                pass
    return None


def unix_ms_from_match_anchor(a: Tag) -> Optional[int]:
    # 1) самый правильный: match-time внутри самой ссылки
    mt = a.select_one("div.match-time[data-unix]")
    if mt:
        v = mt.get("data-unix")
        if v and str(v).isdigit():
            return int(v)

    # 2) иногда match-time может быть рядом (внутри того же row), но не в a
    row = a.parent
    for _ in range(6):
        if not row:
            break
        mt = row.select_one("div.match-time[data-unix]")
        if mt:
            v = mt.get("data-unix")
            if v and str(v).isdigit():
                return int(v)
        row = row.parent

    return None


def find_unix_ms_near_anchor(a: Tag) -> Optional[int]:
    v = _parse_unix_ms_from_attrs(a)
    if v:
        return v
    if a.parent:
        v = _parse_unix_ms_from_attrs(a.parent)
        if v:
            return v

    node = a.parent
    for _ in range(10):
        if not node:
            break

        v = _parse_unix_ms_from_attrs(node)
        if v:
            return v

        for t in node.find_all(True):
            v = _parse_unix_ms_from_attrs(t)
            if v:
                return v

        node = node.parent

    return None


def find_unix_ms_for_match_anchor(a: Tag) -> Optional[int]:
    node = a
    for _ in range(10):
        if not node:
            break

        mt = node.find("div", class_="match-time", attrs={"data-unix": True})
        if mt:
            v = mt.get("data-unix")
            if v and str(v).isdigit():
                return int(v)

        for t in node.find_all(True, attrs={"data-unix": True}):
            v = t.get("data-unix")
            if v and str(v).isdigit():
                return int(v)

        node = node.parent

    return None


def fetch_when_utc_from_match_page(match_url: str, session: requests.Session) -> Optional[datetime]:
    """
    Последний шанс: если на /events/{id}/matches unix не нашли — попробуем вытащить unix с match page.
    """
    html = fetch_html(match_url, session)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Ищем любой ms unix
    for t in soup.find_all(True):
        v = t.get("data-unix") or t.get("data-time") or t.get("data-zonedgrouping-entry-unix")
        if v and str(v).isdigit():
            try:
                ms = int(v)
                if ms > 10**12:  # ms epoch
                    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            except Exception:
                pass

    return None


def parse_bo(text: str) -> Optional[int]:
    m = re.search(r"\bbo(\d)\b", (text or "").lower())
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _pretty_team_name_from_slug(s: str) -> str:
    s = s.replace("-", " ").strip()
    if any(ch.isalpha() for ch in s):
        return " ".join(w.capitalize() if w.islower() else w for w in s.split())
    return s


def parse_teams_from_context_text(txt: str) -> tuple[Optional[str], Optional[str]]:
    if not txt:
        return None, None
    t = re.sub(r"\s+", " ", txt).strip()

    m = re.search(r"\b(.+?)\s+vs\s+(.+?)\b", t, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"\b(.+?)\s*-\s*(.+?)\b", t)
    if not m:
        return None, None

    a = m.group(1).strip(" -|")
    b = m.group(2).strip(" -|")

    bad = {"grand final", "showmatch", "final", "semi-final", "quarter-final"}
    if a.lower() in bad or b.lower() in bad:
        return None, None

    a = a[:60].strip()
    b = b[:60].strip()
    return a or None, b or None


def looks_like_team_name(s: Optional[str]) -> bool:
    if not s:
        return False

    t = re.sub(r"\s+", " ", s).strip()
    if not t:
        return False

    low = t.lower()
    bad_exact = {
        "tbd", "to be decided",
        "semi", "grand", "final", "semi-final", "quarter-final",
        "showmatch", "winner", "loser", "decider",
        "group", "stage", "playoffs", "bracket",
    }
    if low in bad_exact:
        return False

    if re.match(r"^\d{1,2}:\d{2}\b", t):
        return False

    if re.fullmatch(r"\d{1,2}\s*[-:]\s*\d{1,2}", low):
        return False

    if len(t) > 40:
        return False

    tournamentish = [
        "major", "season", "series", "cup", "qualifier", "tournament", "event",
        "open", "closed", "regional", "international",
        "europe", "america", "asia", "oceania", "africa",
        "masters", "elite", "league", "division",
        "stage", "group", "playoffs",
        "starladder", "cct", "esea", "frag", "fissure", "blast", "iem", "esl",
    ]
    if any(w in low for w in tournamentish):
        return False

    if not re.search(r"[a-zA-Z0-9]", t):
        return False

    return True


def looks_like_bad_team_name(s: Optional[str]) -> bool:
    if not s:
        return True

    t = re.sub(r"\s+", " ", s).strip()
    if not t:
        return True

    low = t.lower()

    if re.match(r"^\d{1,2}:\d{2}\b", t):
        return True
    if re.search(r"\bbo[1-9]\b", low):
        return True

    tail_words = [
        "winner", "loser", "grand", "semi", "final", "quarter",
        "showmatch", "decider",
    ]
    if any(w in low.split() for w in tail_words):
        return True

    tournamentish = [
        "major", "season", "series", "cup", "qualifier", "tournament", "event",
        "league", "division", "stage", "group", "playoffs",
        "starladder", "cct", "esea", "frag", "fissure", "blast", "iem", "esl",
    ]
    if any(w in low for w in tournamentish):
        return True

    if len(t) > 32:
        return True

    return False


def parse_teams_from_match_href(href: str, event_slug: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    try:
        parts = href.strip("/").split("/")
        if len(parts) < 3:
            return None, None

        slug = parts[2]
        if "-vs-" not in slug:
            return None, None

        core = slug

        if event_slug:
            tail = "-" + event_slug.strip()
            pos = core.find(tail)
            if pos != -1:
                core = core[:pos]

        if "-vs-" in core:
            left, right = core.split("-vs-", 1)

            stop_words = [
                "-grand-final", "-final", "-semi-final", "-quarter-final",
                "-showmatch", "-playoffs", "-group", "-stage", "-qualifier",
                "-major", "-cup", "-series", "-season", "-tournament", "-event",
            ]
            for sw in stop_words:
                p = right.find(sw)
                if p != -1:
                    right = right[:p]
                    break

            t1 = _pretty_team_name_from_slug(left)
            t2 = _pretty_team_name_from_slug(right)
            return t1, t2

        return None, None

    except Exception:
        return None, None


def fetch_teams_from_match_page(match_url: str, session: requests.Session) -> tuple[
    Optional[tuple[str, str, str, int, str]],
    Optional[tuple[str, str, str, int, str]],
]:
    html = fetch_html(match_url, session)
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    teams = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = TEAM_LINK_RE.match(href)
        if not m:
            continue

        team_id = int(m.group(1))
        slug = m.group(2)
        name = a.get_text(" ", strip=True)
        if not name:
            continue

        path = href
        url = abs_url(href)
        teams.append((name, url, path, team_id, slug))

    uniq = {}
    for t in teams:
        uniq[t[3]] = t
        if len(uniq) >= 2:
            break

    vals = list(uniq.values())
    if len(vals) >= 2:
        return vals[0], vals[1]
    if len(vals) == 1:
        return vals[0], None
    return None, None


# -----------------------------
# GLOBAL MATCHES PARSING
# -----------------------------

def extract_event_slug_from_match_slug(match_slug: str) -> Optional[str]:
    if not match_slug or "-vs-" not in match_slug:
        return None
    parts = match_slug.split("-vs-", 1)
    if len(parts) != 2:
        return None
    right = parts[1]
    idx = right.find("-")
    if idx == -1:
        return None
    return right[idx + 1:] or None


def parse_global_matches_page(html: str) -> List[Tuple[int, int, str, Optional[datetime], Optional[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        mm = MATCH_LINK_RE.match(href)
        if not mm:
            continue

        match_id = int(mm.group(1))
        match_url = abs_url(href)

        parts = href.strip("/").split("/")
        match_slug = parts[2] if len(parts) >= 3 else ""
        wanted_event_slug = extract_event_slug_from_match_slug(match_slug)

        event_id = None

        container = a.parent
        for _ in range(8):
            if not container:
                break
            for x in container.find_all("a", href=True):
                ev = re.match(r"^/events/(\d+)/([^/?#]+)", x.get("href", ""))
                if not ev:
                    continue
                ev_id = int(ev.group(1))
                ev_slug = ev.group(2)

                if wanted_event_slug and ev_slug == wanted_event_slug:
                    event_id = ev_id
                    break

            if event_id:
                break
            container = container.parent

        if not event_id:
            continue

        when_utc = None
        match_time_raw = None

        txt = (a.parent.get_text(" ", strip=True) if a.parent else a.get_text(" ", strip=True)) or ""
        tm = re.search(r"\b(\d{1,2}:\d{2})\b", txt)
        if tm:
            match_time_raw = tm.group(1)

        out.append((match_id, event_id, match_url, when_utc, match_time_raw))

    uniq = {r[0]: r for r in out}
    return list(uniq.values())


# -----------------------------
# DB SCHEMA / DETECT
# -----------------------------

def find_table_schema(conn, table_name: str, preferred_schema: str | None = None) -> Optional[str]:
    with conn.cursor() as cur:
        if preferred_schema:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema=%s AND table_name=%s
                )
            """, (preferred_schema, table_name))
            if cur.fetchone()[0]:
                return preferred_schema

        cur.execute("""
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_name=%s
            ORDER BY CASE WHEN table_schema='public' THEN 0 ELSE 1 END, table_schema
            LIMIT 1
        """, (table_name,))
        row = cur.fetchone()
        return row[0] if row else None


def get_table_columns_any_schema(conn, table_name: str, schema: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
        """, (schema, table_name))
        return {r[0] for r in cur.fetchall()}


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS public.{EVENTS_TABLE} (
                event_id    BIGINT PRIMARY KEY,
                name        TEXT NOT NULL,
                slug        TEXT NOT NULL,
                url         TEXT NOT NULL,
                status      TEXT NOT NULL,     -- upcoming | ongoing | finished
                date_range  TEXT NULL,
                last_seen_at TIMESTAMPTZ NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS public.{MATCHES_TABLE} (
                match_id    BIGINT PRIMARY KEY,
                event_id    BIGINT NOT NULL,
                event_title TEXT,
                url         TEXT NOT NULL,
                status      TEXT NOT NULL,     -- upcoming | live | finished
                when_utc    TIMESTAMPTZ NULL,
                when_msk    TIMESTAMPTZ NULL,
                match_time_raw TEXT NULL,
                match_time_msk TEXT NULL,
                team1       TEXT NULL,
                team2       TEXT NULL,
                bo          INT NULL,
                last_seen_at TIMESTAMPTZ NULL,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        cur.execute(f"CREATE INDEX IF NOT EXISTS {EVENTS_TABLE}_status_idx ON public.{EVENTS_TABLE}(status);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_when_idx ON public.{MATCHES_TABLE}(when_utc);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_status_idx ON public.{MATCHES_TABLE}(status);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {MATCHES_TABLE}_event_idx ON public.{MATCHES_TABLE}(event_id);")

        # мягкие миграции
        cur.execute(f"ALTER TABLE public.{EVENTS_TABLE} ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NULL;")
        cur.execute(f"ALTER TABLE public.{EVENTS_TABLE} ADD COLUMN IF NOT EXISTS date_range TEXT NULL;")

        cur.execute(f"ALTER TABLE public.{MATCHES_TABLE} ADD COLUMN IF NOT EXISTS match_time_raw TEXT NULL;")
        cur.execute(f"ALTER TABLE public.{MATCHES_TABLE} ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NULL;")

        # MSK
        cur.execute(f"ALTER TABLE public.{MATCHES_TABLE} ADD COLUMN IF NOT EXISTS when_msk TIMESTAMPTZ NULL;")
        cur.execute(f"ALTER TABLE public.{MATCHES_TABLE} ADD COLUMN IF NOT EXISTS match_time_msk TEXT NULL;")

    conn.commit()
    logging.info("DB schema ensured: %s + %s exist/updated in public", EVENTS_TABLE, MATCHES_TABLE)


def detect_events_schema(conn) -> EventsSchema:
    schema_name = find_table_schema(conn, EVENTS_TABLE, EVENTS_SCHEMA or None)
    if not schema_name:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            db, usr = cur.fetchone()
        raise RuntimeError(
            f"Table '{EVENTS_TABLE}' not found in any schema. "
            f"current_database={db}, current_user={usr}. "
            f"Set CS2_EVENTS_TABLE and/or CS2_EVENTS_SCHEMA in .env"
        )

    cols = get_table_columns_any_schema(conn, EVENTS_TABLE, schema_name)
    if not cols:
        raise RuntimeError(f"Table '{schema_name}.{EVENTS_TABLE}' found but has no visible columns (permissions?)")

    pk = "event_id" if "event_id" in cols else ("id" if "id" in cols else None)
    if not pk:
        raise RuntimeError(f"Can't detect PK column for {schema_name}.{EVENTS_TABLE}. Columns={sorted(cols)}")

    title = "name" if "name" in cols else ("title" if "title" in cols else None)
    if not title:
        raise RuntimeError(f"Can't detect title column for {schema_name}.{EVENTS_TABLE}. Columns={sorted(cols)}")

    slug = "slug" if "slug" in cols else None
    url = "url" if "url" in cols else None
    status = "status" if "status" in cols else None
    if not (slug and url and status):
        raise RuntimeError(f"Missing slug/url/status in {schema_name}.{EVENTS_TABLE}. Columns={sorted(cols)}")

    logging.info(
        "Detected events schema: table=%s.%s pk=%s title=%s slug=%s url=%s status=%s",
        schema_name, EVENTS_TABLE, pk, title, slug, url, status
    )

    schema_obj = EventsSchema(pk=pk, title=title, slug=slug, url=url, status=status)
    schema_obj._table_schema = schema_name  # type: ignore[attr-defined]
    return schema_obj


def load_events_candidates(conn) -> List[EventRow]:
    schema = detect_events_schema(conn)
    table_schema = getattr(schema, "_table_schema", "public")

    sql = f"""
        SELECT {schema.pk}, {schema.title}, {schema.slug}, {schema.url}, {schema.status}
        FROM {table_schema}.{EVENTS_TABLE}
        WHERE {schema.status} IN ('ongoing','upcoming')
        ORDER BY {schema.pk} DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    out: List[EventRow] = []
    for r in rows:
        out.append(EventRow(
            event_id=int(r[0]),
            title=str(r[1]),
            slug=str(r[2]),
            url=str(r[3]),
            status=str(r[4]),
        ))
    return out


def parse_hltv_date_range(text: str, default_year: int) -> Optional[Tuple[date, date]]:
    if not text:
        return None
    t = text.strip().replace("–", "-").replace("—", "-")
    t = re.sub(r"\s+", " ", t)

    t = re.sub(
        r"([A-Za-z]{3}\s*\d{1,2}(?:st|nd|rd|th)?)\s*-\s*([A-Za-z]{3}\s*\d{1,2}(?:st|nd|rd|th)?)",
        r"\1 - \2",
        t
    )

    parts = re.split(r"\s*-\s*", t)
    if len(parts) != 2:
        return None

    def parse_one(s: str) -> Optional[Tuple[int, int]]:
        s = s.strip()
        s = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", s, flags=re.IGNORECASE)
        m = re.search(r"\b([A-Za-z]{3})\b", s)
        d = re.search(r"\b(\d{1,2})\b", s)
        if not (m and d):
            return None
        mon = MONTHS.get(m.group(1).lower())
        if not mon:
            return None
        return mon, int(d.group(1))

    a = parse_one(parts[0])
    b = parse_one(parts[1])
    if not a or not b:
        return None

    mon1, day1 = a
    mon2, day2 = b

    y1 = default_year
    y2 = default_year
    if mon2 < mon1:
        y2 += 1

    try:
        return date(y1, mon1, day1), date(y2, mon2, day2)
    except Exception:
        return None


def intersects_window(dr: Optional[Tuple[date, date]], win_start: date, win_end: date) -> bool:
    if not dr:
        return False
    a, b = dr
    return not (b < win_start or a > win_end)


def extract_event_date_range_from_event_page(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(
        r"\b([A-Za-z]{3}\s*\d{1,2}(?:st|nd|rd|th)?)\s*-\s*([A-Za-z]{3}\s*\d{1,2}(?:st|nd|rd|th)?)\b",
        text
    )
    if m:
        return f"{m.group(1)} - {m.group(2)}"
    return None


def choose_events_for_window(
    events: List[EventRow],
    session: requests.Session,
    win_start: date,
    win_end: date,
) -> List[EventRow]:
    selected: List[EventRow] = []
    max_year = win_end.year

    def extract_year_from_text(s: str) -> Optional[int]:
        m = re.search(r"(19|20)\d{2}", s)
        return int(m.group(0)) if m else None

    pages_done = 0

    for ev in events:
        if ev.status != "ongoing":
            continue
        y = extract_year_from_text(ev.url) or extract_year_from_text(ev.slug)
        if y and y > max_year:
            continue
        selected.append(ev)

    for ev in events:
        if ev.status != "upcoming":
            continue

        y = extract_year_from_text(ev.url) or extract_year_from_text(ev.slug)
        if y and y > max_year:
            continue

        if pages_done >= HLTV_MAX_EVENT_PAGES_PER_RUN:
            break

        html = fetch_html(ev.url, session)
        pages_done += 1
        time.sleep(HLTV_EVENT_PAGE_DELAY_SECONDS)

        if not html:
            continue

        dr_text = extract_event_date_range_from_event_page(html)
        if not dr_text:
            continue

        dr = parse_hltv_date_range(dr_text, default_year=win_start.year)
        if intersects_window(dr, win_start, win_end):
            selected.append(ev)

    uniq: Dict[int, EventRow] = {}
    for ev in selected:
        uniq[ev.event_id] = ev

    out = list(uniq.values())
    out.sort(key=lambda e: (e.status != "ongoing", e.event_id), reverse=False)
    return out


# -----------------------------
# MATCHES TABLE COMPAT
# -----------------------------

def get_table_columns(conn, table_name: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
        """, (table_name,))
        return {r[0] for r in cur.fetchall()}


def ensure_matches_table(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name=%s
            )
        """, (MATCHES_TABLE,))
        exists = bool(cur.fetchone()[0])

    target = MATCHES_TABLE

    if exists:
        cols = get_table_columns(conn, MATCHES_TABLE)
        if "match_id" not in cols:
            target = MATCHES_TABLE + "_v2"
            logging.warning("Table %s exists but has no match_id. Using %s instead.", MATCHES_TABLE, target)

    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target} (
                match_id BIGINT PRIMARY KEY,
                event_id BIGINT NOT NULL,
                event_title TEXT,
                url TEXT NOT NULL,
                status TEXT NOT NULL,
                when_utc TIMESTAMPTZ NULL,
                when_msk TIMESTAMPTZ NULL,
                match_time_raw TEXT NULL,
                match_time_msk TEXT NULL,
                team1 TEXT NULL,
                team2 TEXT NULL,
                bo INT NULL,
                last_seen_at TIMESTAMPTZ NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        cur.execute(f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS match_time_raw TEXT NULL;")
        cur.execute(f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NULL;")

        # MSK
        cur.execute(f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS when_msk TIMESTAMPTZ NULL;")
        cur.execute(f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS match_time_msk TEXT NULL;")

        conn.commit()

    return target


# -----------------------------
# TEAMS TABLE
# -----------------------------

def ensure_cs2_teams_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS public.{TEAMS_TABLE} (
                id SERIAL PRIMARY KEY,
                hltv_team_id BIGINT,
                hltv_slug TEXT,
                hltv_path TEXT,
                hltv_url TEXT NOT NULL,
                name TEXT NOT NULL,
                country TEXT,
                region TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS {TEAMS_TABLE}_lower_name_idx
            ON public.{TEAMS_TABLE} (lower(name));
        """)

        cur.execute(f"""
            UPDATE public.{TEAMS_TABLE}
            SET hltv_path = COALESCE(
                hltv_path,
                CASE
                    WHEN hltv_team_id IS NOT NULL AND hltv_slug IS NOT NULL
                        THEN '/team/' || hltv_team_id::text || '/' || hltv_slug
                    ELSE NULL
                END
            )
            WHERE hltv_path IS NULL;
        """)

        cur.execute(f"DELETE FROM public.{TEAMS_TABLE} WHERE hltv_path IS NULL;")

        cur.execute(f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema='public'
                      AND table_name='{TEAMS_TABLE}'
                      AND column_name='hltv_path'
                      AND is_nullable='YES'
                ) THEN
                    ALTER TABLE public.{TEAMS_TABLE}
                    ALTER COLUMN hltv_path SET NOT NULL;
                END IF;
            END $$;
        """)

        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = '{TEAMS_TABLE}_hltv_path_uq'
                ) THEN
                    ALTER TABLE public.{TEAMS_TABLE}
                    ADD CONSTRAINT {TEAMS_TABLE}_hltv_path_uq UNIQUE (hltv_path);
                END IF;
            END $$;
        """)

    conn.commit()


def upsert_cs2_team(
    conn,
    *,
    name: str,
    hltv_url: str,
    hltv_path: Optional[str],
    hltv_team_id: Optional[int],
    hltv_slug: Optional[str],
    country: Optional[str] = None,
    region: Optional[str] = None
) -> None:
    if not hltv_path and hltv_team_id:
        hltv_path = f"/team/{hltv_team_id}/{hltv_slug or ''}".rstrip("/")

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO public.{TEAMS_TABLE} (hltv_team_id, hltv_slug, hltv_path, hltv_url, name, country, region, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
            ON CONFLICT (hltv_path)
            DO UPDATE SET
                hltv_team_id=COALESCE(EXCLUDED.hltv_team_id, public.{TEAMS_TABLE}.hltv_team_id),
                hltv_slug=COALESCE(EXCLUDED.hltv_slug, public.{TEAMS_TABLE}.hltv_slug),
                hltv_url=EXCLUDED.hltv_url,
                name=EXCLUDED.name,
                country=COALESCE(EXCLUDED.country, public.{TEAMS_TABLE}.country),
                region=COALESCE(EXCLUDED.region, public.{TEAMS_TABLE}.region),
                updated_at=NOW()
        """, (hltv_team_id, hltv_slug, hltv_path, hltv_url, name, country, region))
    conn.commit()


# -----------------------------
# PARSERS
# -----------------------------

def parse_matches_from_event_matches(html: str, event: EventRow, session: requests.Session, conn) -> List[MatchItem]:
    """
    /events/{id}/matches

    TIME RULES:
      - unix ms -> when_utc (точно), then when_msk
      - если unix нет -> берем HH:MM и трактуем как MSK time на section_date, строим when_utc
      - match_time_raw/match_time_msk в БД всегда ПОЛНЫЕ TIMESTAMP-строки (не "13:00")
    """
    soup = BeautifulSoup(html, "html.parser")

    section_date: Optional[date] = None
    items: Dict[int, MatchItem] = {}

    for tag in soup.find_all(True):
        txt = tag.get_text(" ", strip=True)

        m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", txt)
        if m:
            try:
                section_date = datetime.strptime(m.group(1), "%Y-%m-%d").date()
            except Exception:
                pass

        if tag.name != "a":
            continue

        href = tag.get("href")
        if not href:
            continue

        mm = MATCH_LINK_RE.match(href)
        if not mm:
            continue

        match_id = int(mm.group(1))
        if match_id in items:
            continue

        ctx_text = tag.get_text(" ", strip=True)
        if tag.parent:
            ptxt = tag.parent.get_text(" ", strip=True)
            if 0 < len(ptxt) <= 250:
                ctx_text = ptxt

        match_url = abs_url(href)

        # --- TIME PART ---
        # Это "видимое" время (HH:MM). Используем ТОЛЬКО чтобы построить when_utc, если unix нет.
        match_time_hhmm: Optional[str] = None

        # try get visible match-time text
        mt = tag.select_one("div.match-time")
        if not mt:
            node = tag.parent
            for _ in range(6):
                if not node:
                    break
                mt = node.select_one("div.match-time")
                if mt:
                    break
                node = node.parent
        if mt:
            match_time_hhmm = (mt.get_text(" ", strip=True) or None)

        if not match_time_hhmm:
            time_m = re.search(r"\b(\d{1,2}:\d{2})\b", ctx_text)
            if time_m:
                match_time_hhmm = time_m.group(1)


        # unix ms (TRUST) — только из строки матча
        unix_ms = unix_ms_from_same_match_row(tag)

        when_utc: Optional[datetime] = None
        if unix_ms:
            try:
                when_utc = datetime.fromtimestamp(unix_ms / 1000.0, tz=timezone.utc)
            except Exception:
                when_utc = None

        # last chance: match page
        if when_utc is None:
            when_utc = fetch_when_utc_from_match_page(match_url, session)

        when_utc: Optional[datetime] = None

        if unix_ms:
            try:
                when_utc = datetime.fromtimestamp(unix_ms / 1000.0, tz=timezone.utc)
            except Exception:
                when_utc = None

        # last chance: match page
        if when_utc is None:
            when_utc = fetch_when_utc_from_match_page(match_url, session)

        # if still none -> interpret HH:MM as MSK time for section_date (requested behavior)
        if when_utc is None and section_date and match_time_hhmm:
            hm = _parse_hhmm(match_time_hhmm)
            if hm:
                h, mi = hm
                try:
                    local_msk = datetime(
                        section_date.year, section_date.month, section_date.day,
                        h, mi, tzinfo=TZ_MSK
                    )
                    when_utc = local_msk.astimezone(timezone.utc)
                except Exception:
                    pass

        when_msk: Optional[datetime] = _to_msk(when_utc) if when_utc else None

        # IMPORTANT: сохраняем В БД не "13:00", а полный timestamp
        match_time_raw_ts: Optional[str] = _fmt_ts(_ensure_tz_utc(when_utc)) if when_utc else None
        match_time_msk_ts: Optional[str] = _fmt_ts(when_msk) if when_msk else None

        low = ctx_text.lower()
        status = "upcoming"
        if "live" in low:
            status = "live"
        if re.search(r"\b\d{1,2}\s*[-:]\s*\d{1,2}\b", low):
            status = "finished"

        team1, team2 = parse_teams_from_match_href(href, event.slug)

        if team1 and not looks_like_team_name(team1):
            team1 = None
        if team2 and not looks_like_team_name(team2):
            team2 = None

        if not team1 or not team2:
            t1b, t2b = parse_teams_from_context_text(ctx_text)
            team1 = team1 or t1b
            team2 = team2 or t2b

        if team1 and not looks_like_team_name(team1):
            team1 = None
        if team2 and not looks_like_team_name(team2):
            team2 = None

        need_match_page = (
            (team1 is None or team2 is None) or
            looks_like_bad_team_name(team1) or
            looks_like_bad_team_name(team2)
        )

        if need_match_page:
            t1p, t2p = fetch_teams_from_match_page(match_url, session)

            if t1p and looks_like_team_name(t1p[0]):
                team1 = t1p[0]
                upsert_cs2_team(
                    conn,
                    name=t1p[0],
                    hltv_url=t1p[1],
                    hltv_path=t1p[2],
                    hltv_team_id=t1p[3],
                    hltv_slug=t1p[4],
                )

            if t2p and looks_like_team_name(t2p[0]):
                team2 = t2p[0]
                upsert_cs2_team(
                    conn,
                    name=t2p[0],
                    hltv_url=t2p[1],
                    hltv_path=t2p[2],
                    hltv_team_id=t2p[3],
                    hltv_slug=t2p[4],
                )

        if team1 and looks_like_bad_team_name(team1):
            team1 = None
        if team2 and looks_like_bad_team_name(team2):
            team2 = None

        items[match_id] = MatchItem(
            match_id=match_id,
            url=match_url,
            event_id=event.event_id,
            event_title=event.title,
            status=status,
            when_utc=when_utc,
            when_msk=when_msk,
            match_time_raw=match_time_raw_ts,
            match_time_msk=match_time_msk_ts,
            team1=team1,
            team2=team2,
            bo=parse_bo(ctx_text),
        )

    return list(items.values())


def parse_matches_from_event_results(html: str, event: EventRow) -> List[MatchItem]:
    soup = BeautifulSoup(html, "html.parser")
    items: Dict[int, MatchItem] = {}

    for a in soup.find_all("a", href=True):
        href = a["href"]
        mm = MATCH_LINK_RE.match(href)
        if not mm:
            continue
        match_id = int(mm.group(1))
        if match_id in items:
            continue

        items[match_id] = MatchItem(
            match_id=match_id,
            url=abs_url(href),
            event_id=event.event_id,
            event_title=event.title,
            status="finished",
            when_utc=None,
            when_msk=None,
            match_time_raw=None,
            match_time_msk=None,
            team1=None,
            team2=None,
            bo=None,
        )

    return list(items.values())


# -----------------------------
# UPSERT MATCHES
# -----------------------------

def upsert_matches(conn, table_name: str, matches: List[MatchItem]) -> int:
    if not matches:
        return 0

    now_ts = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        for m in matches:
            cur.execute(f"""
                INSERT INTO {table_name} (
                    match_id, event_id, event_title, url, status,
                    when_utc, when_msk,
                    match_time_raw, match_time_msk,
                    team1, team2, bo,
                    last_seen_at, updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (match_id) DO UPDATE SET
                    event_id=EXCLUDED.event_id,
                    event_title=EXCLUDED.event_title,
                    url=EXCLUDED.url,
                    status=EXCLUDED.status,

                    -- если смогли вычислить время — обновляем
                    when_utc=COALESCE(EXCLUDED.when_utc, {table_name}.when_utc),
                    when_msk=COALESCE(EXCLUDED.when_msk, {table_name}.when_msk),

                    match_time_raw=COALESCE(EXCLUDED.match_time_raw, {table_name}.match_time_raw),
                    match_time_msk=COALESCE(EXCLUDED.match_time_msk, {table_name}.match_time_msk),

                    team1=COALESCE(EXCLUDED.team1, {table_name}.team1),
                    team2=COALESCE(EXCLUDED.team2, {table_name}.team2),
                    bo=COALESCE(EXCLUDED.bo, {table_name}.bo),
                    last_seen_at=EXCLUDED.last_seen_at,
                    updated_at=NOW()
            """, (
                m.match_id, m.event_id, m.event_title, m.url, m.status,
                m.when_utc, m.when_msk,
                m.match_time_raw, m.match_time_msk,
                m.team1, m.team2, m.bo,
                now_ts
            ))
        conn.commit()
    return len(matches)


# -----------------------------
# SYNC EVENTS FROM /events
# -----------------------------

def clean_event_display_name(raw: Optional[str], *, max_len: int = 60) -> Optional[str]:
    if not raw:
        return None

    s = " ".join(raw.split())

    if " | " in s:
        s = s.split(" | ", 1)[0].strip()

    low = s.lower()

    tag = None
    if re.search(r"\blan\b", low):
        tag = "LAN"
    elif re.search(r"\bonline\b", low):
        tag = "Online"

    s = re.sub(r"\bLAN\b.*$", "LAN", s, flags=re.IGNORECASE)
    s = re.sub(r"\bOnline\b.*$", "Online", s, flags=re.IGNORECASE)

    s = re.sub(r"\$\s*[\d,]+", "", s)

    s = re.sub(r"\bTeams\b.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\bPrize\b.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\bGroup\b.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\bStage\b.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\bPlayoffs\b.*$", "", s, flags=re.IGNORECASE).strip()

    s = re.sub(r"\bSeason\s+(\d+)\b", r"S\1", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+\b\d{1,3}\b\s*$", "", s).strip()

    s = re.sub(r"\b(Local|Regional|Reg\.|Intl\.|International|Qualifier|Qualifiers)\b\.?", "", s, flags=re.IGNORECASE)
    s = " ".join(s.split()).strip(" -|")

    if tag and tag.lower() not in s.lower():
        s = f"{s} ({tag})"
    else:
        s = re.sub(r"\b(LAN|Online)\b$", r"(\1)", s, flags=re.IGNORECASE)

    s = " ".join(s.split()).strip()

    if max_len and len(s) > max_len:
        cut = s[:max_len].rstrip()
        if " " in cut:
            cut = cut.rsplit(" ", 1)[0]
        s = cut + "…"

    return s or None


def sync_events_from_hltv(conn, session: requests.Session, limit: int = 200) -> int:
    html = fetch_html(HLTV_EVENTS_LIST_URL, session)
    if not html:
        logging.error("Failed to fetch HLTV events list: %s", HLTV_EVENTS_LIST_URL)
        return 0

    soup = BeautifulSoup(html, "html.parser")

    found: Dict[int, Tuple[str, str, str]] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.match(r"^/events/(\d+)/([^/?#]+)", href)
        if not m:
            continue

        event_id = int(m.group(1))
        slug = m.group(2)
        url = abs_url(href)

        raw_title = a.get_text(" ", strip=True) or ""
        title = clean_event_display_name(raw_title) or raw_title

        title = re.sub(r"\s+", " ", title).strip()
        if len(title) < 3:
            title = slug.replace("-", " ")

        found[event_id] = (title[:300], slug[:200], url[:500])

        if len(found) >= limit:
            break

    if not found:
        logging.warning("HLTV events list parsed: found 0 events. Markup might have changed or blocked.")
        return 0

    saved = 0
    now_ts = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        for event_id, (name, slug, url) in found.items():
            cur.execute(f"""
                INSERT INTO public.{EVENTS_TABLE} (event_id, name, slug, url, status, last_seen_at, updated_at)
                VALUES (%s, %s, %s, %s, 'upcoming', %s, NOW())
                ON CONFLICT (event_id) DO UPDATE SET
                    name=EXCLUDED.name,
                    slug=EXCLUDED.slug,
                    url=EXCLUDED.url,
                    last_seen_at=EXCLUDED.last_seen_at,
                    updated_at=NOW()
            """, (event_id, name, slug, url, now_ts))
            saved += 1
    conn.commit()

    logging.info("Synced events into %s: %s", EVENTS_TABLE, saved)
    return saved


# -----------------------------
# WORKER
# -----------------------------

def cs2_worker_once() -> None:
    t0 = time.time()
    win_start = datetime.now().date()
    win_end = win_start + timedelta(days=1)

    with db_connect() as conn:
        ensure_tables(conn)
        ensure_cs2_teams_table(conn)

        session = requests.Session()

        try:
            candidates = load_events_candidates(conn)
        except Exception as e:
            logging.error("Failed to load events candidates from DB: %s", e)
            return

        if not candidates:
            logging.warning("No events in %s. Trying to sync from HLTV /events...", EVENTS_TABLE)
            synced = sync_events_from_hltv(conn, session, limit=HLTV_EVENTS_SYNC_LIMIT)
            if synced <= 0:
                logging.warning("Events table still empty after sync. HLTV might be blocked or markup changed.")
                return
            candidates = load_events_candidates(conn)

        events = choose_events_for_window(candidates, session, win_start, win_end)
        logging.info("Active events to parse (window %s..%s): %s", win_start, win_end, [e.event_id for e in events])

        if not events:
            logging.warning("No events intersect window %s..%s", win_start, win_end)
            return

        target_matches_table = ensure_matches_table(conn)
        logging.info("Matches table target=%s", target_matches_table)

        total_saved = 0
        had_event_matches_unavailable = 0

        for ev in events:
            matches_url = f"{HLTV_BASE_URL}/events/{ev.event_id}/matches"
            results_url = f"{HLTV_BASE_URL}/events/{ev.event_id}/results"

            html = fetch_html(matches_url, session)
            if html:
                parsed = parse_matches_from_event_matches(html, ev, session, conn)
                saved = upsert_matches(conn, target_matches_table, parsed)
                total_saved += saved
                logging.info("event=%s parsed_from=event_matches total=%s saved=%s", ev.event_id, len(parsed), saved)
                continue

            had_event_matches_unavailable += 1

            html = fetch_html(results_url, session)
            if not html:
                logging.error("event=%s failed both matches and results", ev.event_id)
                continue

            parsed = parse_matches_from_event_results(html, ev)
            saved = upsert_matches(conn, target_matches_table, parsed)
            total_saved += saved
            logging.warning("event=%s parsed_from=results_only => finished_only total=%s saved=%s", ev.event_id, len(parsed), saved)

        # global fallback: он времени "инстантом" не даёт — не кладём HH:MM в match_time_raw/match_time_msk,
        # чтобы не портить CSV "13:00".
        if HLTV_GLOBAL_MATCHES_FALLBACK and had_event_matches_unavailable > 0:
            try:
                global_matches_url = f"{HLTV_BASE_URL}/matches"
                html = fetch_html(global_matches_url, session)
                if html:
                    rows = parse_global_matches_page(html)
                    want_events = {e.event_id: e for e in events}

                    extra: List[MatchItem] = []
                    for match_id, event_id, match_url, when_utc, match_time_raw in rows:
                        ev = want_events.get(event_id)
                        if not ev:
                            continue

                        extra.append(MatchItem(
                            match_id=match_id,
                            url=match_url,
                            event_id=event_id,
                            event_title=ev.title,
                            status="upcoming",
                            when_utc=None,
                            when_msk=None,
                            match_time_raw=None,
                            match_time_msk=None,
                            team1=None,
                            team2=None,
                            bo=None,
                        ))

                    if extra:
                        saved = upsert_matches(conn, target_matches_table, extra)
                        total_saved += saved
                        logging.warning("Global /matches fallback used: extra_saved=%s (matched_events=%s)", saved, len(events))
            except Exception as e:
                logging.error("Global /matches fallback failed: %s", e)

        elapsed = time.time() - t0
        logging.info("Worker finished: saved/updated total=%s, elapsed=%.2fs", total_saved, elapsed)

        if had_event_matches_unavailable > 0:
            logging.warning(
                "Note: /events/{id}/matches was unavailable for %s events (often HLTV 403). "
                "In that case you mostly get FINISHED from /results. "
                "If you need reliable upcoming/live, you must make /events/{id}/matches accessible "
                "(cookies/proxy/headless).",
                had_event_matches_unavailable
            )


def main() -> None:
    init_logging()
    logging.info("DATABASE_URL resolved: %s", "SET" if DATABASE_URL else "NOT SET")
    logging.info("HLTV_BASE_URL=%s", HLTV_BASE_URL)
    logging.info("Events table=%s, Matches table=%s, Teams table=%s", EVENTS_TABLE, MATCHES_TABLE, TEAMS_TABLE)
    logging.info("HLTV_GLOBAL_MATCHES_FALLBACK=%s", HLTV_GLOBAL_MATCHES_FALLBACK)
    logging.info("TARGET_TIMEZONE=%s", TARGET_TIMEZONE)
    cs2_worker_once()


if __name__ == "__main__":
    main()
