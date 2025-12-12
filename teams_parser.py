#!/usr/bin/env python3
"""
teams_parser.py — минимальный парсер команд Liquipedia Dota2 (ТОЛЬКО команды).

Источник:
- https://liquipedia.net/dota2/Portal:Teams

Парсим ТОЛЬКО:
- name
- liquipedia_url
(+ вычисляем liquipedia_slug из URL)

Режим записи в БД:
- INSERT ONLY: добавляем только те команды, которых ещё нет (по liquipedia_slug)
- если команда уже есть — ничего не меняем

DB: async psycopg3 + AsyncConnectionPool
Логи:
- logs/team_parser.log (и в stdout)
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv

import requests
from bs4 import BeautifulSoup
from psycopg_pool import AsyncConnectionPool


# --------------------------
# ENV
# --------------------------
load_dotenv()


# --------------------------
# LOGGING
# --------------------------
def setup_logging() -> logging.Logger:
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    logs_dir = Path(__file__).resolve().parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "team_parser.log"

    logger = logging.getLogger("teams_parser")
    logger.setLevel(log_level)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(log_level)
    fh.setFormatter(formatter)

    sh = logging.StreamHandler()
    sh.setLevel(log_level)
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)

    logger.info("Логирование включено: %s (level=%s)", str(log_file), log_level_name)
    return logger


logger = setup_logging()


# --------------------------
# CONSTANTS
# --------------------------
LIQUIPEDIA_BASE = "https://liquipedia.net"
PORTAL_TEAMS_URL = f"{LIQUIPEDIA_BASE}/dota2/Portal:Teams"

HEADERS = {
    "User-Agent": os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DB_CONNECT_TIMEOUT_SEC = int(os.getenv("DB_CONNECT_TIMEOUT_SEC", "10"))
DB_LOCK_TIMEOUT_MS = int(os.getenv("DB_LOCK_TIMEOUT_MS", "5000"))
DB_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "30000"))

INSERT_CHUNK_SIZE = int(os.getenv("INSERT_CHUNK_SIZE", "200"))

CONNINFO = (
    f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} "
    f"connect_timeout={DB_CONNECT_TIMEOUT_SEC} application_name=teams_parser"
)


@dataclass(frozen=True)
class TeamRow:
    liquipedia_slug: str
    liquipedia_url: str
    name: str


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def fetch_html(url: str) -> str:
    t0 = time.monotonic()
    logger.info("HTTP GET: %s", url)
    resp = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
    ms = int((time.monotonic() - t0) * 1000)
    logger.info("HTTP %s (%d ms), bytes=%s", resp.status_code, ms, resp.headers.get("Content-Length", "unknown"))
    resp.raise_for_status()
    return resp.text


def slug_from_liquipedia_url(full_url: str) -> str:
    path = urlparse(full_url).path
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2:
        return parts[-1]
    return path.lstrip("/")


from urllib.parse import urlparse

def canonical_liquipedia_path(value: str) -> str:
    """
    Приводит к виду: /dota2/Team_Liquid
    Принимает:
      - 'https://liquipedia.net/dota2/Team_Liquid?x=1' -> '/dota2/Team_Liquid'
      - '/dota2/Team_Liquid' -> '/dota2/Team_Liquid'
      - 'Team_Liquid' -> 'Team_Liquid' (на всякий)
    """
    if not value:
        return ""

    v = value.strip()

    if v.startswith("http://") or v.startswith("https://"):
        v = urlparse(v).path

    # убрать query/fragment если вдруг пришло не URL, а строка с ?
    v = v.split("?", 1)[0].split("#", 1)[0]
    v = v.rstrip("/")

    return v


def parse_teams_from_portal(html: str) -> list[TeamRow]:
    """
    Берём команды через team-template, как в старой версии:
    span.team-template-team-standard -> span.team-template-text a
    """
    t0 = time.monotonic()
    soup = BeautifulSoup(html, "lxml")

    spans = soup.select("span.team-template-team-standard")
    found_links = 0
    redlinks = 0
    empty = 0

    teams_by_slug: dict[str, TeamRow] = {}

    for span in spans:
        a = span.select_one("span.team-template-text a[href]")
        if not a:
            continue

        href = normalize_text(a.get("href", ""))
        name = normalize_text(a.get_text())

        if not href or not name:
            empty += 1
            continue

        found_links += 1

        if "redlink=1" in href:
            redlinks += 1
            continue

        href = href.split("#", 1)[0]
        full_url = urljoin(LIQUIPEDIA_BASE, href)
        slug = slug_from_liquipedia_url(full_url)

        if ":" in slug:
            continue

        teams_by_slug[slug] = TeamRow(liquipedia_slug=slug, liquipedia_url=full_url, name=name)

    result = list(teams_by_slug.values())
    ms = int((time.monotonic() - t0) * 1000)

    examples = ", ".join([f"{t.name}({t.liquipedia_slug})" for t in result[:8]])
    logger.info(
        "Portal parse: spans=%d, links=%d, redlinks=%d, empty=%d, deduped=%d, time=%d ms",
        len(spans),
        found_links,
        redlinks,
        empty,
        len(result),
        ms,
    )
    logger.info("Examples: %s%s", examples, "" if len(result) <= 8 else ", ...")
    return result


async def ensure_schema(pool: AsyncConnectionPool) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS public.dota_teams
    (
        id              serial primary key,
        liquipedia_slug text not null unique,
        liquipedia_url  text not null,
        name            text not null,
        short_name      text,
        country         text,
        region          text,
        created_at      timestamp with time zone default now() not null,
        updated_at      timestamp with time zone default now() not null
    );
    """
    t0 = time.monotonic()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)
        await conn.commit()
    logger.info("Schema ensured: public.dota_teams (time=%d ms)", int((time.monotonic() - t0) * 1000))


def chunked(seq: list[TeamRow], size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


async def count_teams(pool: AsyncConnectionPool) -> int:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM public.dota_teams;")
            (cnt,) = await cur.fetchone()
            return int(cnt)


async def insert_new_teams(pool: AsyncConnectionPool, teams: list[TeamRow]) -> int:
    """
    INSERT ONLY:
    - новые вставляем
    - существующие (по liquipedia_slug) пропускаем
    """
    if not teams:
        logger.warning("No teams to insert (empty list).")
        return 0

    sql = """
    INSERT INTO public.dota_teams (liquipedia_slug, liquipedia_url, name, created_at, updated_at)
    VALUES (%s, %s, %s, now(), now())
    ON CONFLICT (liquipedia_slug) DO NOTHING;
    """

    total_inserted = 0
    t0 = time.monotonic()

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"SET lock_timeout = '{DB_LOCK_TIMEOUT_MS}ms';")
            await cur.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT_MS}ms';")

            for part_no, part in enumerate(chunked(teams, INSERT_CHUNK_SIZE), start=1):
                params = [(t.liquipedia_slug, t.liquipedia_url, t.name) for t in part]

                logger.info("Insert chunk %d: start rows=%d", part_no, len(params))
                part_t0 = time.monotonic()

                await cur.executemany(sql, params)

                inserted = cur.rowcount or 0  # сколько реально вставилось в этом executemany
                total_inserted += inserted

                logger.info(
                    "Insert chunk %d: done attempted=%d inserted=%d time=%d ms",
                    part_no,
                    len(params),
                    inserted,
                    int((time.monotonic() - part_t0) * 1000),
                )

        await conn.commit()

    logger.info("Insert complete: inserted=%d, time=%d ms", total_inserted, int((time.monotonic() - t0) * 1000))
    return total_inserted


async def main() -> None:
    run_t0 = time.monotonic()

    logger.info("Starting teams sync (insert-only).")
    logger.info("Liquipedia source: %s", PORTAL_TEAMS_URL)
    logger.info("DB target: host=%s port=%s db=%s user=%s", DB_HOST, DB_PORT, DB_NAME, DB_USER)
    logger.info(
        "DB timeouts: connect_timeout=%ss lock_timeout=%sms statement_timeout=%sms",
        DB_CONNECT_TIMEOUT_SEC,
        DB_LOCK_TIMEOUT_MS,
        DB_STATEMENT_TIMEOUT_MS,
    )

    html = fetch_html(PORTAL_TEAMS_URL)
    teams = parse_teams_from_portal(html)

    pool = AsyncConnectionPool(CONNINFO, min_size=1, max_size=5, open=False)
    await pool.open()
    try:
        await ensure_schema(pool)

        before = await count_teams(pool)
        inserted = await insert_new_teams(pool, teams)
        after = await count_teams(pool)

        logger.info(
            "Teams sync done: parsed=%d, inserted_new=%d, db_before=%d, db_after=%d, total_time=%d ms",
            len(teams),
            inserted,
            before,
            after,
            int((time.monotonic() - run_t0) * 1000),
        )
    finally:
        await pool.close()
        logger.info("DB pool closed.")


if __name__ == "__main__":
    asyncio.run(main())
