#!/usr/bin/env python3
"""
teams_parser.py — парсер команд и составов Liquipedia Dota 2.

1. Берём список команд со страницы Portal:Teams.
2. Для каждой команды заходим на её страницу.
3. Сохраняем:
   - страну;
   - регион;
   - активный состав (ник, имя, дата присоединения);
   - инактивный состав.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple

import psycopg
import requests
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv
from urllib.parse import urljoin

# ---------------------------------------------------------
# ЛОГИ / ОКРУЖЕНИЕ
# ---------------------------------------------------------

logger = logging.getLogger(__name__)

BASE_URL = "https://liquipedia.net"
TEAMS_PORTAL_URL = f"{BASE_URL}/dota2/Portal:Teams"

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
}


# ---------------------------------------------------------
# ДАТАКЛАССЫ
# ---------------------------------------------------------

@dataclass
class PlayerInfo:
    nickname: str
    real_name: Optional[str] = None
    liquipedia_url: Optional[str] = None
    joined_at: Optional[date] = None
    joined_raw: Optional[str] = None
    role: Optional[str] = None


@dataclass
class TeamInfo:
    slug: str              # "/dota2/1win_Team"
    name: str              # "1w Team"
    url: str               # полный URL
    country: Optional[str] = None
    region: Optional[str] = None
    active_roster: List[PlayerInfo] = field(default_factory=list)
    inactive_roster: List[PlayerInfo] = field(default_factory=list)


# ---------------------------------------------------------
# УТИЛИТЫ
# ---------------------------------------------------------

def get_db_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def normalize_whitespace(s: str) -> str:
    return " ".join(s.split())


def parse_join_date(raw: Optional[str]) -> Tuple[Optional[date], Optional[str]]:
    """
    Пытаемся распарсить дату присоединения.
    На Liquipedia часто формат типа: "February 10, 2024" или "2024-02-10".
    Если не получилось — возвращаем (None, raw).
    """
    if not raw:
        return None, None

    raw = normalize_whitespace(raw)
    formats = [
        "%B %d, %Y",   # February 10, 2024
        "%d %B %Y",    # 10 February 2024
        "%Y-%m-%d",    # 2024-02-10
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.date(), raw
        except ValueError:
            continue
    # если не смогли распарсить — всё равно сохраняем сырой текст
    return None, raw


# ---------------------------------------------------------
# СХЕМА БД (CREATE TABLE IF NOT EXISTS)
# ---------------------------------------------------------

def ensure_team_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dota_teams (
                id              SERIAL PRIMARY KEY,
                liquipedia_slug TEXT UNIQUE NOT NULL,
                liquipedia_url  TEXT NOT NULL,
                name            TEXT NOT NULL,
                short_name      TEXT,
                country         TEXT,
                region          TEXT,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dota_players (
                id              SERIAL PRIMARY KEY,
                nickname        TEXT NOT NULL,
                real_name       TEXT,
                liquipedia_url  TEXT,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dota_players_nickname
            ON dota_players (lower(nickname));
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dota_team_members (
                id          SERIAL PRIMARY KEY,
                team_id     INTEGER NOT NULL REFERENCES dota_teams(id) ON DELETE CASCADE,
                player_id   INTEGER NOT NULL REFERENCES dota_players(id) ON DELETE CASCADE,
                is_active   BOOLEAN NOT NULL,
                joined_at   DATE,
                raw_joined  TEXT,
                role        TEXT,
                UNIQUE(team_id, player_id, is_active)
            );
            """
        )
    conn.commit()


# ---------------------------------------------------------
# ПАРСИНГ SPAN'ОВ С КОМАНДАМИ С PORTAL:TEAMS
# ---------------------------------------------------------

def parse_teams_from_portal(html: str) -> List[TeamInfo]:
    """
    Берём все <span class="team-template-team-standard"> ... </span>
    и вытаскиваем оттуда название и ссылку.

    Дополнительно:
    - отфильтровываем redlink'и (страницы, которых нет и которые предлагают создать).
    """
    soup = BeautifulSoup(html, "html.parser")
    teams_by_slug: Dict[str, TeamInfo] = {}

    for span in soup.select("span.team-template-team-standard"):
        link = span.select_one("span.team-template-text a")
        if not link:
            continue

        href = link.get("href")
        if not href:
            continue

        # ⚠️ redlink = страница отсутствует / нет прав создать -> пропускаем
        if "redlink=1" in href:
            logger.debug(
                "Пропускаем redlink-команду %s (%s)",
                link.get_text(strip=True),
                href,
            )
            continue

        # чуть подчистим ссылку (уберём якоря, если вдруг есть)
        slug = href.split("#", 1)[0]         # например: "/dota2/1win_Team"
        name = link.get_text(strip=True)     # "1w Team"
        url = urljoin(BASE_URL, slug)

        # На Portal:Teams одна и та же команда может встретиться несколько раз — дедуп
        if slug not in teams_by_slug:
            teams_by_slug[slug] = TeamInfo(slug=slug, name=name, url=url)

    logger.info(
        "Найдено команд на Portal:Teams (после фильтрации redlink'ов): %s",
        len(teams_by_slug),
    )
    return list(teams_by_slug.values())


# ---------------------------------------------------------
# ПАРСИНГ СТРАНИЦЫ КОМАНДЫ
# ---------------------------------------------------------

def _extract_country_region(soup: BeautifulSoup, team: TeamInfo) -> None:
    """
    Вытаскиваем страну и регион из инфобокса команды.

    Структура на Liquipedia сейчас такая:

    <div>
      <div class="infobox-cell-2 infobox-description">Region:</div>
      <div style="width:50%">
        <span class="flag">...</span>
        <a ...>CIS</a>
      </div>
    </div>

    Аналогично для Location / Country.
    """

    # 1. Проходим по всем "левым" ячейкам описания
    for label_div in soup.select("div.infobox-cell-2.infobox-description"):
        label = label_div.get_text(" ", strip=True)
        if not label:
            continue

        label_l = label.lower().rstrip(":").strip()

        # правый соседний div в той же "строке" инфобокса
        value_div = label_div.find_next_sibling("div")
        if not value_div:
            continue

        # тянем текст с учётом ссылок и флагов
        value_text = value_div.get_text(" ", strip=True)
        value = normalize_whitespace(value_text)
        if not value:
            continue

        # Region -> team.region
        if "region" in label_l and not team.region:
            team.region = value

        # Location / Country -> team.country
        if any(k in label_l for k in ("location", "country")) and not team.country:
            team.country = value

    # На всякий случай: можно оставить fallback на старую таблицу, если очень хочется
    # но пока я бы не усложнял, пока не увидим реальный кейс, где это нужно.


def find_roster_tables_candidates(soup: BeautifulSoup) -> List[Tuple[Tag, Optional[str]]]:
    """
    Ищем все wikitable, которые очень похожи на таблицы с составом:
    - в хедерах есть колонки типа ID / Nick / Name / Real name / Role / Join Date.

    Возвращаем список (table, heading_text), где heading_text — текст ближайшего
    предыдущего заголовка (h2/h3/h4), если нашли.
    """
    candidates: List[Tuple[Tag, Optional[str]]] = []

    tables = soup.select("table.wikitable")
    for table in tables:
        header_row = None
        for tr in table.find_all("tr"):
            if tr.find("th"):
                header_row = tr
                break
        if not header_row:
            continue

        headers = [th.get_text(" ", strip=True).lower() for th in header_row.find_all("th")]
        headers_set = set(headers)

        # Хардкодная эвристика: должна быть хотя бы колонка ID/Nick/Player
        # и колонка Name/Real name
        has_id = any(word in h for h in headers for word in ["id", "nick", "player"])
        has_name = any("name" in h and "nick" not in h and "id" not in h for h in headers)

        if not (has_id and has_name):
            continue

        # наш кандидат на таблицу ростера
        # найдём ближайший предыдущий заголовок
        heading_text = None
        prev = table
        while prev:
            prev = prev.find_previous_sibling()
            if prev and prev.name in ("h2", "h3", "h4"):
                heading_text = prev.get_text(" ", strip=True)
                break

        candidates.append((table, heading_text))

    return candidates


def _find_roster_table(
    soup: BeautifulSoup,
    heading_keywords: List[str],
) -> Optional[Tag]:
    """
    Находим заголовок (h2/h3/h4) по ключевым словам и берём следующий за ним <table>.
    heading_keywords, например:
      ["active", "squad"], ["inactive"], ["former", "players"] и т.п.
    """
    def matches(text: str) -> bool:
        t = text.lower()
        return all(k in t for k in heading_keywords)

    for h in soup.find_all(["h2", "h3", "h4"]):
        text = h.get_text(" ", strip=True)
        if matches(text):
            table = h.find_next("table")
            if table:
                return table
    return None


def _parse_roster_table(table: Tag) -> List[PlayerInfo]:
    """
    Универсальный парсер таблицы состава.

    Ожидаем заголовок с колонками вроде:
    - ID / Nick / Nickname / Player
    - Name / Real Name
    - Join Date / Joined / Since
    - Role / Position
    """
    rows = table.find_all("tr")
    if not rows:
        return []

    header_cells = rows[0].find_all(["th", "td"])
    col_idx = {
        "nick": None,
        "real_name": None,
        "joined": None,
        "role": None,
    }

    for idx, cell in enumerate(header_cells):
        label = cell.get_text(" ", strip=True).lower()
        if any(word in label for word in ["id", "nick", "nickname", "player"]):
            col_idx["nick"] = idx
        elif "name" in label and "nick" not in label and "id" not in label:
            col_idx["real_name"] = idx
        elif any(word in label for word in ["join", "since", "from", "date"]):
            col_idx["joined"] = idx
        elif any(word in label for word in ["role", "position"]):
            col_idx["role"] = idx

    def get_cell(cells, idx: Optional[int]) -> Optional[str]:
        if idx is None:
            return None
        if idx >= len(cells):
            return None
        return cells[idx].get_text(" ", strip=True)

    roster: List[PlayerInfo] = []

    for row in rows[1:]:
        cells = row.find_all("td")
        if not cells:
            continue

        nick = get_cell(cells, col_idx["nick"])
        if not nick:
            continue

        real_name = get_cell(cells, col_idx["real_name"])
        joined_raw = get_cell(cells, col_idx["joined"])
        joined_at, joined_raw_norm = parse_join_date(joined_raw)
        role = get_cell(cells, col_idx["role"])

        # ссылка на игрока — обычно в ячейке с ником
        player_url = None
        if col_idx["nick"] is not None and col_idx["nick"] < len(cells):
            link = cells[col_idx["nick"]].find("a", href=True)
            if link:
                player_url = urljoin(BASE_URL, link["href"])

        p = PlayerInfo(
            nickname=normalize_whitespace(nick),
            real_name=normalize_whitespace(real_name) if real_name else None,
            liquipedia_url=player_url,
            joined_at=joined_at,
            joined_raw=joined_raw_norm,
            role=normalize_whitespace(role) if role else None,
        )
        roster.append(p)

    return roster


def parse_team_page(html: str, team: TeamInfo) -> None:
    """
    Дополняем TeamInfo полями:
      - country / region
      - active_roster
      - inactive_roster
    """
    soup = BeautifulSoup(html, "html.parser")

    # страна / регион
    _extract_country_region(soup, team)

    # --- 1. Пытаемся умно найти таблицы с ростером ---
    candidates = find_roster_tables_candidates(soup)

    active_players: List[PlayerInfo] = []
    inactive_players: List[PlayerInfo] = []

    for table, heading_text in candidates:
        players = _parse_roster_table(table)
        if not players:
            continue

        heading_lower = (heading_text or "").lower()
        is_inactive = any(word in heading_lower for word in ["former", "inactive", "past", "previous"])

        if is_inactive:
            inactive_players.extend(players)
        else:
            active_players.extend(players)

    # --- 2. Если новый способ ничего не нашёл — пробуем старый ---
    if not active_players and not inactive_players:
        # активный состав
        active_table = _find_roster_table(
            soup,
            heading_keywords=["active", "squad"],  # "Active Squad"
        ) or _find_roster_table(
            soup,
            heading_keywords=["active", "roster"],  # "Active Roster"
        )
        if active_table:
            active_players = _parse_roster_table(active_table)

        # инактив / former
        inactive_table = (
            _find_roster_table(soup, ["inactive"])
            or _find_roster_table(soup, ["former", "players"])
            or _find_roster_table(soup, ["substitutes"])
        )
        if inactive_table:
            inactive_players = _parse_roster_table(inactive_table)

    team.active_roster = active_players
    team.inactive_roster = inactive_players


# ---------------------------------------------------------
# СОХРАНЕНИЕ В БД
# ---------------------------------------------------------

def upsert_team(cur, team: TeamInfo) -> int:
    cur.execute(
        """
        INSERT INTO dota_teams (liquipedia_slug, liquipedia_url, name, country, region, updated_at)
        VALUES (%(slug)s, %(url)s, %(name)s, %(country)s, %(region)s, NOW())
        ON CONFLICT (liquipedia_slug) DO UPDATE
        SET
            name = EXCLUDED.name,
            liquipedia_url = EXCLUDED.liquipedia_url,
            country = COALESCE(EXCLUDED.country, dota_teams.country),
            region = COALESCE(EXCLUDED.region, dota_teams.region),
            updated_at = NOW()
        RETURNING id;
        """,
        {
            "slug": team.slug,
            "url": team.url,
            "name": team.name,
            "country": team.country,
            "region": team.region,
        },
    )
    team_id = cur.fetchone()[0]
    return team_id


def get_or_create_player(cur, player: PlayerInfo) -> int:
    """
    Ищем игрока по nickname (case-insensitive).
    Если нет — создаём.
    """
    cur.execute(
        """
        SELECT id FROM dota_players
        WHERE lower(nickname) = lower(%(nick)s)
        LIMIT 1;
        """,
        {"nick": player.nickname},
    )
    row = cur.fetchone()
    if row:
        player_id = row[0]
        # обновим real_name / url по мере появления
        cur.execute(
            """
            UPDATE dota_players
            SET real_name = COALESCE(%(real_name)s, real_name),
                liquipedia_url = COALESCE(%(url)s, liquipedia_url),
                updated_at = NOW()
            WHERE id = %(id)s;
            """,
            {
                "id": player_id,
                "real_name": player.real_name,
                "url": player.liquipedia_url,
            },
        )
        return player_id

    # создаём нового
    cur.execute(
        """
        INSERT INTO dota_players (nickname, real_name, liquipedia_url)
        VALUES (%(nick)s, %(real_name)s, %(url)s)
        RETURNING id;
        """,
        {
            "nick": player.nickname,
            "real_name": player.real_name,
            "url": player.liquipedia_url,
        },
    )
    return cur.fetchone()[0]


def upsert_team_members(
    cur,
    team_id: int,
    players: List[PlayerInfo],
    is_active: bool,
) -> None:
    """
    Обновляем membership.

    Стратегия для крон-запуска:
    - если список players ПУСТОЙ — НИЧЕГО не трогаем (считаем, что парсинг не удался или данных нет);
    - если НЕ пустой — полностью обновляем:
        - удаляем старые связи для (team_id, is_active),
        - добавляем актуальные.
    """
    if not players:
        # Ничего не обновляем, чтобы не стереть корректные данные при временном сбое парсинга
        logger.info(
            "Пропускаем обновление состава team_id=%s is_active=%s: players пустой",
            team_id,
            is_active,
        )
        return

    # Чистим только если есть новые данные
    cur.execute(
        """
        DELETE FROM dota_team_members
        WHERE team_id = %(team_id)s AND is_active = %(is_active)s;
        """,
        {"team_id": team_id, "is_active": is_active},
    )

    for p in players:
        player_id = get_or_create_player(cur, p)
        cur.execute(
            """
            INSERT INTO dota_team_members (team_id, player_id, is_active, joined_at, raw_joined, role)
            VALUES (%(team_id)s, %(player_id)s, %(is_active)s, %(joined_at)s, %(raw_joined)s, %(role)s)
            ON CONFLICT (team_id, player_id, is_active) DO UPDATE
            SET joined_at = EXCLUDED.joined_at,
                raw_joined = EXCLUDED.raw_joined,
                role = EXCLUDED.role;
            """,
            {
                "team_id": team_id,
                "player_id": player_id,
                "is_active": is_active,
                "joined_at": p.joined_at,
                "raw_joined": p.joined_raw,
                "role": p.role,
            },
        )



# ---------------------------------------------------------
# ГЛАВНАЯ ФУНКЦИЯ СИНХРОНИЗАЦИИ
# ---------------------------------------------------------

def sync_teams_from_portal() -> None:
    """
    Точка входа:
    1) тянем Portal:Teams
    2) парсим список команд
    3) по каждой команде тянем страницу, парсим состав/регион/страну
    4) сохраняем в БД (коммит после каждой команды)
    """
    try:
        html = fetch_html(TEAMS_PORTAL_URL)
    except Exception as e:
        logger.error("Не удалось скачать Portal:Teams: %s", e)
        return

    teams = parse_teams_from_portal(html)
    if not teams:
        logger.warning("На Portal:Teams не найдено ни одной команды")
        return

    with get_db_connection() as conn:
        logger.info(
            "Подключились к БД: %s@%s:%s/%s",
            DB_USER,
            DB_HOST,
            DB_PORT,
            DB_NAME,
        )

        ensure_team_schema(conn)

        with conn.cursor() as cur:
            total = len(teams)

            for i, team in enumerate(teams, start=1):
                logger.info(
                    "[%d/%d] Обрабатываем команду: %s (%s)",
                    i,
                    total,
                    team.name,
                    team.slug,
                )

                # 1. скачать страницу команды
                try:
                    page_html = fetch_html(team.url)
                except Exception as e:
                    logger.warning(
                        "[%d/%d] НЕ УДАЛОСЬ скачать %s (%s): %s",
                        i,
                        total,
                        team.name,
                        team.url,
                        e,
                    )
                    continue

                # 2. распарсить страницу
                try:
                    parse_team_page(page_html, team)
                except Exception as e:
                    logger.warning(
                        "[%d/%d] Ошибка парсинга страницы %s: %s",
                        i,
                        total,
                        team.name,
                        e,
                    )
                    continue

                logger.info(
                    "[%d/%d] → состав: активных %d, инактив %d, страна=%s, регион=%s",
                    i,
                    total,
                    len(team.active_roster),
                    len(team.inactive_roster),
                    team.country,
                    team.region,
                )

                # 3. сохранить команду
                team_id = upsert_team(cur, team)

                # 4. сохранить игроков
                upsert_team_members(cur, team_id, team.active_roster, is_active=True)
                upsert_team_members(cur, team_id, team.inactive_roster, is_active=False)

                # 🔑 Ключевой момент — фиксируем изменения сразу
                conn.commit()

            # В конце — просто для контроля: сколько всего лежит в таблицах
            cur.execute("SELECT COUNT(*) FROM dota_teams;")
            teams_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dota_players;")
            players_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dota_team_members;")
            members_count = cur.fetchone()[0]

            logger.info(
                "После синка: dota_teams=%d, dota_players=%d, dota_team_members=%d",
                teams_count,
                players_count,
                members_count,
            )

    logger.info("Синхронизация команд завершена: обработано %d команд", len(teams))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    sync_teams_from_portal()
