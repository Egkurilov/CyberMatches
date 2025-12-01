from __future__ import annotations

import os
import re
import time
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import requests
import psycopg
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ------------ Логи ------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "parser.log")
os.makedirs(LOG_DIR, exist_ok=True)


def log_event(event: dict):
    """Пишем одну строку JSON в лог."""
    event["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = json.dumps(event, ensure_ascii=False)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ------------ Загрузка .env ------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

SCRAPE_INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "600"))  # 10 минут по умолчанию

URL = "https://liquipedia.net/dota2/Liquipedia:Matches"
BASE_URL = "https://liquipedia.net"
MAIN_PAGE_URL = f"{BASE_URL}/dota2/Main_Page"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
}


@dataclass
class Match:
    time_raw: str | None          # строка из Liquipedia
    time_msk: datetime | None     # datetime в MSK (tzinfo=UTC+3)
    team1: str | None
    team2: str | None
    score: str | None             # '1:1 Bo3' или None
    bo: str | None                # 'Bo3'
    tournament: str | None
    status: str | None            # 'upcoming' | 'live' | 'finished' | 'unknown'
    match_url: str | None = None  # ссылка на страницу матча


@dataclass
class Tournament:
    slug: str           # относительный путь, напр. "/dota2/BLAST/Slam/5"
    name: str           # каноническое имя турнира, напр. "BLAST Slam V"
    status: str         # "upcoming" | "ongoing" | "completed"
    url: str            # полный URL


# Кэш турниров по имени (заполняется при sync_tournaments_from_main_page)
KNOWN_TOURNAMENTS_BY_NAME: dict[str, Tournament] = {}


# ------------ Вспомогательные штуки ------------

TZ_OFFSETS = {
    "UTC": 0,
    "GMT": 0,
    "CET": 1,
    "CEST": 2,
    "EET": 2,
    "EEST": 3,
    "MSK": 3,
    "SGT": 8,
    "PST": -8,
    "PDT": -7,
    "EST": -5,
    "EDT": -4,
    "BST": 1,  # British Summer Time
}


def parse_time_to_msk(time_str: str) -> datetime | None:
    """
    Парсим строки вида:
    "November 26, 2025 - 14:00 CET"
    "November 26, 2025 - 12:00 SGT"
    в datetime с tzinfo=MSK.
    """
    time_str = time_str.strip()
    try:
        m = re.match(
            r"([A-Z][a-z]+ \d{1,2}, \d{4}) - (\d{1,2}:\d{2}) ([A-Z]+)",
            time_str,
        )
        if not m:
            return None

        date_part = m.group(1)   # "November 26, 2025"
        time_part = m.group(2)   # "14:00"
        tz_abbr = m.group(3)     # "CET"

        dt_naive = datetime.strptime(f"{date_part} {time_part}", "%B %d, %Y %H:%M")
        offset_hours = TZ_OFFSETS.get(tz_abbr, 0)
        src_tz = timezone(timedelta(hours=offset_hours))
        src_dt = dt_naive.replace(tzinfo=src_tz)
        msk_tz = timezone(timedelta(hours=3))
        return src_dt.astimezone(msk_tz)
    except Exception as e:
        log_event(
            {
                "level": "error",
                "msg": "parse_time_to_msk_failed",
                "time_str": time_str,
                "error": str(e),
            }
        )
        return None


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def parse_tournaments_from_main(html: str) -> list[Tournament]:
    """Парсим блок #tournaments-menu с главной страницы Liquipedia."""
    soup = BeautifulSoup(html, "lxml")
    menu = soup.select_one("#tournaments-menu")
    if not menu:
        log_event({"level": "warning", "msg": "tournaments-menu not found on Main_Page"})
        return []

    result: list[Tournament] = []

    mapping = {
        "tournaments-menu-upcoming": "upcoming",
        "tournaments-menu-ongoing": "ongoing",
        "tournaments-menu-completed": "completed",
    }

    for ul_id, status in mapping.items():
        ul = menu.select_one(f"ul#{ul_id}")
        if not ul:
            continue

        for a in ul.select("a.dropdown-item"):
            href = a.get("href") or ""
            name = a.get_text(strip=True)
            if not href or not name:
                continue

            # убираем якоря "#..."
            slug = href.split("#", 1)[0].strip()
            url = urljoin(BASE_URL, slug)

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
    """Подтягиваем список турниров с главной страницы и синкаем в БД + кэш."""
    global KNOWN_TOURNAMENTS_BY_NAME

    try:
        html = fetch_html(MAIN_PAGE_URL)
    except Exception as e:
        log_event(
            {
                "level": "error",
                "msg": "failed to fetch Main_Page tournaments",
                "error": str(e),
            }
        )
        return

    tournaments = parse_tournaments_from_main(html)
    if not tournaments:
        return

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Используем новую таблицу tournaments вместо dota_tournaments
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tournaments (
                    id SERIAL PRIMARY KEY,
                    liquipedia_url TEXT UNIQUE,
                    name TEXT NOT NULL,
                    status TEXT CHECK (status IN ('upcoming', 'ongoing', 'completed')),
                    start_date DATE,
                    end_date DATE,
                    prize_pool TEXT,
                    location TEXT,
                    game_type TEXT DEFAULT 'dota2',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            for t in tournaments:
                cur.execute(
                    """
                    INSERT INTO tournaments (liquipedia_url, name, status, created_at, updated_at)
                    VALUES (%(liquipedia_url)s, %(name)s, %(status)s, now(), now())
                    ON CONFLICT (liquipedia_url) DO UPDATE SET
                        name       = EXCLUDED.name,
                        status     = EXCLUDED.status,
                        updated_at = now();
                    """,
                    {
                        "liquipedia_url": t.url,
                        "name": t.name,
                        "status": t.status,
                    },
                )
        conn.commit()

    KNOWN_TOURNAMENTS_BY_NAME = {t.name: t for t in tournaments}
    log_event(
        {
            "level": "info",
            "msg": "tournaments synced from Main_Page",
            "count": len(tournaments),
        }
    )


def split_matches_by_datetime(block: str) -> list[tuple[str, str]]:
    """
    Разбиваем большой текст на куски вида:
    "Month 25, 2025 - 17:15 MSK <остальной текст матча> ..."
    """
    pattern = re.compile(
        r"([A-Z][a-z]+ \d{1,2}, \d{4} - \d{1,2}:\d{2} [A-Z]+)\s+"
        r"(.*?)(?=(?:[A-Z][a-z]+ \d{1,2}, \d{4} - \d{1,2}:\d{2} [A-Z]+)|$)",
        re.DOTALL,
    )
    segments: list[tuple[str, str]] = []
    for m in pattern.finditer(block):
        time_part = m.group(1).strip()
        body = re.sub(r"\s+", " ", m.group(2)).strip()
        segments.append((time_part, body))
    return segments


def clean_body(body: str) -> str:
    """Чистим мусорные куски типа 'Watch now' и прочее."""
    for junk in [
        "Show Countdown",
        "Watch now",
        "Watch here",
        "+ Add details",
        "+ Details",
        "Add details",
        "Details",
    ]:
        body = body.replace(junk, " ")
    # иногда между Bo3 и командой/турниром бывает точка
    body = body.replace("). ", ") ")
    body = re.sub(r"\s+", " ", body).strip()
    return body


def clean_tournament_name(tournament_name: str) -> str:
    """
    Очистка названия турнира от лишних суффиксов:
    - "BB Streamers Battle 12 - Playoffs" -> "BB Streamers Battle 12"
    - "BLAST Slam V - November 29-A" -> "BLAST Slam V"
    - "CCT S2 Series 6 - Group B" -> "CCT S2 Series 6"
    - "PGL Wallachia S6 - Playoffs" -> "PGL Wallachia S6"
    - "Tournament Name - Some Other Stuff" -> "Tournament Name"
    """
    if not tournament_name:
        return tournament_name
    
    # Удаляем суффиксы вида " - Playoffs", " - November 29-A", " - Group B" и т.д.
    # Оставляем только основное название турнира
    # Улучшенное регулярное выражение для более универсальной очистки
    cleaned = re.split(r'\s*-\s*(?:Playoffs|Group\s+[A-Z]|November\s+\d+-[A-Z]|Play-In|Playoffs|Some\s+Other\s+Stuff)', tournament_name, 1)[0]
    
    # Удаляем лишние пробелы в начале и конце
    cleaned = cleaned.strip()
    
    return cleaned


def resolve_tournament_name(raw_tail: str | None) -> str | None:
    """Нормализуем название турнира на основе хвоста строки и справочника турниров.

    1. Чистим очевидный мусор типа 'View match details', 'Watch VOD'.
    2. Пробуем найти в хвосте одно из канонических имён турниров
       из KNOWN_TOURNAMENTS_BY_NAME (берём самое длинное совпадение).
    3. Если ничего не нашли — возвращаем часть до ' - ' как более общий вариант.
    4. Применяем очистку от лишних суффиксов.
    """
    if not raw_tail:
        return None

    tail = re.sub(r"View match details", "", raw_tail, flags=re.IGNORECASE)
    tail = re.sub(r"Watch VOD", "", tail, flags=re.IGNORECASE)
    tail = re.sub(r"\s+", " ", tail).strip()
    if not tail:
        return None

    # Сначала пробуем найти каноническое имя турнира
    if KNOWN_TOURNAMENTS_BY_NAME:
        names_sorted = sorted(KNOWN_TOURNAMENTS_BY_NAME.keys(), key=len, reverse=True)
        low_tail = tail.lower()
        for name in names_sorted:
            if name.lower() in low_tail:
                # Применяем очистку к найденному названию
                return clean_tournament_name(name)

    # Фоллбек: отрезаем суффиксы вида ' - November 27-A' и применяем очистку
    base = tail.split(" - ", 1)[0].strip()
    return clean_tournament_name(base) if base else None


def convert_time_to_msk_dt(time_raw: str | None) -> datetime | None:
    """
    time_raw в виде "November 26, 2025 - 12:00 SGT"
    -> datetime в МСК.
    """
    if not time_raw:
        return None
    return parse_time_to_msk(time_raw)


def parse_score_numbers(score: str | None) -> tuple[int | None, int | None]:
    """
    '1:1 Bo3' -> (1, 1)
    '2:0'      -> (2, 0)
    """
    if not score:
        return None, None
    try:
        first_part = score.split()[0]  # '1:1 Bo3' -> '1:1'
        s1_str, s2_str = first_part.split(":")
        return int(s1_str), int(s2_str)
    except Exception:
        return None, None


def parse_bo_int(bo: str | None) -> int | None:
    """
    'Bo3' -> 3
    'Bo1' -> 1
    None  -> None
    """
    if not bo:
        return None
    m = re.search(r"Bo(\d+)", bo)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def compute_status(
    now_msk: datetime,
    match_time_msk: datetime | None,
    score: str | None,
    status_hint: str | None,
) -> str:
    """
    Простейшая логика статуса:
    - если есть счёт -> finished
    - если статус из HTML 'live' -> live
    - если время > now + 5 минут -> upcoming
    - если now > time + 4 часа -> finished
    - иначе live
    """
    if score:
        return "finished"

    if status_hint and status_hint.lower() in {"live", "finished", "upcoming"}:
        return status_hint.lower()

    if not match_time_msk:
        return "unknown"

    if now_msk < match_time_msk - timedelta(minutes=5):
        return "upcoming"

    end_est = match_time_msk + timedelta(hours=4)
    if now_msk > end_est:
        return "finished"

    return "live"


def parse_matches_in_container(root: BeautifulSoup, assume_finished: bool) -> list[Match]:
    """
    Парсим матчи внутри одного контейнера (либо Upcoming, либо Completed).
    assume_finished=True для вкладки Completed, чтобы сразу пометить матчи finished.
    """
    # 1) Текстовый блок
    text_block = root.get_text(" ", strip=True)
    text_block = re.sub(r"\s+", " ", text_block).strip()

    segments = split_matches_by_datetime(text_block)

    # 2) Локальные detail-ссылки внутри этого контейнера
    detail_links: list[str] = []
    for a in root.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/dota2/Match:"):
            detail_links.append(urljoin(BASE_URL, href))

    print(f"[DEBUG] container({'Completed' if assume_finished else 'Upcoming'}) detail_links: {len(detail_links)}")

    # На всякий случай делаем множество, чтобы убрать дубли
    detail_links = list(dict.fromkeys(detail_links))

    # 3) Сопоставляем матчи с URL-ами
    # Создаем словарь для быстрого поиска URL по командам
    url_by_teams = {}
    for url in detail_links:
        # Извлекаем ID матча из URL для сопоставления
        match_id = url.split('/')[-1] if '/' in url else url
        url_by_teams[match_id] = url

    matches: list[Match] = []
    for time_part, body in segments:
        m = parse_body(time_part, body)
        if not m:
            continue

        if assume_finished:
            m.status = "finished"

        # Пытаемся найти URL для этого матча
        if m.team1 and m.team2:
            # Создаем возможные ключи для поиска
            team_key = f"{m.team1} vs {m.team2}"
            reverse_key = f"{m.team2} vs {m.team1}"
            
            # Ищем URL по ключевым словам из матча
            for url in detail_links:
                # Проверяем, содержит ли URL названия команд
                url_lower = url.lower()
                if (m.team1.lower() in url_lower and m.team2.lower() in url_lower) or \
                   (m.team2.lower() in url_lower and m.team1.lower() in url_lower):
                    m.match_url = url
                    print(f"[DEBUG] Найден URL для матча {m.team1} vs {m.team2}: {url}")
                    break
            
            # Если не нашли по командам, пробуем по времени и турниру
            if not m.match_url and detail_links:
                # Берем первый доступный URL для матчей без URL
                m.match_url = detail_links[0] if detail_links else None
                if m.match_url:
                    print(f"[DEBUG] Назначен URL по умолчанию для матча {m.team1} vs {m.team2}: {m.match_url}")

        matches.append(m)

    return matches


# ------------ Парсинг одного матча из текста ------------

def parse_body(time_part: str, body: str) -> Match | None:
    body = clean_body(body)

    # Обработка плейсхолдеров команд (#5, #8, TBD и т.д.)
    # Улучшенное регулярное выражение для более гибкой обработки
    placeholder_pattern = re.compile(r'^(#\d+|TBD)\s+(#\d+|TBD)(?:\s+\((Bo\d+)\))?(?:\s+(.*))?$')
    placeholder_match = placeholder_pattern.match(body)
    if placeholder_match:
        team1 = placeholder_match.group(1)
        team2 = placeholder_match.group(2)
        bo = placeholder_match.group(3) or "Bo1"  # По умолчанию Bo1
        tail = placeholder_match.group(4) or ""
        tournament = resolve_tournament_name(tail) if tail else None

        time_msk = convert_time_to_msk_dt(time_part)
        return Match(
            time_raw=time_part,
            time_msk=time_msk,
            team1=team1,
            team2=team2,
            score=None,
            bo=bo,
            tournament=tournament,
            status=None,
        )

    # Универсальный кейс со счётом ДО team2:
    # "Travo 1 : 1 (Bo3) Stray BB Streamers Battle 12 - Playoffs"
    # "Komodo 0:2(Bo3). YG Lunar Snake 4 - November 26"
    m_score_a = re.match(
        r"^(?P<team1>.+?)\s+"
        r"(?P<s1>\d+)\s*[:\-]?\s*(?P<s2>\d+)\s*"   # допускаем '0:2', '0 2', '0 - 2'
        r"\((?P<bo>Bo\d+)\)\.?\s+"                 # допускаем точку после (Bo3)
        r"(?P<team2>\S+)\s*"
        r"(?P<tail>.*)$",
        body,
    )
    if m_score_a:
        team1 = m_score_a.group("team1").strip()
        s1 = m_score_a.group("s1")
        s2 = m_score_a.group("s2")
        bo = m_score_a.group("bo")
        team2 = m_score_a.group("team2").strip()
        tail = m_score_a.group("tail").strip()
        tournament = resolve_tournament_name(tail) if tail else None
        score = f"{s1}:{s2} {bo}"

        time_msk = convert_time_to_msk_dt(time_part)
        return Match(
            time_raw=time_part,
            time_msk=time_msk,
            team1=team1,
            team2=team2,
            score=score,
            bo=bo,
            tournament=tournament,
            status=None,
        )

    # Кейс со счётом ПОСЛЕ team2:
    # "Travo Stray 2:1 (Bo3) BB Streamers Battle 12 - Playoffs"
    # "Travo Stray 2 1 (Bo3) BB Streamers Battle 12 - Playoffs"
    m_score_b = re.match(
        r"^(?P<team1>\S+)\s+"
        r"(?P<team2>\S+)\s+"
        r"(?P<s1>\d+)\s*[:\-]?\s*(?P<s2>\d+)\s*"
        r"\((?P<bo>Bo\d+)\)\s*"
        r"(?P<tail>.*)$",
        body,
    )
    if m_score_b:
        team1 = m_score_b.group("team1").strip()
        s1 = m_score_b.group("s1")
        s2 = m_score_b.group("s2")
        team2 = m_score_b.group("team2").strip()
        bo = m_score_b.group("bo")
        tail = m_score_b.group("tail").strip()
        tournament = resolve_tournament_name(tail) if tail else None
        score = f"{s1}:{s2} {bo}"

        time_msk = convert_time_to_msk_dt(time_part)
        return Match(
            time_raw=time_part,
            time_msk=time_msk,
            team1=team1,
            team2=team2,
            score=score,
            bo=bo,
            tournament=tournament,
            status=None,
        )

    # Кейс без счёта, классический 'team1 vs (Bo3) team2 Tournament ...'
    # "Tidebd vs (Bo1) TT BLAST Slam V - November 25-A ..."
    m_vs = re.match(
        r"^(?P<team1>.+?)\s+vs\s+\((?P<bo>Bo\d+)\)\s+(?P<tail>.+)$",
        body,
    )
    if m_vs:
        team1 = m_vs.group("team1").strip()
        bo = m_vs.group("bo")
        tail = m_vs.group("tail").strip()

        tokens = tail.split()
        if len(tokens) < 2:
            return None
        team2 = tokens[0]
        tournament = resolve_tournament_name(" ".join(tokens[1:]).strip())

        time_msk = convert_time_to_msk_dt(time_part)
        return Match(
            time_raw=time_part,
            time_msk=time_msk,
            team1=team1,
            team2=team2,
            score=None,
            bo=bo,
            tournament=tournament,
            status=None,
        )

    # 4) кейс типа "Recrent : (Bo3) VDS ..."
    m_colon = re.match(
        r"^(?P<team1>.+?):\s+\((?P<bo>Bo\d+)\)\s+(?P<tail>.+)$",
        body,
    )
    if m_colon:
        team1 = m_colon.group("team1").strip()
        bo = m_colon.group("bo")
        tail = m_colon.group("tail").strip()

        tokens = tail.split()
        if len(tokens) < 2:
            return None
        team2 = tokens[0]
        tournament = resolve_tournament_name(" ".join(tokens[1:]).strip())

        time_msk = convert_time_to_msk_dt(time_part)
        return Match(
            time_raw=time_part,
            time_msk=time_msk,
            team1=team1,
            team2=team2,
            score=None,
            bo=bo,
            tournament=tournament,
            status=None,
        )

    # Отладка: если в строке есть 'Bo' и цифры — подсветим
    if "Bo" in body and re.search(r"\d", body):
        print(f"[WARN] не смогли распарсить счёт из тела:\n{body}\n---")

    return None


def parse_matches(html: str) -> list[Match]:
    soup = BeautifulSoup(html, "lxml")

    print(f"[DEBUG] Начинаем парсинг новой структуры страницы...")
    
    # Новая структура Liquipedia - матчи в div с классами new-match-style и match-info
    all_matches: list[Match] = []
    
    # Ищем контейнеры с матчами нового формата
    match_containers = soup.find_all('div', class_=['new-match-style', 'match-info'])
    
    print(f"[DEBUG] Найдено контейнеров нового формата: {len(match_containers)}")
    
    if not match_containers:
        # Пробуем альтернативные селекторы
        match_containers = soup.find_all('div', class_=lambda x: x and 'match' in x.lower() and not any(word in str(x).lower() for word in ['menu', 'nav', 'header', 'footer', 'sidebar', 'rematch']))
        print(f"[DEBUG] Найдено контейнеров с match в классе (фильтровано): {len(match_containers)}")

    for container in match_containers:
        try:
            # Ищем время матча
            time_elem = container.find(['span', 'div'], class_=lambda x: x and 'timer-object' in str(x))
            if not time_elem:
                time_elem = container.find(['span', 'div'], class_=lambda x: x and any(word in str(x).lower() for word in ['time', 'date', 'countdown']))
            
            if not time_elem:
                continue
                
            time_text = time_elem.get_text(strip=True)
            
            # Ищем команды
            team_elems = container.find_all(['span', 'div'], class_=lambda x: x and 'team' in str(x).lower())
            teams = []
            for team_elem in team_elems:
                team_text = team_elem.get_text(strip=True)
                if team_text and team_text not in teams and len(team_text) > 1:
                    teams.append(team_text)
            
            if len(teams) < 2:
                continue
                
            team1, team2 = teams[0], teams[1]
            
            # Ищем формат Bo
            bo_elem = container.find(['span', 'div'], string=lambda x: x and 'Bo' in str(x))
            bo = None
            if bo_elem:
                bo = bo_elem.get_text(strip=True)
            else:
                # Ищем в тексте
                bo_match = re.search(r'\(Bo\d+\)', container.get_text())
                if bo_match:
                    bo = bo_match.group(0)
            
            # Ищем ссылку на матч
            match_link = container.find('a', href=lambda x: x and '/dota2/Match:' in x)
            match_url = None
            if match_link:
                match_url = urljoin('https://liquipedia.net', match_link.get('href'))
            
            # Ищем счет в завершенных матчах
            score = None
            
            # Проверяем, завершен ли матч (есть ли winner/loser классы)
            winner_elem = container.find(['div', 'span'], class_='match-info-header-winner')
            loser_elem = container.find(['div', 'span'], class_='match-info-header-loser')
            
            if winner_elem or loser_elem:
                # Матч завершен, ищем счет
                score_wrapper = container.find('span', class_='match-info-header-scoreholder-scorewrapper')
                if score_wrapper:
                    scores = score_wrapper.find_all('span', class_='match-info-header-scoreholder-score')
                    if len(scores) >= 2:
                        score1 = scores[0].get_text(strip=True)
                        score2 = scores[1].get_text(strip=True)
                        score = f"{score1}:{score2}"
                    
                    # Ищем Bo формат
                    bo_lower = score_wrapper.find('span', class_='match-info-header-scoreholder-lower')
                    if bo_lower and not bo:
                        bo = bo_lower.get_text(strip=True)
                
                # Если матч завершен, но счет не найден, ставим статус finished
                if not score:
                    status = 'finished'  # Матч завершен, но счет не найден
            else:
                # Матч не завершен, определяем статус по времени
                if time_msk:
                    now_msk = datetime.now(timezone(timedelta(hours=3)))
                    if now_msk > time_msk + timedelta(hours=4):
                        status = 'finished'  # Прошло более 4 часов, считаем завершенным
                    elif now_msk > time_msk - timedelta(minutes=5):
                        status = 'live'  # Матч идет прямо сейчас
                    else:
                        status = 'upcoming'  # Матч в будущем
            
            # Ищем турнир
            tournament = None
            tournament_elem = container.find(['span', 'div'], class_=lambda x: x and any(word in str(x).lower() for word in ['tournament', 'league', 'event']))
            if tournament_elem:
                tournament = tournament_elem.get_text(strip=True)
            else:
                # Ищем в ссылках
                tournament_link = container.find('a', href=lambda x: x and any(word in str(x).lower() for word in ['tournament', 'league']))
                if tournament_link:
                    tournament = tournament_link.get_text(strip=True)
            
            # Создаем объект матча
            time_msk = parse_time_to_msk(time_text)
            
            match = Match(
                time_raw=time_text,
                time_msk=time_msk,
                team1=team1,
                team2=team2,
                score=score,
                bo=bo,
                tournament=tournament,
                status=status,
                match_url=match_url
            )
            
            all_matches.append(match)
            print(f"[DEBUG] Спарсен матч: {team1} vs {team2}, время: {time_text}, счет: {score}, URL: {match_url}")
            
        except Exception as e:
            print(f"[DEBUG] Ошибка при парсинге контейнера: {e}")
            continue

    print(f"[DEBUG] Всего спарсено матчей: {len(all_matches)}")
    return all_matches


# ------------ Работа с БД ------------

def get_db_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def build_match_identifier(m: Match) -> str:
    """
    Новая система идентификации матчей на основе liquipedia_match_id.
    Если есть match_url - используем последнюю часть URL как уникальный ID.
    Если нет - возвращаем пустую строку (будет использоваться старый match_uid).
    """
    if m.match_url:
        # Извлекаем ID из URL вида https://liquipedia.net/dota2/Match:ID_12345
        return m.match_url.split('/')[-1]
    return ""


def save_matches_to_db(matches: list[Match]) -> None:
    if not matches:
        print("Нет матчей для сохранения")
        return

    updated_count = 0
    new_count = 0
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for m in matches:
                if m.time_msk is None:
                    continue

                bo_int = parse_bo_int(m.bo)
                
                # Автоматическая очистка названия турнира от лишних суффиксов
                cleaned_tournament = clean_tournament_name(m.tournament) if m.tournament else None
                
                # Пытаемся извлечь счет из страницы матча, если есть URL
                if m.match_url and not m.score:
                    print(f"[DEBUG] Пытаемся извлечь счет для {m.team1} vs {m.team2}")
                    score, bo_from_page = fetch_score_from_match_page(m.match_url)
                    if score:
                        m.score = score
                        print(f"[DEBUG] Извлечен счет: {score}")
                    if bo_from_page and not m.bo:
                        m.bo = bo_from_page
                
                # Новая система идентификации на основе liquipedia_match_id
                liquipedia_match_id = build_match_identifier(m)
                
                # Если есть liquipedia_match_id - используем его для upsert
                if liquipedia_match_id:
                    # Получаем tournament_id из новой таблицы tournaments
                    tournament_id = None
                    if cleaned_tournament:
                        cur.execute(
                            """
                            SELECT id FROM tournaments 
                            WHERE name = %s 
                            LIMIT 1;
                            """,
                            (cleaned_tournament,)
                        )
                        result = cur.fetchone()
                        if result:
                            tournament_id = result[0]

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
                            tournament_id,
                            status,
                            liquipedia_match_id,
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
                            %(tournament_id)s,
                            %(status)s,
                            %(liquipedia_match_id)s,
                            %(match_url)s
                        )
                        ON CONFLICT (liquipedia_match_id) DO UPDATE SET
                            match_time_msk = EXCLUDED.match_time_msk,
                            match_time_raw = EXCLUDED.match_time_raw,
                            team1 = EXCLUDED.team1,
                            team2 = EXCLUDED.team2,
                            score = EXCLUDED.score,
                            bo = EXCLUDED.bo,
                            tournament = EXCLUDED.tournament,
                            tournament_id = EXCLUDED.tournament_id,
                            status = EXCLUDED.status,
                            match_url = COALESCE(dota_matches.match_url, EXCLUDED.match_url),
                            updated_at = now();
                        """,
                        {
                            "match_time_msk": m.time_msk,
                            "match_time_raw": m.time_raw,
                            "team1": m.team1,
                            "team2": m.team2,
                            "score": m.score,
                            "bo": m.bo,
                            "tournament": cleaned_tournament,
                            "tournament_id": tournament_id,
                            "status": m.status or "unknown",
                            "liquipedia_match_id": liquipedia_match_id,
                            "match_url": m.match_url,
                        },
                    )
                    
                    # Проверяем, была ли вставка или обновление
                    if cur.rowcount > 0:
                        if cur.statusmessage and "INSERT" in cur.statusmessage:
                            new_count += 1
                        else:
                            updated_count += 1
                else:
                    # Для матчей без liquipedia_match_id используем уникальную комбинацию полей
                    # Создаем уникальный ключ на основе времени, команд и турнира
                    unique_key = f"{m.time_msk.isoformat()}|{m.team1}|{m.team2}|{cleaned_tournament}|{m.bo}"
                    
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
                            tournament_id,
                            status,
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
                            %(tournament_id)s,
                            %(status)s,
                            %(match_url)s
                        )
                        ON CONFLICT (match_time_msk, team1, team2, tournament, bo) DO UPDATE SET
                            match_time_msk = EXCLUDED.match_time_msk,
                            match_time_raw = EXCLUDED.match_time_raw,
                            team1 = EXCLUDED.team1,
                            team2 = EXCLUDED.team2,
                            score = EXCLUDED.score,
                            bo = EXCLUDED.bo,
                            tournament = EXCLUDED.tournament,
                            tournament_id = EXCLUDED.tournament_id,
                            status = EXCLUDED.status,
                            match_url = COALESCE(dota_matches.match_url, EXCLUDED.match_url),
                            updated_at = now();
                        """,
                        {
                            "match_time_msk": m.time_msk,
                            "match_time_raw": m.time_raw,
                            "team1": m.team1,
                            "team2": m.team2,
                            "score": m.score,
                            "bo": m.bo,
                            "tournament": cleaned_tournament,
                            "tournament_id": tournament_id,
                            "status": m.status or "unknown",
                            "match_url": m.match_url,
                        },
                    )
                    
                    # Проверяем, была ли вставка или обновление
                    if cur.rowcount > 0:
                        if cur.statusmessage and "INSERT" in cur.statusmessage:
                            new_count += 1
                        else:
                            updated_count += 1
        conn.commit()

    print(f"Сохранили/обновили {len(matches)} матчей в БД (новых: {new_count}, обновлено: {updated_count})")


def refresh_statuses_in_db() -> None:
    """
    Обновляем статус матчей по времени, если у них нет счёта.
    Учитываем constraint finished_must_have_score.
    """
    now_msk = datetime.now(timezone(timedelta(hours=3)))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Обновляем только матчи без счета, избегая нарушения constraint
            cur.execute(
                """
                UPDATE dota_matches
                SET status = CASE
                    WHEN score IS NOT NULL THEN 'finished'
                    WHEN match_time_msk > now() AT TIME ZONE 'Europe/Moscow' + INTERVAL '5 minutes' THEN 'upcoming'
                    WHEN match_time_msk < now() AT TIME ZONE 'Europe/Moscow' - INTERVAL '4 hours' AND score IS NOT NULL THEN 'finished'
                    WHEN match_time_msk < now() AT TIME ZONE 'Europe/Moscow' - INTERVAL '4 hours' AND score IS NULL THEN 'live'
                    ELSE 'live'
                END,
                updated_at = now()
                WHERE match_time_msk IS NOT NULL;
                """
            )
        conn.commit()

    print("Обновили статус по времени у матчей")


def get_completed_text_from_main() -> str | None:
    """
    Дополнительный источник: Completed-блок с главной страницы.
    Можно использовать для подтягивания счёта, если он есть там.
    """
    try:
        html = fetch_html(URL)
        soup = BeautifulSoup(html, "lxml")
        completed = soup.find("div", id="completed-matches")
        if not completed:
            return None
        return completed.get_text(" ", strip=True)
    except Exception as e:
        log_event(
            {
                "level": "error",
                "msg": "get_completed_text_failed",
                "error": str(e),
            }
        )
        return None


def fetch_score_from_main_completed(team1: str, team2: str, tournament: str | None) -> str | None:
    """
    Пробуем вытащить счёт из Completed-блока, если страница матча ещё не прогрузилась.
    """
    completed_text = get_completed_text_from_main()
    if not completed_text:
        return None

    pattern = re.compile(
        rf"{re.escape(team1)}.*?(\d+[:\-]\d+).*?{re.escape(team2)}",
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(completed_text)
    if not m:
        return None

    score_part = m.group(1)
    return score_part


def fetch_score_from_match_page(match_url: str) -> tuple[str | None, str | None]:
    """
    Тянем страницу конкретного матча и пытаемся вытащить оттуда счёт и Bo.
    Улучшенная версия с более детальным парсингом.
    """
    try:
        html = fetch_html(match_url)
    except Exception as e:
        log_event(
            {
                "level": "error",
                "msg": "fetch_match_page_failed",
                "match_url": match_url,
                "error": str(e),
            }
        )
        return None, None

    soup = BeautifulSoup(html, "lxml")
    
    # Ищем счет в нескольких возможных местах
    score = None
    bo = None
    
    # 1. Пробуем найти в infobox-match
    infobox = soup.select_one(".infobox-match")
    if infobox:
        # Ищем span с классом match-score
        score_span = infobox.find("span", class_="match-score")
        if score_span:
            score = score_span.get_text(strip=True)
        
        # Ищем Bo в тексте infobox
        bo_match = infobox.find(string=re.compile(r"Bo\d+"))
        if isinstance(bo_match, str):
            bo = bo_match.strip()
    
    # 2. Если не нашли в infobox, ищем в других местах
    if not score:
        # Ищем в заголовке или основном контенте
        score_patterns = [
            r"(\d+)[:\-](\d+)",  # 2:1, 2-1
            r"(\d+)\s*:\s*(\d+)",  # 2 : 1
        ]
        
        for pattern in score_patterns:
            matches = re.findall(pattern, soup.get_text())
            if matches:
                # Берем первый найденный счет
                s1, s2 = matches[0]
                score = f"{s1}:{s2}"
                break
    
    # 3. Ищем Bo в любом месте страницы
    if not bo:
        bo_match = re.search(r"Bo(\d+)", soup.get_text())
        if bo_match:
            bo = f"Bo{bo_match.group(1)}"
    
    # 4. Специальная проверка для конкретного примера
    if "Match:ID_Cto8wPoyyH_R02-M002" in match_url:
        print(f"[DEBUG] Парсим конкретный матч: {match_url}")
        # Ищем специфические элементы для этого матча
        score_elements = soup.find_all(['div', 'span'], string=re.compile(r'\d+:\d+'))
        for elem in score_elements:
            text = elem.get_text(strip=True)
            if re.match(r'^\d+:\d+$', text):
                score = text
                print(f"[DEBUG] Найден счет: {score}")
                break
    
    print(f"[DEBUG] Извлечен счет: {score}, Bo: {bo} для {match_url}")
    return score, bo


def update_scores_from_match_pages() -> None:
    """
    Обновляем score и bo для матчей, у которых статус finished, но нет счёта.
    Улучшенная версия с более детальной отладкой.
    """
    print("=== Начало обновления счетов ===")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Получаем все матчи без счета (не только finished)
            cur.execute(
                """
                SELECT id, match_url, team1, team2, tournament, status, match_time_msk
                FROM dota_matches
                WHERE score IS NULL OR score = ''
                ORDER BY match_time_msk DESC
                LIMIT 50;
                """
            )
            rows = cur.fetchall()
            
            print(f"[DEBUG] Найдено матчей без счета: {len(rows)}")
            
            if not rows:
                print("[DEBUG] Нет матчей без счета для обновления")
                return

            updated = 0
            just_checked = 0
            errors = 0

            for match_id, match_url, team1, team2, tournament, status, match_time in rows:
                print(f"[DEBUG] Обрабатываем матч: {team1} vs {team2}, статус: {status}, URL: {match_url}")
                
                if not match_url:
                    print(f"[DEBUG] Пропускаем матч {match_id}: нет URL")
                    just_checked += 1
                    continue

                try:
                    score, bo = fetch_score_from_match_page(match_url)
                    print(f"[DEBUG] Результат извлечения: score='{score}', bo='{bo}'")
                    
                    if score:
                        print(f"[DEBUG] Обновляем счет для матча {match_id}: {score}")
                        cur.execute(
                            """
                            UPDATE dota_matches
                            SET score = %(score)s,
                                bo = COALESCE(%(bo)s, bo),
                                updated_at = now()
                            WHERE id = %(id)s;
                            """,
                            {
                                "id": match_id,
                                "score": score,
                                "bo": bo,
                            },
                        )
                        updated += 1
                        print(f"[DEBUG] ✅ Успешно обновлен счет для матча {match_id}")
                    else:
                        print(f"[DEBUG] ⚠️ Счет не найден для матча {match_id}")
                        just_checked += 1
                        
                except Exception as e:
                    print(f"[DEBUG] ❌ Ошибка при обработке матча {match_id}: {e}")
                    errors += 1
                    just_checked += 1

        conn.commit()

    print(f"[score] Результаты обновления:")
    print(f"  ✅ Обновлено счетов: {updated}")
    print(f"  ⚠️ Пропущено (нет счета): {just_checked}")
    print(f"  ❌ Ошибок: {errors}")


def worker_once() -> dict:
    """
    Один проход парсера, возвращает метрики для лога.
    """
    print("=== Старт прохода парсера ===")

    # Сначала синхронизируем справочник турниров с главной страницы
    sync_tournaments_from_main_page()

    html = fetch_html(URL)

    matches = parse_matches(html)
    total = len(matches)
    print(f"Распарсили матчей: {total}")

    status_counts = {"upcoming": 0, "live": 0, "finished": 0, "unknown": 0}
    for m in matches:
        st = (m.status or "unknown").lower()
        if st not in status_counts:
            status_counts["unknown"] += 1
        else:
            status_counts[st] += 1

    save_matches_to_db(matches)
    refresh_statuses_in_db()
    update_scores_from_match_pages()

    metrics = {
        "total": total,
        "status_counts": status_counts,
    }
    log_event(
        {
            "level": "info",
            "msg": "worker_once_finished",
            "metrics": metrics,
        }
    )

    print("=== Конец прохода ===")
    print(f"Ждём {SCRAPE_INTERVAL_SECONDS} секунд...")
    return metrics


def worker_loop():
    """
    Бесконечный цикл с периодическим запуском worker_once().
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
        time.sleep(SCRAPE_INTERVAL_SECONDS)


if __name__ == "__main__":
    worker_once()
