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


def parse_matches(html: str) -> list[Match]:
    """
    Используем улучшенный парсер из improved_parser.py
    """
    # Импортируем функцию из improved_parser
    from improved_parser import parse_matches_from_html
    return parse_matches_from_html(html)


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
                
                # Получаем tournament_id из новой таблицы tournaments для всех случаев
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
                
                # Если есть liquipedia_match_id - используем его для upsert
                if liquipedia_match_id:
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
                            "bo": bo_int,
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
                
                try:
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
                            "bo": bo_int,
                            "tournament": cleaned_tournament,
                            "tournament_id": tournament_id,
                            "status": m.status or "unknown",
                            "match_url": m.match_url,
                        },
                    )
                except psycopg.errors.UniqueViolation:
                    # Игнорируем дубликаты - это нормально, так как матчи могут дублироваться на странице
                    print(f"[DEBUG] Пропускаем дубликат: {m.team1} vs {m.team2} в {cleaned_tournament}")
                    continue
                    
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


def worker_once() -> dict:
    """
    Один проход парсера, возвращает метрики для лога.
    """
    print("=== Старт прохода парсера ===")

    # Сначала синхронизируем справочник турниров с главной страницы
    sync_tournaments_from_main_page()

    html = fetch_html(URL)

    # Используем улучшенный парсер
    from improved_parser import parse_matches_from_html
    matches = parse_matches_from_html(html)
    
    total = len(matches)
    print(f"Распарсили матчей: {total}")

    status_counts = {"upcoming": 0, "live": 0, "finished": 0, "unknown": 0}
    for m in matches:
        # Улучшенный парсер возвращает словари, а не объекты Match
        if isinstance(m, dict):
            st = (m.get("status") or "unknown").lower()
        else:
            st = (m.status or "unknown").lower()
        if st not in status_counts:
            status_counts["unknown"] += 1
        else:
            status_counts[st] += 1

    # Сохраняем матчи в БД
    if matches:
        # Конвертируем словари в объекты Match
        match_objects = []
        for match_data in matches:
            if isinstance(match_data, dict):
                match_obj = Match(
                    time_raw=match_data.get("time_raw"),
                    time_msk=match_data.get("time_msk"),
                    team1=match_data.get("team1"),
                    team2=match_data.get("team2"),
                    score=match_data.get("score"),
                    bo=match_data.get("bo"),
                    tournament=match_data.get("tournament"),
                    status=match_data.get("status"),
                    match_url=match_data.get("match_url")
                )
                match_objects.append(match_obj)
            else:
                match_objects.append(match_data)
        save_matches_to_db(match_objects)
    
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
