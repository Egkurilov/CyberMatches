from __future__ import annotations

import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional, Dict, Any
from functools import lru_cache
import re
from dotenv import load_dotenv
import psycopg
from psycopg import AsyncConnection
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

# ---------- Конфигурация и логирование ----------

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("cybermatches_api")

# Конфигурация БД
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Проверка конфигурации
if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise RuntimeError("Не хватает параметров подключения к БД в .env")

# ---------- Пул подключений к БД ----------
class DatabasePool:
    """Асинхронный пул (по факту один коннект) к PostgreSQL"""

    def __init__(self):
        self.conn_str = (
            f"host={DB_HOST} "
            f"port={DB_PORT} "
            f"dbname={DB_NAME} "
            f"user={DB_USER} "
            f"password={DB_PASSWORD}"
        )
        self._pool: Optional[AsyncConnection] = None

    async def _create_connection(self) -> AsyncConnection:
        """Создаёт новое соединение с БД"""
        conn = await AsyncConnection.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            autocommit=True,
        )
        return conn

    async def _ensure_connection(self):
        """
        Гарантирует, что у нас есть живое соединение.
        Если коннект отсутствует или закрыт — создаём новый.
        """
        if self._pool is None or getattr(self._pool, "closed", False):
            if self._pool is not None:
                try:
                    await self._pool.close()
                except Exception:
                    pass

            logger.warning("Соединение с БД отсутствует или закрыто, пробуем переподключиться...")
            self._pool = await self._create_connection()
            logger.info("Соединение с БД установлено")

    async def init_pool(self):
        """Инициализация пула подключений"""
        await self._ensure_connection()

    @asynccontextmanager
    async def get_connection(self):
        """
        Контекстный менеджер для получения курсора.
        При каждом вызове проверяем, что коннект жив.
        """
        await self._ensure_connection()

        try:
            async with self._pool.cursor() as cur:
                yield cur
        except (psycopg.OperationalError, psycopg.InterfaceError) as e:
            logger.error(f"Ошибка работы с БД (соединение будет пересоздано): {e}")
            try:
                if self._pool and not getattr(self._pool, "closed", False):
                    await self._pool.close()
            except Exception:
                pass

            self._pool = None
            raise

    async def close_pool(self):
        """Закрытие пула подключений"""
        if self._pool:
            try:
                await self._pool.close()
            finally:
                logger.info("Пул подключений к БД закрыт")


# Создание глобального экземпляра пула
db_pool = DatabasePool()

# ---------- Кэширование и оптимизация ----------

@lru_cache(maxsize=128)
def _get_timezone_msk() -> timezone:
    """Кэширование часового пояса МСК"""
    return timezone(timedelta(hours=3))

@lru_cache(maxsize=32)
def _format_date_cache(date_str: str) -> date:
    """Кэшированное преобразование строки даты в объект date"""
    return datetime.strptime(date_str, "%d-%m-%Y").date()


def extract_liquipedia_id(match_uid: Optional[str], match_url: Optional[str]) -> Optional[str]:
    """
    Пытаемся вытащить Liquipedia Match:ID из:
      1) match_uid формата 'lp:ID_xxx'
      2) match_url, где есть 'Match:ID_...'
    """
    if not match_uid and not match_url:
        return None

    if match_uid and match_uid.startswith("lp:"):
        return match_uid[3:]

    if match_url:
        m1 = re.search(r"Match:(ID_[^&#/?]+)", match_url)
        if m1:
            return m1.group(1)
        m2 = re.search(r"(ID_[A-Za-z0-9]+(?:_[0-9]+)?)", match_url)
        if m2:
            return m2.group(1)

    return None


# ---------- DOTA2: бизнес-логика ----------

async def get_matches_for_date(target_date: date) -> List[Dict[str, Any]]:
    """
    Асинхронно получает список матчей на указанную дату (по МСК).
    """
    tz_msk = _get_timezone_msk()

    async with db_pool.get_connection() as cur:
        await cur.execute(
            """
            SELECT
                match_time_msk,
                team1,
                team2,
                bo,
                tournament,
                status,
                score,
                liquipedia_match_id,
                match_uid,
                match_url
            FROM dota_matches
            WHERE (match_time_msk AT TIME ZONE 'Europe/Moscow')::date = %s
            ORDER BY match_time_msk;
            """,
            (target_date,),
        )
        rows = await cur.fetchall()

    all_team_names = []
    for row in rows:
        team1 = row[1]
        team2 = row[2]
        if team1:
            all_team_names.append(team1)
        if team2:
            all_team_names.append(team2)

    team_urls = await get_team_urls_batch(all_team_names)

    matches_by_key: Dict[Any, Dict[str, Any]] = {}

    for row in rows:
        (
            match_time_msk,
            team1,
            team2,
            bo_int,
            tournament,
            status,
            score,
            liqui_in_db,
            match_uid,
            match_url,
        ) = row

        if match_time_msk.tzinfo is None:
            match_time_msk = match_time_msk.replace(tzinfo=timezone.utc).astimezone(tz_msk)
        else:
            match_time_msk = match_time_msk.astimezone(tz_msk)

        liquipedia_id = liqui_in_db or extract_liquipedia_id(match_uid, match_url)

        match_dict: Dict[str, Any] = {
            "match_time_msk": match_time_msk.isoformat(),
            "time_msk": match_time_msk.strftime("%H:%M"),
            "team1": team1,
            "team1_url": team_urls.get(team1) if team1 else None,
            "team2": team2,
            "team2_url": team_urls.get(team2) if team2 else None,
            "bo": bo_int,
            "tournament": tournament or "",
            "status": status or "unknown",
            "score": score,
            "liquipedia_match_id": liquipedia_id,
        }

        if liquipedia_id:
            key = ("id", liquipedia_id)
        else:
            key = (
                "fallback",
                match_time_msk.isoformat(),
                (team1 or "").lower(),
                (team2 or "").lower(),
                (tournament or "").lower(),
                bo_int or 0,
            )

        existing = matches_by_key.get(key)
        if existing is None:
            matches_by_key[key] = match_dict
        else:
            def score_weight(s: Optional[str]) -> int:
                if not s or s == "0:0":
                    return 0
                return 1

            cur_score = existing.get("score")
            new_score = score

            if score_weight(new_score) > score_weight(cur_score):
                matches_by_key[key] = match_dict
            elif score_weight(new_score) == score_weight(cur_score):
                cur_bo = existing.get("bo") or 0
                new_bo = bo_int or 0
                if new_bo > cur_bo:
                    matches_by_key[key] = match_dict

    matches = list(matches_by_key.values())

    non_tbd_slots: set[tuple[str, str]] = set()
    for m in matches:
        if m["team1"] != "TBD" and m["team2"] != "TBD":
            non_tbd_slots.add((m["match_time_msk"], m["tournament"]))

    filtered_matches: List[Dict[str, Any]] = []
    for m in matches:
        if (m["team1"] == "TBD" or m["team2"] == "TBD") and (
            m["match_time_msk"],
            m["tournament"],
        ) in non_tbd_slots:
            continue
        filtered_matches.append(m)

    logger.info(f"Получено {len(filtered_matches)} матчей для даты {target_date}")
    return filtered_matches


def get_team_url(conn, team_name: str) -> str | None:
    if not team_name:
        return None

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT liquipedia_url
            FROM dota_teams
            WHERE LOWER(name) = LOWER(%s)
            LIMIT 1;
            """,
            (team_name,),
        )
        row = cur.fetchone()
        return row[0] if row else None


async def get_team_urls_batch(team_names: List[str]) -> Dict[str, Optional[str]]:
    """
    Пакетная загрузка URL команд за один запрос.
    Возвращает dict: {team_name: liquipedia_url or None}
    """
    if not team_names:
        return {}

    unique_names = list(set(name for name in team_names if name))
    if not unique_names:
        return {}

    async with db_pool.get_connection() as cur:
        await cur.execute(
            """
            SELECT name, liquipedia_url
            FROM dota_teams
            WHERE LOWER(name) = ANY(%s);
            """,
            ([name.lower() for name in unique_names],),
        )
        rows = await cur.fetchall()

    result = {name.lower(): None for name in unique_names}
    for row_name, row_url in rows:
        result[row_name.lower()] = row_url

    return {name: result.get(name.lower()) for name in team_names}


# ---------- CS2: бизнес-логика (ОБНОВЛЕНО ПОД НОВУЮ СХЕМУ) ----------

async def get_cs2_team_urls_batch(team_names: List[str]) -> Dict[str, Optional[str]]:
    """
    Пакетная загрузка URL команд CS2 (Liquipedia) из таблицы cs2_teams за один запрос.
    Возвращает dict: {team_name: liquipedia_url or None}
    """
    if not team_names:
        return {}

    unique_names = list(set(name for name in team_names if name))
    if not unique_names:
        return {}

    async with db_pool.get_connection() as cur:
        await cur.execute(
            """
            SELECT name, liquipedia_url
            FROM cs2_teams
            WHERE LOWER(name) = ANY(%s);
            """,
            ([name.lower() for name in unique_names],),
        )
        rows = await cur.fetchall()

    result = {name.lower(): None for name in unique_names}
    for row_name, row_url in rows:
        if row_name:
            result[str(row_name).lower()] = row_url

    # Важно: возвращаем по исходным именам, чтобы не ломать внешний интерфейс
    return {name: result.get(name.lower()) for name in team_names}


async def get_cs2_matches_for_date(target_date: date) -> List[Dict[str, Any]]:
    """
    Асинхронно получает список CS2 матчей на указанную дату (по МСК)
    из таблицы public.cs2_matches (НОВАЯ СХЕМА).

    Используем match_time_msk, score, tournament, team1_url/team2_url если они уже есть.
    Если url команд не заполнены — добираем через cs2_teams по name.
    """
    tz_msk = _get_timezone_msk()

    async with db_pool.get_connection() as cur:
        await cur.execute(
            """
            SELECT
                id,
                match_time_msk,
                team1,
                team2,
                score,
                bo,
                tournament,
                status,
                match_uid,
                match_url,
                liquipedia_match_id,
                team1_url,
                team2_url
            FROM cs2_matches
            WHERE (match_time_msk AT TIME ZONE 'Europe/Moscow')::date = %s
            ORDER BY match_time_msk;
            """,
            (target_date,),
        )
        rows = await cur.fetchall()

    # Собираем команды для batch lookup (только для тех матчей, где URL команд не заполнены)
    need_lookup_team_names: List[str] = []
    for row in rows:
        team1 = row[2]
        team2 = row[3]
        team1_url = row[11]
        team2_url = row[12]

        if team1 and not team1_url:
            need_lookup_team_names.append(team1)
        if team2 and not team2_url:
            need_lookup_team_names.append(team2)

    team_urls_lookup = await get_cs2_team_urls_batch(need_lookup_team_names)

    matches: List[Dict[str, Any]] = []
    seen_uids: set[str] = set()

    for row in rows:
        (
            row_id,
            match_time_msk,
            team1,
            team2,
            score,
            bo_int,
            tournament,
            status,
            match_uid,
            match_url,
            liquipedia_match_id,
            team1_url,
            team2_url,
        ) = row

        # match_uid в схеме NOT NULL + UNIQUE, но на всякий случай
        if match_uid and match_uid in seen_uids:
            continue
        if match_uid:
            seen_uids.add(match_uid)

        if match_time_msk is None:
            continue

        if match_time_msk.tzinfo is None:
            when_msk = match_time_msk.replace(tzinfo=timezone.utc).astimezone(tz_msk)
        else:
            when_msk = match_time_msk.astimezone(tz_msk)

        resolved_team1_url = team1_url or (team_urls_lookup.get(team1) if team1 else None)
        resolved_team2_url = team2_url or (team_urls_lookup.get(team2) if team2 else None)

        # Чтобы клиенту всегда было, за что зацепиться:
        stable_match_id = liquipedia_match_id or match_uid or (str(row_id) if row_id is not None else None)

        matches.append(
            {
                "match_time_msk": when_msk.isoformat(),
                "time_msk": when_msk.strftime("%H:%M"),
                "team1": team1,
                "team1_url": resolved_team1_url,
                "team2": team2,
                "team2_url": resolved_team2_url,
                "bo": bo_int,
                "tournament": tournament or "",
                "status": status or "unknown",
                "score": score,
                # сохраняем “как у dota2”, но под капотом это может быть твой stable id
                "liquipedia_match_id": stable_match_id,
                # дополнительные поля — не мешают, но полезны
                "id": row_id,
                "match_uid": match_uid,
                "match_url": match_url,
            }
        )

    logger.info(f"Получено {len(matches)} CS2 матчей для даты {target_date}")
    return matches


# ---------- FastAPI-приложение ----------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения (startup/shutdown)."""
    logging.info("Startup: инициализация пула подключений к БД")
    await db_pool.init_pool()
    yield
    logging.info("Shutdown: закрытие пула подключений к БД")
    await db_pool.close_pool()


app = FastAPI(
    title="CyberMatches API",
    description="Оптимизированный API для матчей Dota 2 из Liquipedia + CS2",
    version="2.1.0",
    lifespan=lifespan,
)

# ---------- Dota2 endpoints ----------

@app.get("/dota/matches/today")
async def matches_today():
    """Асинхронно получает матчи на сегодня (по МСК)."""
    try:
        tz_msk = _get_timezone_msk()
        today_msk = datetime.now(tz_msk).date()

        matches = await get_matches_for_date(today_msk)

        return {
            "date": today_msk.strftime("%Y-%m-%d"),
            "timezone": "Europe/Moscow",
            "matches": matches,
            "total": len(matches),
        }
    except Exception as e:
        logger.error(f"Ошибка при получении матчей на сегодня: {e}")
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера при получении матчей"
        )

@app.get("/dota/matches/{date_str}")
async def matches_by_date(date_str: str):
    """Асинхронно получает матчи на произвольную дату (по МСК). dd-mm-yyyy"""
    try:
        target_date = _format_date_cache(date_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Неверный формат даты. Используй dd-mm-yyyy, например: 26-11-2025",
        )

    try:
        matches = await get_matches_for_date(target_date)

        return {
            "date": target_date.strftime("%Y-%m-%d"),
            "timezone": "Europe/Moscow",
            "matches": matches,
            "total": len(matches),
        }
    except Exception as e:
        logger.error(f"Ошибка при получении матчей на дату {date_str}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера при получении матчей"
        )


# ---------- CS2 endpoints ----------

@app.get("/cs2/matches/today")
async def cs2_matches_today():
    """Асинхронно получает CS2 матчи на сегодня (по МСК)."""
    try:
        tz_msk = _get_timezone_msk()
        today_msk = datetime.now(tz_msk).date()

        matches = await get_cs2_matches_for_date(today_msk)

        return {
            "date": today_msk.strftime("%Y-%m-%d"),
            "timezone": "Europe/Moscow",
            "matches": matches,
            "total": len(matches),
        }
    except Exception as e:
        logger.error(f"Ошибка при получении CS2 матчей на сегодня: {e}")
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера при получении матчей"
        )


@app.get("/cs2/matches/{date_str}")
async def cs2_matches_by_date(date_str: str):
    """Асинхронно получает CS2 матчи на произвольную дату (по МСК). dd-mm-yyyy"""
    try:
        target_date = _format_date_cache(date_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Неверный формат даты. Используй dd-mm-yyyy, например: 26-11-2025",
        )

    try:
        matches = await get_cs2_matches_for_date(target_date)

        return {
            "date": target_date.strftime("%Y-%m-%d"),
            "timezone": "Europe/Moscow",
            "matches": matches,
            "total": len(matches),
        }
    except Exception as e:
        logger.error(f"Ошибка при получении CS2 матчей на дату {date_str}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера при получении матчей"
        )


# ---------- Общие endpoints ----------

@app.get("/dota/matches/stats")
async def matches_stats():
    """Статистика по Dota матчам."""
    try:
        async with db_pool.get_connection() as cur:
            await cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN status = 'upcoming' THEN 1 END) as upcoming,
                    COUNT(CASE WHEN status = 'live' THEN 1 END) as live,
                    COUNT(CASE WHEN status = 'finished' THEN 1 END) as finished
                FROM dota_matches;
            """)
            total_stats = await cur.fetchone()

            await cur.execute("""
                SELECT tournament, COUNT(*) as count
                FROM dota_matches
                GROUP BY tournament
                ORDER BY count DESC
                LIMIT 10;
            """)
            tournament_stats = await cur.fetchall()

            await cur.execute("""
                SELECT
                    (match_time_msk AT TIME ZONE 'Europe/Moscow')::date as match_date,
                    COUNT(*) as count
                FROM dota_matches
                WHERE match_time_msk >= NOW() - INTERVAL '30 days'
                GROUP BY match_date
                ORDER BY match_date DESC
                LIMIT 30;
            """)
            date_stats = await cur.fetchall()

        return {
            "total_matches": total_stats[0],
            "status_breakdown": {
                "upcoming": total_stats[1],
                "live": total_stats[2],
                "finished": total_stats[3],
            },
            "top_tournaments": [
                {"tournament": row[0], "count": row[1]}
                for row in tournament_stats
            ],
            "recent_activity": [
                {"date": row[0].isoformat(), "count": row[1]}
                for row in date_stats
            ],
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера при получении статистики"
        )

@app.get("/health")
async def health_check():
    """Проверка здоровья API и подключения к БД."""
    try:
        async with db_pool.get_connection() as cur:
            await cur.execute("SELECT 1;")
            await cur.fetchone()

        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.getenv("API_PORT", 8050)),
        reload=False,
    )
