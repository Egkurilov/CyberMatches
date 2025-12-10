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
        # psycopg3: .closed -> True/False (или 0/1)
        if self._pool is None or getattr(self._pool, "closed", False):
            if self._pool is not None:
                try:
                    await self._pool.close()
                except Exception:
                    # если оно уже мёртвое — ну и ладно
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
            # Типичная история: "the connection is closed", "server closed the connection" и т.п.
            logger.error(f"Ошибка работы с БД (соединение будет пересоздано): {e}")
            try:
                if self._pool and not getattr(self._pool, "closed", False):
                    await self._pool.close()
            except Exception:
                pass

            # помечаем текущее соединение как сломанное — следующее обращение создаст новое
            self._pool = None
            # Пробрасываем исключение наверх — конкретный запрос всё равно не удался,
            # но следующий уже будет на свежем соединении
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

    # вариант 1: новый формат match_uid = "lp:ID_..."
    if match_uid and match_uid.startswith("lp:"):
        return match_uid[3:]

    # вариант 2: достаём прямо из URL
    if match_url:
        m1 = re.search(r"Match:(ID_[^&#/?]+)", match_url)
        if m1:
            return m1.group(1)
        m2 = re.search(r"(ID_[A-Za-z0-9]+(?:_[0-9]+)?)", match_url)
        if m2:
            return m2.group(1)

    return None


# ---------- Бизнес-логика ----------

async def get_matches_for_date(target_date: date) -> List[Dict[str, Any]]:
    """
    Асинхронно получает список матчей на указанную дату (по МСК).

    Делает:
      - вытаскивание liquipedia_match_id из match_uid / match_url при необходимости;
      - дедупликацию матчей (по Liquipedia ID или fallback-ключу);
      - фильтрацию мусорных TBD-плейсхолдеров:
          если в том же турнире и в то же время есть матч с нормальными командами,
          то TBD-версия скрывается.
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

        # приводим время к МСК и к таймзоне
        if match_time_msk.tzinfo is None:
            match_time_msk = match_time_msk.replace(tzinfo=timezone.utc).astimezone(tz_msk)
        else:
            match_time_msk = match_time_msk.astimezone(tz_msk)

        # вытаскиваем Liquipedia ID из БД / match_uid / match_url
        liquipedia_id = liqui_in_db or extract_liquipedia_id(match_uid, match_url)


        match_dict: Dict[str, Any] = {
            "match_time_msk": match_time_msk.isoformat(),
            "time_msk": match_time_msk.strftime("%H:%M"),
            "team1": team1,
            "team1_url": get_team_url(conn_sync, row["team1"]),
            "team2": team2,
            "team2_url": get_team_url(conn_sync, row["team2"]),
            "bo": bo_int,
            "tournament": tournament or "",
            "status": status or "unknown",
            "score": score,
            "liquipedia_match_id": liquipedia_id,
        }

        # --- ключ для дедупликации ---
        if liquipedia_id:
            # если есть нормальный Liquipedia ID — доверяем ему
            key = ("id", liquipedia_id)
        else:
            # fallback: по слоту и парам команд
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
            # выбираем "лучший" матч:
            # 1) у кого есть нормальный score (не None и не "0:0")
            # 2) если одинаково — у кого есть bo
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

    # --- превращаем в список ---
    matches = list(matches_by_key.values())

    # --- фильтрация TBD-плейсхолдеров ---
    # если в том же турнире и в то же время есть матч с нормальными командами,
    # то матчи с TBD в этом слоте выкидываем.
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
            # есть нормальный матч в этом же слоте — TBD-версию выкидываем
            continue
        filtered_matches.append(m)

    logger.info(f"Получено {len(filtered_matches)} матчей для даты {target_date}")
    return filtered_matches


async def get_matches_with_tournament_filter(
        target_date: date,
        tournament_ids: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    """
    Получает матчи с возможной фильтрацией по турнирам.
    Использует JOIN с таблицей tournaments для оптимизации.
    Плюс:
      - вытаскивает liquipedia_match_id;
      - убирает дубли.
    """
    tz_msk = _get_timezone_msk()

    query = """
        SELECT
            dm.match_time_msk,
            dm.team1,
            dm.team2,
            dm.bo,
            dm.tournament,
            dm.status,
            dm.score,
            dm.liquipedia_match_id,
            dm.match_uid,
            dm.match_url
        FROM dota_matches dm
    """

    params = [target_date]

    if tournament_ids:
        query += " JOIN tournaments t ON dm.tournament_id = t.id WHERE t.id = ANY(%s) AND"
        params.append(tournament_ids)
    else:
        query += " WHERE"

    query += " (dm.match_time_msk AT TIME ZONE 'Europe/Moscow')::date = %s ORDER BY dm.match_time_msk;"

    async with db_pool.get_connection() as cur:
        await cur.execute(query, params)
        rows = await cur.fetchall()

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

        match_dict = {
            "match_id": match_id,
            "team1": team1,
            "team1_url": get_team_url(conn_sync, team1),
            "team2": team2,
            "team2_url": get_team_url(conn_sync, team2),
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

    return list(matches_by_key.values())


def get_team_url(conn, team_name: str) -> str | None:
    """
    Возвращает Liquipedia URL команды по названию (как оно распарсено в матчах).
    Если команда не найдена в dota_teams — возвращает None.
    """
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
            (team_name,)
        )
        row = cur.fetchone()
        return row[0] if row else None


# ---------- FastAPI-приложение с улучшениями ----------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения (startup/shutdown)."""
    # startup
    logging.info("Startup: инициализация пула подключений к БД")
    await db_pool.init_pool()

    yield  # <-- здесь приложение работает

    # shutdown
    logging.info("Shutdown: закрытие пула подключений к БД")
    await db_pool.close_pool()


app = FastAPI(
    title="CyberMatches API",
    description="Оптимизированный API для матчей Dota 2 из Liquipedia",
    version="2.0.0",
    lifespan=lifespan,
)


@app.get("/dota/matches/today")
async def matches_today():
    """
    Асинхронно получает матчи на сегодня (по МСК).
    """
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
    """
    Асинхронно получает матчи на произвольную дату (по МСК).
    Формат даты: dd-mm-yyyy, например: 26-11-2025
    """
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

@app.get("/dota/matches/stats")
async def matches_stats():
    """
    Получает статистику по матчам (общее количество, количество по статусам и т.д.)
    """
    try:
        async with db_pool.get_connection() as cur:
            # Общая статистика
            await cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN status = 'upcoming' THEN 1 END) as upcoming,
                    COUNT(CASE WHEN status = 'live' THEN 1 END) as live,
                    COUNT(CASE WHEN status = 'finished' THEN 1 END) as finished
                FROM dota_matches;
            """)
            total_stats = await cur.fetchone()
            
            # Статистика по турнирам
            await cur.execute("""
                SELECT tournament, COUNT(*) as count
                FROM dota_matches
                GROUP BY tournament
                ORDER BY count DESC
                LIMIT 10;
            """)
            tournament_stats = await cur.fetchall()
            
            # Статистика по датам
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
    """
    Проверка здоровья API и подключения к БД
    """
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
