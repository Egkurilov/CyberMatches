from __future__ import annotations

import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional, Dict, Any
from functools import lru_cache

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
    """Асинхронный пул подключений к PostgreSQL"""
    
    def __init__(self):
        self.conn_str = (
            f"host={DB_HOST} "
            f"port={DB_PORT} "
            f"dbname={DB_NAME} "
            f"user={DB_USER} "
            f"password={DB_PASSWORD}"
        )
        self._pool = None
    
    async def init_pool(self):
        """Инициализация пула подключений"""
        if not self._pool:
            self._pool = await AsyncConnection.connect(
                self.conn_str,
                autocommit=True,
                min_size=2,
                max_size=10
            )
            logger.info("Пул подключений к БД инициализирован")
    
    @asynccontextmanager
    async def get_connection(self):
        """Контекстный менеджер для получения соединения из пула"""
        if not self._pool:
            await self.init_pool()
        
        async with self._pool.cursor() as cur:
            yield cur
    
    async def close_pool(self):
        """Закрытие пула подключений"""
        if self._pool:
            await self._pool.close()
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
    """Кэширование форматирования дат"""
    return datetime.strptime(date_str, "%d-%m-%Y").date()

# ---------- Бизнес-логика ----------

async def get_matches_for_date(target_date: date) -> List[Dict[str, Any]]:
    """
    Асинхронно получает список матчей на указанную дату (по МСК).
    Использует пул подключений и оптимизированные запросы.
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
                liquipedia_match_id
            FROM dota_matches
            WHERE (match_time_msk AT TIME ZONE 'Europe/Moscow')::date = %s
            ORDER BY match_time_msk;
            """,
            (target_date,),
        )
        rows = await cur.fetchall()

    matches = []
    for row in rows:
        match_time_msk, team1, team2, bo_int, tournament, status, score, liquipedia_match_id = row

        # Приводим к МСК
        if match_time_msk.tzinfo is None:
            match_time_msk = match_time_msk.replace(tzinfo=timezone.utc).astimezone(tz_msk)
        else:
            match_time_msk = match_time_msk.astimezone(tz_msk)

        matches.append({
            "match_time_msk": match_time_msk.isoformat(),
            "time_msk": match_time_msk.strftime("%H:%M"),
            "team1": team1,
            "team2": team2,
            "bo": bo_int,
            "tournament": tournament or "",
            "status": status or "unknown",
            "score": score,
            "liquipedia_match_id": liquipedia_match_id,
        })

    logger.info(f"Получено {len(matches)} матчей для даты {target_date}")
    return matches

async def get_matches_with_tournament_filter(
    target_date: date, 
    tournament_ids: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    """
    Получает матчи с возможной фильтрацией по турнирам.
    Использует JOIN с таблицей tournaments для оптимизации.
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
            dm.liquipedia_match_id
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

    matches = []
    for row in rows:
        match_time_msk, team1, team2, bo_int, tournament, status, score, liquipedia_match_id = row

        if match_time_msk.tzinfo is None:
            match_time_msk = match_time_msk.replace(tzinfo=timezone.utc).astimezone(tz_msk)
        else:
            match_time_msk = match_time_msk.astimezone(tz_msk)

        matches.append({
            "match_time_msk": match_time_msk.isoformat(),
            "time_msk": match_time_msk.strftime("%H:%M"),
            "team1": team1,
            "team2": team2,
            "bo": bo_int,
            "tournament": tournament or "",
            "status": status or "unknown",
            "score": score,
            "liquipedia_match_id": liquipedia_match_id,
        })

    return matches

# ---------- FastAPI-приложение с улучшениями ----------

app = FastAPI(
    title="CyberMatches API",
    description="Оптимизированный API для матчей Dota 2 из Liquipedia",
    version="2.0.0",
)

# Жизненный цикл приложения
@app.on_event("startup")
async def startup_event():
    """Инициализация при старте приложения"""
    await db_pool.init_pool()
    logger.info("CyberMatches API запущен")

@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при остановке приложения"""
    await db_pool.close_pool()
    logger.info("CyberMatches API остановлен")

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
