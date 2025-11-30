from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional

from dotenv import load_dotenv
import psycopg
from fastapi import FastAPI, HTTPException

# ---------- Конфиг и подключение к БД ----------

def get_db_connection():
    load_dotenv()

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
        raise RuntimeError("Не хватает параметров подключения к БД в .env")

    conn_str = (
        f"host={DB_HOST} "
        f"port={DB_PORT} "
        f"dbname={DB_NAME} "
        f"user={DB_USER} "
        f"password={DB_PASSWORD}"
    )

    return psycopg.connect(conn_str)


# ---------- Логика выборки матчей ----------

def get_matches_for_date(target_date: date):
    """
    Возвращает список матчей на указанную дату (по МСК).
    """
    tz_msk = timezone(timedelta(hours=3))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    match_time_msk,
                    team1,
                    team2,
                    bo,
                    tournament,
                    status,
                    score
                FROM dota_matches
                WHERE (match_time_msk AT TIME ZONE 'Europe/Moscow')::date = %s
                ORDER BY match_time_msk;
                """,
                (target_date,),
            )
            rows = cur.fetchall()

    matches = []
    for row in rows:
        match_time_msk, team1, team2, bo_int, tournament, status, score = row

        # Приводим к МСК
        if match_time_msk.tzinfo is None:
            match_time_msk = match_time_msk.replace(tzinfo=timezone.utc).astimezone(tz_msk)
        else:
            match_time_msk = match_time_msk.astimezone(tz_msk)

        matches.append(
            {
                "match_time_msk": match_time_msk.isoformat(),
                "time_msk": match_time_msk.strftime("%H:%M"),
                "team1": team1,
                "team2": team2,
                "bo": bo_int,
                "tournament": tournament or "",
                "status": status or "unknown",
                "score": score,  # может быть None, если результата ещё нет
            }
        )

    return matches


# ---------- FastAPI-приложение ----------

app = FastAPI(
    title="CyberMatches API",
    description="Простой API для матчей Dota 2 из Liquipedia",
    version="1.0.0",
)


@app.get("/dota/matches/today")
def matches_today():
    """
    Матчи на сегодня (по МСК).
    """
    tz_msk = timezone(timedelta(hours=3))
    today_msk = datetime.now(tz_msk).date()

    matches = get_matches_for_date(today_msk)

    return {
        "date": today_msk.strftime("%Y-%m-%d"),
        "timezone": "Europe/Moscow",
        "matches": matches,
    }


@app.get("/dota/matches/{date_str}")
def matches_by_date(date_str: str):
    """
    Матчи на произвольную дату (по МСК).
    Формат даты: dd-mm-yyyy, например: 26-11-2025
    """
    try:
        target_date = datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Неверный формат даты. Используй dd-mm-yyyy, например: 26-11-2025",
        )

    matches = get_matches_for_date(target_date)

    return {
        "date": target_date.strftime("%Y-%m-%d"),
        "timezone": "Europe/Moscow",
        "matches": matches,
    }
