import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
import psycopg


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


def get_today_matches():
    """
    Возвращает список сегодняшних матчей (по МСК) из БД.
    """
    # МСК = UTC+3
    tz_msk = timezone(timedelta(hours=3))
    now_msk = datetime.now(tz_msk)
    today_msk = now_msk.date()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Берём матчи, которые по МСК выпадают на сегодняшний день
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
                (today_msk,),
            )
            rows = cur.fetchall()

    matches = []
    for row in rows:
        match_time_msk, team1, team2, bo_int, tournament, status, score = row

        # match_time_msk хранится с таймзоной (скорее всего UTC+3), но на всякий случай приведём к МСК
        if match_time_msk.tzinfo is None:
            match_time_msk = match_time_msk.replace(tzinfo=timezone.utc).astimezone(tz_msk)
        else:
            match_time_msk = match_time_msk.astimezone(tz_msk)

        time_str = match_time_msk.strftime("%H:%M")

        bo_str = f"Bo{bo_int}" if bo_int is not None else ""
        tournament = tournament or ""
        status = status or "unknown"

        matches.append(
            {
                "time": time_str,
                "team1": team1,
                "team2": team2,
                "bo": bo_str,
                "tournament": tournament,
                "status": status,
                "score": score,
            }
        )

    return today_msk, matches


def print_today_matches():
    today_msk, matches = get_today_matches()

    print(f"Матчи на сегодня (МСК) — {today_msk.isoformat()}")
    print("-" * 80)

    if not matches:
        print("Сегодня матчей не найдено 🤷‍♂️")
        return

    for m in matches:
        time_part = f"{m['time']} MSK"
        vs_part = f"{m['team1']} vs {m['team2']}"
        bo_part = f" ({m['bo']})" if m['bo'] else ""
        score_part = f" | счёт: {m['score']}" if m['score'] else ""
        status_part = f" [{m['status']}]" if m['status'] else ""
        tournament_part = m["tournament"]

        line = f"{time_part:<10} | {vs_part:<30}{bo_part} | {tournament_part}{score_part}{status_part}"
        print(line)


if __name__ == "__main__":
    print_today_matches()
