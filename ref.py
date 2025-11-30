import os
from dotenv import load_dotenv
import psycopg
import sys


def build_match_uid(team1: str, team2: str, tournament: str | None, bo_int: int | None) -> str:
    """
    Полностью повторяет алгоритм из main.py:
    lower(trim(team1)) | lower(trim(team2)) | lower(trim(tournament)) | bo_int
    """
    team1 = (team1 or "").strip().lower()
    team2 = (team2 or "").strip().lower()
    tournament = (tournament or "").strip().lower()
    bo = str(bo_int or "").strip()

    return f"{team1}|{team2}|{tournament}|{bo}"


def main():
    load_dotenv()

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
        print("❌ Не хватает параметров в .env")
        print("DB_HOST:", DB_HOST)
        print("DB_PORT:", DB_PORT)
        print("DB_NAME:", DB_NAME)
        print("DB_USER:", DB_USER)
        print("DB_PASSWORD:", "SET" if DB_PASSWORD else "EMPTY")
        sys.exit(1)

    # БЕЗ sslmode=require, как в main.py
    conn_str = (
        f"host={DB_HOST} "
        f"port={DB_PORT} "
        f"dbname={DB_NAME} "
        f"user={DB_USER} "
        f"password={DB_PASSWORD}"
    )

    print("Подключаемся к базе...")
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            # 1) Получаем все строки
            cur.execute("""
                SELECT id, team1, team2, tournament, bo, match_uid
                FROM dota_matches
                ORDER BY id;
            """)
            rows = cur.fetchall()

            print(f"Найдено строк: {len(rows)}")

            updated = 0
            skipped = 0

            # 2) Проходим по строкам и пересчитываем
            for row in rows:
                match_id, team1, team2, tournament, bo_int, old_uid = row
                new_uid = build_match_uid(team1, team2, tournament, bo_int)

                if new_uid == (old_uid or ""):
                    skipped += 1
                    continue

                # 3) Обновляем только если uid реально отличается
                cur.execute(
                    """
                    UPDATE dota_matches
                    SET match_uid = %s,
                        updated_at = now()
                    WHERE id = %s;
                    """,
                    (new_uid, match_id),
                )

                updated += 1

            print("Готово.")
            print(f"Обновлено UID: {updated}")
            print(f"Пропущено (уже правильные): {skipped}")

        conn.commit()

    print("✨ Все match_uid пересчитаны.")


if __name__ == "__main__":
    main()
