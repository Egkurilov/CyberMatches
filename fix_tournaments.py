from __future__ import annotations

import os
import re

import psycopg
from dotenv import load_dotenv


def clean_tournament_name(tournament: str | None) -> str | None:
    if not tournament:
        return None

    t = re.sub(r"\bView match details\b", " ", tournament, flags=re.IGNORECASE)
    t = re.sub(r"\bWatch VOD\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return None

    month_pattern = (
        "January|February|March|April|May|June|July|August|September|October|November|December"
    )

    m = re.match(
        rf"^(?P<base>.+?)\s*-\s*(?:{month_pattern})\s+\d{{1,2}}(?:-[A-Z])?(?:\s+.*)?$",
        t,
    )
    if m:
        return m.group("base").strip()

    return t


def get_db_connection() -> psycopg.Connection:
    load_dotenv()

    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "postgres")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")

    conn_str = (
        f"host={DB_HOST} "
        f"port={DB_PORT} "
        f"dbname={DB_NAME} "
        f"user={DB_USER} "
        f"password={DB_PASSWORD}"
    )
    return psycopg.connect(conn_str)


def main():
    conn = get_db_connection()
    updated = 0
    skipped = 0

    with conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, tournament
            FROM dota_matches
            WHERE tournament IS NOT NULL;
            """
        )
        rows = cur.fetchall()

        for match_id, old_tournament in rows:
            new_tournament = clean_tournament_name(old_tournament)

            # ничего не меняем
            if not new_tournament or new_tournament == old_tournament:
                skipped += 1
                continue

            cur.execute(
                """
                UPDATE dota_matches
                SET tournament = %(tournament)s
                WHERE id = %(id)s;
                """,
                {"tournament": new_tournament, "id": match_id},
            )
            updated += 1

    print(f"Готово. Обновлено {updated} записей, пропущено {skipped}.")


if __name__ == "__main__":
    main()
