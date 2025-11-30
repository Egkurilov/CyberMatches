import os
from dataclasses import dataclass
from typing import Optional, List, Dict

from dotenv import load_dotenv
import psycopg


@dataclass
class MatchRow:
    id: int
    match_uid: str
    score: Optional[str]
    match_url: Optional[str]
    status: Optional[str]
    updated_at: Optional[str]


def calc_score(row: MatchRow) -> int:
    """
    Оцениваем «полезность» записи:
    +2 за наличие score
    +1 за наличие match_url
    +1 если status = 'finished'
    """
    points = 0
    if row.score:
        points += 2
    if row.match_url:
        points += 1
    if row.status == "finished":
        points += 1
    return points


def choose_best(rows: List[MatchRow]) -> MatchRow:
    """
    Выбираем лучшую запись из группы одного match_uid:
    1) по calc_score
    2) по updated_at (новее лучше)
    3) по id (больше лучше)
    """
    def sort_key(r: MatchRow):
        return (
            calc_score(r),
            r.updated_at or "",
            r.id,
        )

    # max по ключу — лучший
    best = max(rows, key=sort_key)
    return best


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
        return

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
            # 1) Находим все match_uid, у которых больше одной записи
            cur.execute("""
                SELECT match_uid, COUNT(*) AS cnt
                FROM dota_matches
                GROUP BY match_uid
                HAVING COUNT(*) > 1;
            """)
            dup_uids = cur.fetchall()

            if not dup_uids:
                print("✅ Дубли по match_uid не найдены. Чистить нечего.")
                return

            print(f"Найдено групп-дублей: {len(dup_uids)}")

            uid_list = [row[0] for row in dup_uids]

            # 2) Подтягиваем все строки по этим match_uid
            cur.execute("""
                SELECT
                    id,
                    match_uid,
                    score,
                    match_url,
                    status,
                    updated_at
                FROM dota_matches
                WHERE match_uid = ANY(%s)
                ORDER BY match_uid, id;
            """, (uid_list,))

            rows = cur.fetchall()

            groups: Dict[str, List[MatchRow]] = {}
            for r in rows:
                row = MatchRow(
                    id=r[0],
                    match_uid=r[1],
                    score=r[2],
                    match_url=r[3],
                    status=r[4],
                    updated_at=(r[5].isoformat() if r[5] is not None else None),
                )
                groups.setdefault(row.match_uid, []).append(row)

            total_to_delete = 0
            to_delete_ids: List[int] = []

            print()
            print("Подготовка к удалению дублей...")
            print("--------------------------------")

            for uid, gr in groups.items():
                if len(gr) <= 1:
                    continue

                best = choose_best(gr)
                losers = [r for r in gr if r.id != best.id]

                print(f"match_uid = {uid}")
                print(f"  оставляем id={best.id} (score={best.score!r}, url={best.match_url!r}, status={best.status})")
                if losers:
                    print("  удаляем:")
                    for l in losers:
                        print(f"    id={l.id} (score={l.score!r}, url={l.match_url!r}, status={l.status})")
                        to_delete_ids.append(l.id)
                print()

            total_to_delete = len(to_delete_ids)
            if total_to_delete == 0:
                print("✅ Формально дубликаты есть, но выбирать лучшее не пришлось (что-то пошло не так логически).")
                return

            print("--------------------------------")
            print(f"ИТОГО к удалению: {total_to_delete} записей.")
            confirm = input("Удалить эти записи? Напиши 'yes' для подтверждения: ").strip().lower()
            if confirm != "yes":
                print("Отменено пользователем. Ничего не удалено.")
                return

            # 3) Удаляем
            cur.execute(
                "DELETE FROM dota_matches WHERE id = ANY(%s);",
                (to_delete_ids,),
            )

        conn.commit()

    print(f"🧹 Готово, удалено записей: {total_to_delete}")


if __name__ == "__main__":
    main()
