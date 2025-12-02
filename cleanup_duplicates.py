#!/usr/bin/env python3
"""
Скрипт для очистки дубликатов в базе данных перед добавлением constraints
"""

import psycopg
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

def cleanup_duplicates():
    """Очищаем дубликаты в базе данных"""
    print("🧹 Очищаем дубликаты в базе данных...")
    
    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        ) as conn:
            with conn.cursor() as cur:
                print("Поиск дубликатов по комбинации полей...")
                
                # Находим дубликаты
                cur.execute("""
                    SELECT match_time_msk, team1, team2, tournament, bo, COUNT(*) as cnt
                    FROM dota_matches
                    WHERE match_time_msk IS NOT NULL 
                      AND team1 IS NOT NULL 
                      AND team2 IS NOT NULL
                    GROUP BY match_time_msk, team1, team2, tournament, bo
                    HAVING COUNT(*) > 1
                    ORDER BY cnt DESC;
                """)
                
                duplicates = cur.fetchall()
                print(f"Найдено групп с дубликатами: {len(duplicates)}")
                
                if duplicates:
                    print("Удаляем дубликаты, оставляя только первую запись в каждой группе...")
                    
                    # Удаляем дубликаты, оставляя только одну запись в каждой группе
                    cur.execute("""
                        DELETE FROM dota_matches a
                        USING dota_matches b
                        WHERE a.id > b.id
                          AND a.match_time_msk = b.match_time_msk
                          AND a.team1 = b.team1
                          AND a.team2 = b.team2
                          AND a.tournament = b.tournament
                          AND a.bo = b.bo;
                    """)
                    
                    deleted_count = cur.rowcount
                    print(f"Удалено дубликатов: {deleted_count}")
                
                # Проверяем дубликаты по liquipedia_match_id
                print("Поиск дубликатов по liquipedia_match_id...")
                cur.execute("""
                    SELECT liquipedia_match_id, COUNT(*) as cnt
                    FROM dota_matches
                    WHERE liquipedia_match_id IS NOT NULL 
                      AND liquipedia_match_id != ''
                    GROUP BY liquipedia_match_id
                    HAVING COUNT(*) > 1
                    ORDER BY cnt DESC;
                """)
                
                liquipedia_duplicates = cur.fetchall()
                print(f"Найдено дубликатов по liquipedia_match_id: {len(liquipedia_duplicates)}")
                
                if liquipedia_duplicates:
                    print("Удаляем дубликаты по liquipedia_match_id...")
                    
                    # Удаляем дубликаты, оставляя только одну запись с наибольшим ID
                    cur.execute("""
                        DELETE FROM dota_matches a
                        USING dota_matches b
                        WHERE a.id < b.id
                          AND a.liquipedia_match_id = b.liquipedia_match_id;
                    """)
                    
                    deleted_liquipedia = cur.rowcount
                    print(f"Удалено дубликатов по liquipedia_match_id: {deleted_liquipedia}")
                
                conn.commit()
                print("✅ Дубликаты успешно удалены!")
                
                # Показываем статистику после очистки
                cur.execute("SELECT COUNT(*) FROM dota_matches;")
                total_matches = cur.fetchone()[0]
                print(f"Общее количество матчей после очистки: {total_matches}")
                
    except Exception as e:
        print(f"❌ Ошибка при очистке дубликатов: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    cleanup_duplicates()
