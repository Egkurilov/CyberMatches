#!/usr/bin/env python3
"""
Финальное исправление для сервера - обработка дубликатов на уровне всей функции
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

def fix_duplicate_issues():
    """Удаляем дубликаты и добавляем правильную обработку"""
    print("🔧 Исправляем проблему с дубликатами...")
    
    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        ) as conn:
            with conn.cursor() as cur:
                # 1. Находим и удаляем дубликаты
                print("1. Удаляем дубликаты из базы данных...")
                
                # Находим дубликаты по комбинации полей
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
                    # Удаляем дубликаты, оставляя только одну запись с наибольшим ID
                    cur.execute("""
                        DELETE FROM dota_matches a
                        USING dota_matches b
                        WHERE a.id < b.id
                          AND a.match_time_msk = b.match_time_msk
                          AND a.team1 = b.team1
                          AND a.team2 = b.team2
                          AND a.tournament = b.tournament
                          AND a.bo = b.bo;
                    """)
                    
                    deleted_count = cur.rowcount
                    print(f"Удалено дубликатов: {deleted_count}")
                
                # 2. Проверяем текущее состояние
                cur.execute("SELECT COUNT(*) FROM dota_matches;")
                total_matches = cur.fetchone()[0]
                print(f"Общее количество матчей после очистки: {total_matches}")
                
                conn.commit()
                print("✅ Очистка дубликатов завершена!")
                
    except Exception as e:
        print(f"❌ Ошибка при очистке дубликатов: {e}")
        return False
    
    return True

if __name__ == "__main__":
    fix_duplicate_issues()
