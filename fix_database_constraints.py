#!/usr/bin/env python3
"""
Скрипт для исправления constraints в базе данных
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

def fix_database_constraints():
    """Исправляем constraints в базе данных"""
    print("🔧 Исправляем constraints в базе данных...")
    
    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        ) as conn:
            with conn.cursor() as cur:
                # Проверяем существующие constraints
                print("Проверяем существующие constraints...")
                
                # Проверяем constraints для таблицы dota_matches
                cur.execute("""
                    SELECT conname, contype, pg_get_constraintdef(oid) 
                    FROM pg_constraint 
                    WHERE conrelid = 'dota_matches'::regclass;
                """)
                
                existing_constraints = cur.fetchall()
                print(f"Найдено constraints: {len(existing_constraints)}")
                for constraint in existing_constraints:
                    print(f"  - {constraint[0]} ({constraint[1]}): {constraint[2]}")
                
                # Удаляем существующие уникальные constraints если они есть
                print("Удаляем старые constraints...")
                cur.execute("""
                    DO $$
                    BEGIN
                        IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dota_matches_unique_key') THEN
                            ALTER TABLE dota_matches DROP CONSTRAINT dota_matches_unique_key;
                        END IF;
                    END $$;
                """)
                
                # Добавляем уникальный constraint для liquipedia_match_id
                print("Добавляем уникальный constraint для liquipedia_match_id...")
                cur.execute("""
                    ALTER TABLE dota_matches 
                    ADD CONSTRAINT dota_matches_liquipedia_match_id_unique 
                    UNIQUE (liquipedia_match_id) 
                    WHERE liquipedia_match_id IS NOT NULL AND liquipedia_match_id != '';
                """)
                
                # Добавляем уникальный constraint для комбинации полей
                print("Добавляем уникальный constraint для комбинации полей...")
                cur.execute("""
                    ALTER TABLE dota_matches 
                    ADD CONSTRAINT dota_matches_unique_combination 
                    UNIQUE (match_time_msk, team1, team2, tournament, bo);
                """)
                
                # Проверяем, что constraints добавлены
                print("Проверяем новые constraints...")
                cur.execute("""
                    SELECT conname, contype, pg_get_constraintdef(oid) 
                    FROM pg_constraint 
                    WHERE conrelid = 'dota_matches'::regclass 
                    AND conname IN ('dota_matches_liquipedia_match_id_unique', 'dota_matches_unique_combination');
                """)
                
                new_constraints = cur.fetchall()
                print(f"Добавлено constraints: {len(new_constraints)}")
                for constraint in new_constraints:
                    print(f"  - {constraint[0]} ({constraint[1]}): {constraint[2]}")
                
                conn.commit()
                print("✅ Constraints успешно добавлены!")
                
    except Exception as e:
        print(f"❌ Ошибка при исправлении constraints: {e}")
        return False
    
    return True

if __name__ == "__main__":
    fix_database_constraints()
