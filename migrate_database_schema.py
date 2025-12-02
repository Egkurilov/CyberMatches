#!/usr/bin/env python3
"""
Комплексный скрипт миграции базы данных для правильной работы парсера
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

def migrate_database_schema():
    """Выполняем полную миграцию схемы базы данных"""
    print("🗄️ Выполняем миграцию базы данных...")
    
    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        ) as conn:
            with conn.cursor() as cur:
                print("Создаем таблицу tournaments...")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tournaments (
                        id SERIAL PRIMARY KEY,
                        liquipedia_url TEXT UNIQUE,
                        name TEXT NOT NULL,
                        status TEXT CHECK (status IN ('upcoming', 'ongoing', 'completed')),
                        start_date DATE,
                        end_date DATE,
                        prize_pool TEXT,
                        location TEXT,
                        game_type TEXT DEFAULT 'dota2',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """)
                
                print("Создаем таблицу dota_matches с правильными constraints...")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dota_matches (
                        id SERIAL PRIMARY KEY,
                        match_time_msk TIMESTAMPTZ,
                        match_time_raw TEXT,
                        team1 TEXT,
                        team2 TEXT,
                        score TEXT,
                        bo TEXT,
                        tournament TEXT,
                        tournament_id INTEGER REFERENCES tournaments(id),
                        status TEXT CHECK (status IN ('upcoming', 'live', 'finished', 'unknown')),
                        liquipedia_match_id TEXT,
                        match_url TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """)
                
                # Добавляем уникальные constraints
                print("Добавляем уникальные constraints...")
                
                # Constraint для liquipedia_match_id (обычный unique, но будем проверять в коде)
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint 
                            WHERE conname = 'dota_matches_liquipedia_match_id_unique'
                        ) THEN
                            ALTER TABLE dota_matches 
                            ADD CONSTRAINT dota_matches_liquipedia_match_id_unique 
                            UNIQUE (liquipedia_match_id);
                        END IF;
                    END $$;
                """)
                
                # Constraint для комбинации полей
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint 
                            WHERE conname = 'dota_matches_unique_combination'
                        ) THEN
                            ALTER TABLE dota_matches 
                            ADD CONSTRAINT dota_matches_unique_combination 
                            UNIQUE (match_time_msk, team1, team2, tournament, bo);
                        END IF;
                    END $$;
                """)
                
                # Создаем индексы для улучшения производительности
                print("Создаем индексы...")
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_dota_matches_time ON dota_matches(match_time_msk);
                    CREATE INDEX IF NOT EXISTS idx_dota_matches_teams ON dota_matches(team1, team2);
                    CREATE INDEX IF NOT EXISTS idx_dota_matches_status ON dota_matches(status);
                    CREATE INDEX IF NOT EXISTS idx_dota_matches_tournament ON dota_matches(tournament);
                    CREATE INDEX IF NOT EXISTS idx_tournaments_name ON tournaments(name);
                """)
                
                # Проверяем результат
                print("Проверяем созданные объекты...")
                cur.execute("""
                    SELECT table_name, column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name IN ('dota_matches', 'tournaments')
                    ORDER BY table_name, ordinal_position;
                """)
                
                columns = cur.fetchall()
                print(f"Создано столбцов: {len(columns)}")
                
                cur.execute("""
                    SELECT conname, contype, pg_get_constraintdef(oid)
                    FROM pg_constraint
                    WHERE conrelid IN ('dota_matches'::regclass, 'tournaments'::regclass)
                    ORDER BY conname;
                """)
                
                constraints = cur.fetchall()
                print(f"Создано constraints: {len(constraints)}")
                for constraint in constraints:
                    print(f"  - {constraint[0]} ({constraint[1]}): {constraint[2]}")
                
                conn.commit()
                print("✅ Миграция базы данных завершена успешно!")
                
    except Exception as e:
        print(f"❌ Ошибка при миграции базы данных: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    migrate_database_schema()
