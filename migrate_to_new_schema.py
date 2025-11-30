#!/usr/bin/env python3
"""
Миграция на новую схему данных:
1. Создание таблицы tournaments
2. Миграция существующих турниров
3. Обновление таблицы dota_matches
4. Изменение системы идентификации матчей
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Optional, Dict, List

import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")


def get_db_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def create_tournaments_table(conn: psycopg.Connection):
    """Создание таблицы турниров"""
    print("Создание таблицы tournaments...")
    
    with conn.cursor() as cur:
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
        
        # Создаем индексы для производительности
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tournaments_name ON tournaments(name);
            CREATE INDEX IF NOT EXISTS idx_tournaments_status ON tournaments(status);
        """)
    
    conn.commit()
    print("✅ Таблица tournaments создана")


def migrate_existing_tournaments(conn: psycopg.Connection) -> Dict[str, int]:
    """Миграция существующих турниров из dota_matches"""
    print("Миграция существующих турниров...")
    
    with conn.cursor() as cur:
        # Получаем уникальные турниры из dota_matches
        cur.execute("""
            SELECT DISTINCT tournament 
            FROM dota_matches 
            WHERE tournament IS NOT NULL AND tournament != '';
        """)
        
        existing_tournaments = [row[0] for row in cur.fetchall()]
        print(f"Найдено {len(existing_tournaments)} уникальных турниров")
        
        tournament_ids = {}
        
        for tournament_name in existing_tournaments:
            # Генерируем URL для Liquipedia на основе названия
            liquipedia_url = generate_liquipedia_url(tournament_name)
            
            # Определяем статус турнира на основе матчей
            status = determine_tournament_status(cur, tournament_name)
            
            # Вставляем турнир
            cur.execute("""
                INSERT INTO tournaments (liquipedia_url, name, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (liquipedia_url) DO UPDATE SET
                    name = EXCLUDED.name,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING id;
            """, (liquipedia_url, tournament_name, status))
            
            tournament_id = cur.fetchone()[0]
            tournament_ids[tournament_name] = tournament_id
            
            print(f"✓ Мигрирован турнир: {tournament_name} (ID: {tournament_id})")
    
    conn.commit()
    return tournament_ids


def generate_liquipedia_url(tournament_name: str) -> str:
    """Генерация URL для Liquipedia на основе названия турнира"""
    # Заменяем пробелы на подчеркивания и удаляем специальные символы
    clean_name = re.sub(r'[^\w\s-]', '', tournament_name)
    clean_name = re.sub(r'[-\s]+', '_', clean_name)
    return f"https://liquipedia.net/dota2/{clean_name}"


def determine_tournament_status(cur: psycopg.Cursor, tournament_name: str) -> str:
    """Определение статуса турнира на основе матчей"""
    cur.execute("""
        SELECT status, COUNT(*) as count
        FROM dota_matches
        WHERE tournament = %s AND status IS NOT NULL
        GROUP BY status;
    """, (tournament_name,))
    
    status_counts = dict(cur.fetchall())
    
    if not status_counts:
        return 'upcoming'
    
    # Если есть завершенные матчи и нет предстоящих - турнир завершен
    if status_counts.get('finished', 0) > 0 and status_counts.get('upcoming', 0) == 0:
        return 'completed'
    
    # Если есть live матчи - турнир идет
    if status_counts.get('live', 0) > 0:
        return 'ongoing'
    
    # Если есть предстоящие матчи - турнир предстоящий
    if status_counts.get('upcoming', 0) > 0:
        return 'upcoming'
    
    return 'ongoing'


def update_dota_matches_table(conn: psycopg.Connection, tournament_ids: Dict[str, int]):
    """Обновление таблицы dota_matches - добавление tournament_id"""
    print("Обновление таблицы dota_matches...")
    
    with conn.cursor() as cur:
        # Добавляем колонку tournament_id
        cur.execute("""
            ALTER TABLE dota_matches 
            ADD COLUMN IF NOT EXISTS tournament_id INTEGER REFERENCES tournaments(id);
        """)
        
        # Обновляем tournament_id для существующих записей
        for tournament_name, tournament_id in tournament_ids.items():
            cur.execute("""
                UPDATE dota_matches
                SET tournament_id = %s
                WHERE tournament = %s;
            """, (tournament_id, tournament_name))
            
            print(f"✓ Обновлено {cur.rowcount} матчей для турнира: {tournament_name}")
    
    conn.commit()
    print("✅ Таблица dota_matches обновлена")


def add_liquipedia_match_id_column(conn: psycopg.Connection):
    """Добавление колонки для новой системы идентификации матчей"""
    print("Добавление колонки liquipedia_match_id...")
    
    with conn.cursor() as cur:
        # Добавляем колонку liquipedia_match_id
        cur.execute("""
            ALTER TABLE dota_matches 
            ADD COLUMN IF NOT EXISTS liquipedia_match_id TEXT;
        """)
        
        # Создаем уникальный индекс
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dota_matches_liquipedia_id 
            ON dota_matches(liquipedia_match_id) 
            WHERE liquipedia_match_id IS NOT NULL;
        """)
    
    conn.commit()
    print("✅ Колонка liquipedia_match_id добавлена")


def migrate_match_uids_to_liquipedia_ids(conn: psycopg.Connection):
    """Миграция существующих match_uid в liquipedia_match_id где возможно"""
    print("Миграция match_uid в liquipedia_match_id...")
    
    with conn.cursor() as cur:
        # Получаем матчи с match_url и обрабатываем дубликаты
        cur.execute("""
            SELECT id, match_url
            FROM dota_matches
            WHERE match_url IS NOT NULL AND match_url != ''
            ORDER BY id;
        """)
        
        matches = cur.fetchall()
        updated_count = 0
        skipped_count = 0
        used_ids = set()
        
        for match_id, match_url in matches:
            if match_url and '/Match:' in match_url:
                # Извлекаем ID из URL
                liquipedia_id = match_url.split('/')[-1]
                
                # Если ID уже использовался, пропускаем
                if liquipedia_id in used_ids:
                    skipped_count += 1
                    continue
                
                try:
                    cur.execute("""
                        UPDATE dota_matches
                        SET liquipedia_match_id = %s
                        WHERE id = %s;
                    """, (liquipedia_id, match_id))
                    
                    used_ids.add(liquipedia_id)
                    updated_count += 1
                    
                except psycopg.errors.UniqueViolation:
                    # Если возникла ошибка уникальности, пропускаем
                    conn.rollback()
                    skipped_count += 1
                    continue
        
        print(f"✓ Обновлено {updated_count} матчей с liquipedia_match_id")
        print(f"✓ Пропущено {skipped_count} дубликатов")
    
    conn.commit()


def create_migration_report(conn: psycopg.Connection):
    """Создание отчета о миграции"""
    print("\n📊 Отчет о миграции:")
    
    with conn.cursor() as cur:
        # Количество турниров
        cur.execute("SELECT COUNT(*) FROM tournaments;")
        tournament_count = cur.fetchone()[0]
        
        # Количество матчей с tournament_id
        cur.execute("SELECT COUNT(*) FROM dota_matches WHERE tournament_id IS NOT NULL;")
        matches_with_tournament = cur.fetchone()[0]
        
        # Количество матчей с liquipedia_match_id
        cur.execute("SELECT COUNT(*) FROM dota_matches WHERE liquipedia_match_id IS NOT NULL;")
        matches_with_liquipedia_id = cur.fetchone()[0]
        
        # Общее количество матчей
        cur.execute("SELECT COUNT(*) FROM dota_matches;")
        total_matches = cur.fetchone()[0]
        
        print(f"✅ Создано турниров: {tournament_count}")
        print(f"✅ Матчей с привязкой к турниру: {matches_with_tournament}/{total_matches}")
        print(f"✅ Матчей с liquipedia_match_id: {matches_with_liquipedia_id}/{total_matches}")
        
        # Примеры новых данных
        print("\n🔍 Примеры мигрированных данных:")
        cur.execute("""
            SELECT t.name, COUNT(dm.id) as match_count
            FROM tournaments t
            JOIN dota_matches dm ON t.id = dm.tournament_id
            GROUP BY t.name
            ORDER BY match_count DESC
            LIMIT 5;
        """)
        
        for tournament_name, match_count in cur.fetchall():
            print(f"  • {tournament_name}: {match_count} матчей")


def main():
    """Основная функция миграции"""
    print("🚀 Начало миграции на новую схему данных...")
    
    try:
        with get_db_connection() as conn:
            # 1. Создаем таблицу турниров
            create_tournaments_table(conn)
            
            # 2. Мигрируем существующие турниры
            tournament_ids = migrate_existing_tournaments(conn)
            
            # 3. Обновляем таблицу матчей
            update_dota_matches_table(conn, tournament_ids)
            
            # 4. Добавляем колонку для новой системы идентификации
            add_liquipedia_match_id_column(conn)
            
            # 5. Мигрируем существующие данные
            migrate_match_uids_to_liquipedia_ids(conn)
            
            # 6. Создаем отчет
            create_migration_report(conn)
            
            print("\n✅ Миграция успешно завершена!")
            
    except Exception as e:
        print(f"\n❌ Ошибка при миграции: {e}")
        raise


if __name__ == "__main__":
    main()
