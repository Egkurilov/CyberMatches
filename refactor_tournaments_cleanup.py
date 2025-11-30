#!/usr/bin/env python3
"""
Рефакторинг турниров и очистка таблицы matches:
1. Очистка названий турниров от лишних суффиксов
2. Удаление ненужных колонок (source_url, match_uid)
3. Обновление связей с таблицей tournaments
"""

from __future__ import annotations

import os
import re
from typing import Dict, List, Tuple

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


def clean_tournament_name(tournament_name: str) -> str:
    """
    Очистка названия турнира от лишних суффиксов:
    - "BB Streamers Battle 12 - Playoffs" -> "BB Streamers Battle 12"
    - "BLAST Slam V - November 29-A" -> "BLAST Slam V"
    - "CCT S2 Series 6 - Group B" -> "CCT S2 Series 6"
    """
    if not tournament_name:
        return tournament_name
    
    # Удаляем суффиксы вида " - Playoffs", " - November 29-A", " - Group B" и т.д.
    # Оставляем только основное название турнира
    cleaned = re.split(r'\s*-\s*(?:Playoffs|Group\s+[A-Z]|November\s+\d+-[A-Z]|Play-In)', tournament_name, 1)[0]
    
    # Удаляем лишние пробелы в начале и конце
    cleaned = cleaned.strip()
    
    return cleaned


def get_tournament_mappings(conn: psycopg.Connection) -> Dict[str, List[Tuple[int, str]]]:
    """Получаем маппинг старых названий турниров к их ID и количеству матчей"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tournament, COUNT(*) as count, array_agg(id) as match_ids
            FROM dota_matches 
            WHERE tournament IS NOT NULL 
            GROUP BY tournament
            ORDER BY count DESC;
        """)
        
        mappings = {}
        for tournament, count, match_ids in cur.fetchall():
            mappings[tournament] = (count, match_ids)
            
    return mappings


def update_tournament_names(conn: psycopg.Connection) -> Dict[str, str]:
    """Обновление названий турниров в таблице matches"""
    print("🧹 Очистка названий турниров...")
    
    mappings = get_tournament_mappings(conn)
    updated_mappings = {}
    
    with conn.cursor() as cur:
        for old_name, (count, match_ids) in mappings.items():
            new_name = clean_tournament_name(old_name)
            
            if new_name != old_name:
                print(f"  • '{old_name}' -> '{new_name}' ({count} матчей)")
                
                # Обновляем названия турниров в матчах
                cur.execute("""
                    UPDATE dota_matches
                    SET tournament = %s
                    WHERE id = ANY(%s);
                """, (new_name, match_ids))
                
                updated_mappings[old_name] = new_name
    
    conn.commit()
    print(f"✅ Обновлено {len(updated_mappings)} названий турниров")
    return updated_mappings


def update_tournaments_table(conn: psycopg.Connection, name_mappings: Dict[str, str]):
    """Обновление таблицы tournaments с новыми названиями"""
    print("🔄 Обновление таблицы tournaments...")
    
    with conn.cursor() as cur:
        for old_name, new_name in name_mappings.items():
            # Проверяем, существует ли турнир с новым названием
            cur.execute("""
                SELECT id FROM tournaments WHERE name = %s;
            """, (new_name,))
            
            existing = cur.fetchone()
            
            if existing:
                # Обновляем существующие матчи, чтобы они ссылались на правильный турнир
                new_tournament_id = existing[0]
                
                # Сначала обновляем tournament_id для всех матчей со старым названием
                cur.execute("""
                    UPDATE dota_matches
                    SET tournament_id = %s
                    WHERE tournament_id IN (
                        SELECT id FROM tournaments WHERE name = %s
                    );
                """, (new_tournament_id, old_name))
                
                # Теперь можно безопасно удалить дубликат турнира со старым названием
                cur.execute("""
                    DELETE FROM tournaments WHERE name = %s;
                """, (old_name,))
                
                print(f"  • Объединен турнир '{old_name}' в '{new_name}'")
            else:
                # Просто обновляем название
                cur.execute("""
                    UPDATE tournaments
                    SET name = %s, updated_at = NOW()
                    WHERE name = %s;
                """, (new_name, old_name))
                
                print(f"  • Переименован турнир '{old_name}' в '{new_name}'")
    
    conn.commit()
    print("✅ Таблица tournaments обновлена")


def remove_unused_columns(conn: psycopg.Connection):
    """Удаление ненужных колонок из таблицы dota_matches"""
    print("🗑️ Удаление ненужных колонок...")
    
    with conn.cursor() as cur:
        # Проверяем существование колонок перед удалением
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'dota_matches' 
            AND table_schema = 'public'
            AND column_name IN ('source_url', 'match_uid');
        """)
        
        existing_columns = [row[0] for row in cur.fetchall()]
        
        if 'source_url' in existing_columns:
            cur.execute("ALTER TABLE dota_matches DROP COLUMN IF EXISTS source_url;")
            print("  • Удалена колонка source_url")
        
        if 'match_uid' in existing_columns:
            cur.execute("ALTER TABLE dota_matches DROP COLUMN IF EXISTS match_uid;")
            print("  • Удалена колонка match_uid")
    
    conn.commit()
    print("✅ Ненужные колонки удалены")


def create_refactored_indexes(conn: psycopg.Connection):
    """Создание оптимизированных индексов для новой схемы"""
    print("📊 Создание оптимизированных индексов...")
    
    with conn.cursor() as cur:
        # Индекс для быстрого поиска по времени
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dota_matches_time_msk 
            ON dota_matches(match_time_msk);
        """)
        
        # Индекс для поиска по статусу
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dota_matches_status 
            ON dota_matches(status);
        """)
        
        # Индекс для поиска по tournament_id
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dota_matches_tournament_id 
            ON dota_matches(tournament_id);
        """)
        
        # Композитный индекс для частых запросов
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dota_matches_time_status 
            ON dota_matches(match_time_msk, status);
        """)
    
    conn.commit()
    print("✅ Индексы созданы")


def generate_refactoring_report(conn: psycopg.Connection):
    """Генерация отчета о рефакторинге"""
    print("\n📋 Отчет о рефакторинге:")
    
    with conn.cursor() as cur:
        # Количество уникальных турниров после очистки
        cur.execute("""
            SELECT COUNT(DISTINCT tournament) 
            FROM dota_matches 
            WHERE tournament IS NOT NULL;
        """)
        unique_tournaments = cur.fetchone()[0]
        
        # Примеры очищенных турниров
        cur.execute("""
            SELECT DISTINCT tournament, COUNT(*) as match_count
            FROM dota_matches 
            WHERE tournament IS NOT NULL
            GROUP BY tournament
            ORDER BY match_count DESC
            LIMIT 10;
        """)
        
        print(f"✅ Уникальных турниров после очистки: {unique_tournaments}")
        print("\n🔍 Топ-10 турниров после очистки:")
        for tournament, count in cur.fetchall():
            print(f"  • {tournament}: {count} матчей")
        
        # Проверка структуры таблицы
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'dota_matches' 
            AND table_schema = 'public'
            ORDER BY ordinal_position;
        """)
        
        print("\n📊 Структура таблицы dota_matches:")
        for column_name, data_type in cur.fetchall():
            print(f"  • {column_name}: {data_type}")


def main():
    """Основная функция рефакторинга"""
    print("🚀 Начало рефакторинга турниров и БД...")
    
    try:
        with get_db_connection() as conn:
            # 1. Очищаем названия турниров
            name_mappings = update_tournament_names(conn)
            
            # 2. Обновляем таблицу tournaments
            if name_mappings:
                update_tournaments_table(conn, name_mappings)
            
            # 3. Удаляем ненужные колонки
            remove_unused_columns(conn)
            
            # 4. Создаем оптимизированные индексы
            create_refactored_indexes(conn)
            
            # 5. Генерируем отчет
            generate_refactoring_report(conn)
            
            print("\n✅ Рефакторинг успешно завершен!")
            
    except Exception as e:
        print(f"\n❌ Ошибка при рефакторинге: {e}")
        raise


if __name__ == "__main__":
    main()
