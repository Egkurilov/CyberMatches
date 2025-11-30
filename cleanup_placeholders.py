#!/usr/bin/env python3
"""
Скрипт для очистки БД от плейсхолдеров команд (#5, #7, #10, TBD и т.д.)
"""

import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

def cleanup_placeholders():
    """Удаляет матчи с плейсхолдерами команд из БД"""
    
    print("🧹 Начинаем очистку БД от плейсхолдеров...")
    
    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        ) as conn:
            with conn.cursor() as cur:
                # Показываем текущее количество матчей с плейсхолдерами
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM dota_matches 
                    WHERE team1 LIKE '#%' OR team2 LIKE '#%' 
                       OR team1 = 'TBD' OR team2 = 'TBD'
                """)
                placeholder_count = cur.fetchone()[0]
                print(f"📊 Найдено матчей с плейсхолдерами: {placeholder_count}")
                
                if placeholder_count == 0:
                    print("✅ Плейсхолдеров не найден, БД уже чиста!")
                    return
                
                # Показываем примеры плейсхолдеров
                cur.execute("""
                    SELECT DISTINCT team1, team2, COUNT(*) as count
                    FROM dota_matches 
                    WHERE team1 LIKE '#%' OR team2 LIKE '#%' 
                       OR team1 = 'TBD' OR team2 = 'TBD'
                    GROUP BY team1, team2
                    ORDER BY count DESC
                    LIMIT 10
                """)
                examples = cur.fetchall()
                print("🔍 Примеры плейсхолдеров:")
                for team1, team2, count in examples:
                    print(f"  {team1} vs {team2}: {count} матчей")
                
                # Удаляем матчи с плейсхолдерами
                cur.execute("""
                    DELETE FROM dota_matches 
                    WHERE team1 LIKE '#%' OR team2 LIKE '#%' 
                       OR team1 = 'TBD' OR team2 = 'TBD'
                """)
                deleted_count = cur.rowcount
                conn.commit()
                
                print(f"🗑️ Удалено матчей с плейсхолдерами: {deleted_count}")
                
                # Показываем оставшееся количество матчей
                cur.execute("SELECT COUNT(*) FROM dota_matches")
                remaining_count = cur.fetchone()[0]
                print(f"📈 Осталось матчей в БД: {remaining_count}")
                
                print("✅ Очистка завершена успешно!")
                
    except Exception as e:
        print(f"❌ Ошибка при очистке БД: {e}")
        raise

if __name__ == "__main__":
    cleanup_placeholders()
