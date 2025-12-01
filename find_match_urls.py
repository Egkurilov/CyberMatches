#!/usr/bin/env python3
"""
Скрипт для поиска URL матчей по командам и времени
"""

import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv
import psycopg

load_dotenv()

URL = "https://liquipedia.net/dota2/Liquipedia:Matches"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
}

def find_match_url_by_teams(team1, team2):
    """
    Ищет URL матча по названиям команд на главной странице Liquipedia
    """
    try:
        html = requests.get(URL, headers=HEADERS, timeout=15).text
        soup = BeautifulSoup(html, 'lxml')
        
        # Ищем все ссылки на матчи
        match_links = soup.find_all('a', href=lambda x: x and '/dota2/Match:' in x)
        
        print(f"[DEBUG] Ищем матч {team1} vs {team2}")
        print(f"[DEBUG] Найдено ссылок на матчи: {len(match_links)}")
        
        for link in match_links:
            href = link.get('href', '')
            full_url = urljoin('https://liquipedia.net', href)
            
            # Получаем текст вокруг ссылки
            parent = link.parent
            if parent:
                # Ищем в родительском элементе названия команд
                parent_text = parent.get_text(strip=True).lower()
                
                if (team1.lower() in parent_text and team2.lower() in parent_text) or \
                   (team2.lower() in parent_text and team1.lower() in parent_text):
                    print(f"[DEBUG] Найден URL: {full_url}")
                    return full_url
            
            # Проверяем саму ссылку
            link_text = link.get_text(strip=True).lower()
            if (team1.lower() in link_text and team2.lower() in link_text) or \
               (team2.lower() in link_text and team1.lower() in link_text):
                print(f"[DEBUG] Найден URL: {full_url}")
                return full_url
        
        print(f"[DEBUG] URL не найден для {team1} vs {team2}")
        return None
        
    except Exception as e:
        print(f"[DEBUG] Ошибка при поиске URL: {e}")
        return None

def update_missing_match_urls():
    """
    Обновляет URL для матчей без liquipedia_match_id
    """
    conn = psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )
    
    try:
        with conn.cursor() as cur:
            # Получаем матчи без liquipedia_match_id
            cur.execute('''
                SELECT id, team1, team2, tournament 
                FROM dota_matches 
                WHERE liquipedia_match_id IS NULL 
                AND score IS NULL
                ORDER BY match_time_msk DESC
                LIMIT 10
            ''')
            matches = cur.fetchall()
            
            print(f"[INFO] Найдено матчей для обновления: {len(matches)}")
            
            updated_count = 0
            
            for match_id, team1, team2, tournament in matches:
                print(f"\n[INFO] Обрабатываем: {team1} vs {team2} ({tournament})")
                
                # Ищем URL
                match_url = find_match_url_by_teams(team1, team2)
                
                if match_url:
                    # Извлекаем ID из URL
                    liquipedia_match_id = match_url.split('/')[-1]
                    
                    # Обновляем запись
                    cur.execute('''
                        UPDATE dota_matches 
                        SET match_url = %s, liquipedia_match_id = %s, updated_at = NOW()
                        WHERE id = %s
                    ''', (match_url, liquipedia_match_id, match_id))
                    
                    updated_count += 1
                    print(f"[SUCCESS] Обновлено: {match_url}")
                else:
                    print(f"[WARNING] URL не найден")
                
                # Небольшая задержка, чтобы не перегружать сервер
                import time
                time.sleep(1)
            
            conn.commit()
            print(f"\n[INFO] Обновлено матчей: {updated_count}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    update_missing_match_urls()
