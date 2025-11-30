#!/usr/bin/env python3
"""
Тестовый скрипт для проверки парсинга матчей
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

URL = "https://liquipedia.net/dota2/Liquipedia:Matches"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
}

def test_parser():
    print("🧪 Тестируем парсер матчей...")
    
    try:
        html = requests.get(URL, headers=HEADERS, timeout=15).text
        soup = BeautifulSoup(html, 'lxml')
        
        print(f"✅ HTML загружен, размер: {len(html)} символов")
        
        # Ищем контейнеры с матчами
        match_containers = soup.find_all('div', class_=['new-match-style', 'match-info'])
        print(f"📊 Найдено контейнеров нового формата: {len(match_containers)}")
        
        if not match_containers:
            # Пробуем альтернативные селекторы
            match_containers = soup.find_all('div', class_=lambda x: x and 'match' in x.lower() and not any(word in str(x).lower() for word in ['menu', 'nav', 'header', 'footer', 'sidebar', 'rematch']))
            print(f"📊 Найдено контейнеров с match в классе (фильтровано): {len(match_containers)}")
        
        # Показываем первые 3 контейнера для анализа
        for i, container in enumerate(match_containers[:3]):
            print(f"\n=== Контейнер {i+1} ===")
            
            # Ищем время
            time_elem = container.find(['span', 'div'], class_=lambda x: x and 'timer-object' in str(x))
            if not time_elem:
                time_elem = container.find(['span', 'div'], class_=lambda x: x and any(word in str(x).lower() for word in ['time', 'date', 'countdown']))
            
            if time_elem:
                print(f"⏰ Время: {time_elem.get_text(strip=True)}")
            else:
                print("⏰ Время не найдено")
            
            # Ищем команды
            team_elems = container.find_all(['span', 'div'], class_=lambda x: x and 'team' in str(x).lower())
            teams = []
            for team_elem in team_elems:
                team_text = team_elem.get_text(strip=True)
                if team_text and team_text not in teams and len(team_text) > 1:
                    teams.append(team_text)
            
            print(f"👥 Команды: {teams}")
            
            # Ищем ссылку на матч
            match_link = container.find('a', href=lambda x: x and '/dota2/Match:' in x)
            if match_link:
                match_url = urljoin('https://liquipedia.net', match_link.get('href'))
                print(f"🔗 URL матча: {match_url}")
            else:
                print("🔗 URL матча не найден")
            
            # Ищем счет
            score_elem = container.find(['span', 'div'], class_=lambda x: x and 'score' in str(x).lower())
            if score_elem:
                score_text = score_elem.get_text(strip=True)
                print(f"🎯 Счет: {score_text}")
            else:
                print("🎯 Счет не найден")
            
            # Ищем турнир
            tournament_elem = container.find(['span', 'div'], class_=lambda x: x and any(word in str(x).lower() for word in ['tournament', 'league', 'event']))
            if tournament_elem:
                tournament = tournament_elem.get_text(strip=True)
                print(f"🏆 Турнир: {tournament}")
            else:
                print("🏆 Турнир не найден")
        
        print(f"\n✅ Тест завершен. Найдено {len(match_containers)} контейнеров с матчами.")
        
        # Проверяем, есть ли ссылки на матчи
        match_links = soup.find_all('a', href=lambda x: x and '/dota2/Match:' in x)
        print(f"🔗 Всего ссылок на матчи: {len(match_links)}")
        
        if match_links:
            print("Первые 5 ссылок:")
            for i, link in enumerate(match_links[:5]):
                print(f"  {i+1}. {link.get('href')}")
        
        return len(match_containers) > 0
        
    except Exception as e:
        print(f"❌ Ошибка при тестировании: {e}")
        return False

if __name__ == "__main__":
    success = test_parser()
    if success:
        print("\n🎉 Парсер работает и находит матчи!")
    else:
        print("\n⚠️ Парсер не находит матчи, нужна доработка.")
