#!/usr/bin/env python3
"""
Тест для отладки извлечения турниров
"""

import requests
from bs4 import BeautifulSoup
from improved_parser import URL, HEADERS

def debug_tournament_extraction():
    """Отладка извлечения турниров"""
    print("🔍 Отладка извлечения турниров...")
    
    try:
        html = requests.get(URL, headers=HEADERS, timeout=15).text
        soup = BeautifulSoup(html, 'lxml')
        
        # Ищем контейнеры с матчами
        match_containers = soup.find_all('div', class_=['new-match-style', 'match-info'])
        
        print(f"Найдено контейнеров: {len(match_containers)}")
        
        # Анализируем первые 3 контейнера
        for i, container in enumerate(match_containers[:3]):
            print(f"\n--- Контейнер {i+1} ---")
            
            # Проверяем div с классом match-info-tournament
            tournament_div = container.find('div', class_='match-info-tournament')
            if tournament_div:
                print(f"✅ Найден div match-info-tournament")
                print(f"   HTML: {tournament_div}")
                print(f"   Текст: '{tournament_div.get_text(strip=True)}'")
                
                # Проверяем ссылку внутри
                tournament_link = tournament_div.find('a')
                if tournament_link:
                    print(f"   ✅ Найдена ссылка: href='{tournament_link.get('href')}'")
                    print(f"   ✅ Текст ссылки: '{tournament_link.get_text(strip=True)}'")
                else:
                    print(f"   ❌ Ссылка не найдена")
            else:
                print(f"❌ div match-info-tournament не найден")
                # Показываем все div элементы
                all_divs = container.find_all('div')
                print(f"   Все div классы: {[div.get('class', []) for div in all_divs]}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    debug_tournament_extraction()
