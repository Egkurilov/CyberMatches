#!/usr/bin/env python3
"""
Тест для анализа HTML структуры турниров
"""

import requests
from bs4 import BeautifulSoup
from improved_parser import URL, HEADERS

def analyze_tournament_structure():
    """Анализируем структуру HTML для турниров"""
    print("🔍 Анализируем структуру турниров в HTML...")
    
    try:
        html = requests.get(URL, headers=HEADERS, timeout=15).text
        soup = BeautifulSoup(html, 'lxml')
        
        # Ищем контейнеры с матчами
        match_containers = soup.find_all('div', class_=['new-match-style', 'match-info'])
        
        print(f"Найдено контейнеров: {len(match_containers)}")
        
        # Анализируем первые 5 контейнеров
        for i, container in enumerate(match_containers[:5]):
            print(f"\n--- Контейнер {i+1} ---")
            
            # Показываем все div элементы
            divs = container.find_all('div')
            print(f"Все div элементы:")
            for j, div in enumerate(divs):
                classes = div.get('class', [])
                text = div.get_text(strip=True)[:100]  # Первые 100 символов
                print(f"  div {j}: class={classes}, text='{text}'")
            
            # Показываем все ссылки
            links = container.find_all('a')
            print(f"Все ссылки:")
            for j, link in enumerate(links):
                href = link.get('href', '')
                text = link.get_text(strip=True)[:50]
                print(f"  a {j}: href='{href}', text='{text}'")
            
            # Показываем весь HTML контейнера
            print(f"HTML контейнера:")
            print(container.prettify()[:500] + "..." if len(container.prettify()) > 500 else container.prettify())
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    analyze_tournament_structure()
