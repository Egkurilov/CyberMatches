#!/usr/bin/env python3
"""
Тестовый скрипт для отладки парсера и анализа проблемы с сохранением матчей
"""

import requests
from bs4 import BeautifulSoup
from improved_parser import parse_matches_from_html, URL, HEADERS

def analyze_matches():
    """Анализируем что именно парсится и почему только 2 матча сохраняются"""
    print("🧪 Анализируем парсинг матчей...")
    
    try:
        html = requests.get(URL, headers=HEADERS, timeout=15).text
        matches = parse_matches_from_html(html)
        
        print(f"\n📊 Общая статистика:")
        print(f"Всего матчей найдено: {len(matches)}")
        
        # Анализируем по статусам
        status_counts = {"upcoming": 0, "live": 0, "finished": 0, "unknown": 0}
        matches_with_scores = []
        matches_with_urls = []
        matches_with_tournaments = []
        
        for match in matches:
            status = match.get('status', 'unknown')
            if status in status_counts:
                status_counts[status] += 1
            else:
                status_counts['unknown'] += 1
                
            if match.get('score'):
                matches_with_scores.append(match)
            if match.get('match_url'):
                matches_with_urls.append(match)
            if match.get('tournament'):
                matches_with_tournaments.append(match)
        
        print(f"Статистика по статусам:")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
            
        print(f"\nМатчи со счетом: {len(matches_with_scores)}")
        print(f"Матчи с URL: {len(matches_with_urls)}")
        print(f"Матчи с турниром: {len(matches_with_tournaments)}")
        
        # Показываем первые 10 матчей со счетом
        print(f"\n🏆 Первые 10 матчей со счетом:")
        for i, match in enumerate(matches_with_scores[:10]):
            print(f"  {i+1}. {match['time_raw']}: {match['team1']} vs {match['team2']} - {match['score']} ({match['bo']})")
            print(f"      Статус: {match['status']}, URL: {match['match_url']}, Турнир: {match['tournament']}")
        
        # Показываем первые 10 предстоящих матчей
        upcoming_matches = [m for m in matches if m.get('status') == 'upcoming']
        print(f"\n📅 Первые 10 предстоящих матчей:")
        for i, match in enumerate(upcoming_matches[:10]):
            print(f"  {i+1}. {match['time_raw']}: {match['team1']} vs {match['team2']}")
            print(f"      Статус: {match['status']}, URL: {match['match_url']}, Турнир: {match['tournament']}")
        
        # Анализируем проблему с сохранением
        print(f"\n🔍 Анализ проблемы сохранения:")
        
        # Проверяем матчи без времени
        matches_without_time = [m for m in matches if not m.get('time_msk')]
        print(f"Матчи без времени MSK: {len(matches_without_time)}")
        
        # Проверяем дубликаты
        match_identifiers = []
        for match in matches:
            identifier = f"{match.get('time_raw')}|{match.get('team1')}|{match.get('team2')}"
            match_identifiers.append(identifier)
        
        unique_identifiers = set(match_identifiers)
        print(f"Уникальных идентификаторов: {len(unique_identifiers)} из {len(match_identifiers)}")
        
        if len(unique_identifiers) < len(match_identifiers):
            print("⚠️ Найдены дубликаты!")
        
        return matches
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return []

if __name__ == "__main__":
    matches = analyze_matches()
    print(f"\n✅ Анализ завершен. Найдено {len(matches)} матчей.")
