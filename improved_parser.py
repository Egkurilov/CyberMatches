#!/usr/bin/env python3
"""
Улучшенный парсер для извлечения счета из завершенных матчей Liquipedia
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
from datetime import datetime, timedelta, timezone

URL = "https://liquipedia.net/dota2/Liquipedia:Matches"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
}

def parse_time_to_msk(time_str: str) -> datetime | None:
    """Парсим строки вида 'November 30, 2025 - 17:15 MSK' в datetime с tzinfo=MSK."""
    try:
        m = re.match(
            r"([A-Z][a-z]+ \d{1,2}, \d{4}) - (\d{1,2}:\d{2}) ([A-Z]+)",
            time_str.strip(),
        )
        if not m:
            return None

        date_part = m.group(1)
        time_part = m.group(2)
        tz_abbr = m.group(3)

        dt_naive = datetime.strptime(f"{date_part} {time_part}", "%B %d, %Y %H:%M")
        
        # Таблица часовых поясов
        tz_offsets = {
            "UTC": 0, "GMT": 0, "CET": 1, "CEST": 2, "EET": 2, "EEST": 3,
            "MSK": 3, "SGT": 8, "PST": -8, "PDT": -7, "EST": -5, "EDT": -4,
        }
        
        offset_hours = tz_offsets.get(tz_abbr, 0)
        src_tz = timezone(timedelta(hours=offset_hours))
        src_dt = dt_naive.replace(tzinfo=src_tz)
        msk_tz = timezone(timedelta(hours=3))
        return src_dt.astimezone(msk_tz)
    except Exception as e:
        print(f"Ошибка парсинга времени: {e}")
        return None

def extract_score_from_container(container):
    """
    Извлекает счет из контейнера матча
    """
    # Ищем wrapper со счетом
    score_wrapper = container.find('span', class_='match-info-header-scoreholder-scorewrapper')
    if not score_wrapper:
        return None, None
    
    # Ищем счета
    scores = score_wrapper.find_all('span', class_='match-info-header-scoreholder-score')
    if len(scores) < 2:
        return None, None
    
    score1 = scores[0].get_text(strip=True)
    score2 = scores[1].get_text(strip=True)
    
    # Ищем формат Bo
    bo_lower = score_wrapper.find('span', class_='match-info-header-scoreholder-lower')
    bo = bo_lower.get_text(strip=True) if bo_lower else None
    
    return f"{score1}:{score2}", bo

def extract_teams_from_container(container):
    """
    Извлекает названия команд из контейнера
    """
    team_containers = container.find_all('div', class_='match-info-header-opponent')
    if len(team_containers) < 2:
        return None, None
    
    team1_elem = team_containers[0].find('span', class_='name')
    team2_elem = team_containers[1].find('span', class_='name')
    
    if not team1_elem or not team2_elem:
        return None, None
    
    team1 = team1_elem.get_text(strip=True)
    team2 = team2_elem.get_text(strip=True)
    
    return team1, team2

def extract_time_from_container(container):
    """
    Извлекает время матча из контейнера
    """
    # Ищем в нескольких возможных местах
    time_elem = container.find('span', class_='timer-object-date')
    if not time_elem:
        time_elem = container.find('span', class_='timer-object')
    
    if not time_elem:
        return None
    
    return time_elem.get_text(strip=True)

def extract_tournament_from_container(container):
    """
    Извлекает название турнира из контейнера
    """
    tournament_elem = container.find('div', class_='match-info-tournament')
    if not tournament_elem:
        return None
    
    tournament_link = tournament_elem.find('a')
    if tournament_link:
        return tournament_link.get_text(strip=True)
    
    return tournament_elem.get_text(strip=True)

def extract_match_url_from_container(container):
    """
    Извлекает URL матча из контейнера
    """
    match_link = container.find('a', href=lambda x: x and '/dota2/Match:' in x)
    if not match_link:
        return None
    
    href = match_link.get('href', '')
    return urljoin('https://liquipedia.net', href)

def is_match_finished(container):
    """
    Проверяет, завершен ли матч по наличию winner/loser классов
    """
    return bool(container.find(['div', 'span'], class_=['match-info-header-winner', 'match-info-header-loser']))

def parse_matches_from_html(html: str) -> list[dict]:
    """
    Основная функция парсинга матчей из HTML
    """
    soup = BeautifulSoup(html, 'lxml')
    
    # Ищем контейнеры с матчами
    match_containers = soup.find_all('div', class_=['new-match-style', 'match-info'])
    
    print(f"[INFO] Найдено контейнеров с матчами: {len(match_containers)}")
    
    matches = []
    
    for i, container in enumerate(match_containers):
        try:
            print(f"[DEBUG] Обрабатываем контейнер {i+1}")
            
            # Ищем время
            time_text = extract_time_from_container(container)
            print(f"[DEBUG] Время: {time_text}")
            if not time_text:
                continue
            
            # Ищем команды
            team1, team2 = extract_teams_from_container(container)
            print(f"[DEBUG] Команды: {team1} vs {team2}")
            if not team1 or not team2:
                continue
            
            # Ищем счет и формат Bo
            score, bo = extract_score_from_container(container)
            print(f"[DEBUG] Счет: {score}, Bo: {bo}")
            
            # Ищем турнир
            tournament = extract_tournament_from_container(container)
            print(f"[DEBUG] Турнир: {tournament}")
            
            # Ищем URL
            match_url = extract_match_url_from_container(container)
            print(f"[DEBUG] URL: {match_url}")
            
            # Определяем статус
            if score:
                status = "finished"
            elif is_match_finished(container):
                status = "finished"
            else:
                # Определяем по времени
                time_msk = parse_time_to_msk(time_text)
                if time_msk:
                    now_msk = datetime.now(timezone(timedelta(hours=3)))
                    if now_msk > time_msk + timedelta(hours=4):
                        status = "finished"
                    elif now_msk > time_msk - timedelta(minutes=5):
                        status = "live"
                    else:
                        status = "upcoming"
                else:
                    status = "unknown"
            
            # Создаем объект матча
            match = {
                'time_raw': time_text,
                'time_msk': parse_time_to_msk(time_text),
                'team1': team1,
                'team2': team2,
                'score': score,
                'bo': bo,
                'tournament': tournament,
                'status': status,
                'match_url': match_url
            }
            
            matches.append(match)
            
            if score:
                print(f"✅ Найден завершенный матч: {team1} vs {team2} - {score}")
            
        except Exception as e:
            print(f"[ERROR] Ошибка при парсинге контейнера {i}: {e}")
            continue
    
    return matches

def test_parser():
    """Тестовая функция"""
    print("🧪 Тестируем улучшенный парсер...")
    
    try:
        html = requests.get(URL, headers=HEADERS, timeout=15).text
        matches = parse_matches_from_html(html)
        
        print(f"\n📊 Результаты парсинга:")
        print(f"Всего матчей: {len(matches)}")
        
        finished_matches = [m for m in matches if m['status'] == 'finished']
        matches_with_score = [m for m in matches if m['score']]
        
        print(f"Завершенных матчей: {len(finished_matches)}")
        print(f"Матчей со счетом: {len(matches_with_score)}")
        
        # Показываем первые 5 матчей со счетом
        if matches_with_score:
            print("\n🏆 Первые матчи со счетом:")
            for match in matches_with_score[:5]:
                print(f"  {match['time_raw']}: {match['team1']} vs {match['team2']} - {match['score']}")
        
        return matches
    
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return []

if __name__ == "__main__":
    matches = test_parser()
    print(f"\n✅ Парсинг завершен. Найдено {len(matches)} матчей.")
