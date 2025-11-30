#!/usr/bin/env python3
"""
Тестирование функции очистки названий турниров
"""

from main import clean_tournament_name

def test_tournament_cleanup():
    """Тестирование функции очистки турниров"""
    
    test_cases = [
        # Исходное название -> Ожидаемый результат
        ("BB Streamers Battle 12 - Playoffs", "BB Streamers Battle 12"),
        ("BLAST Slam V - November 29-A", "BLAST Slam V"),
        ("CCT S2 Series 6 - Group B", "CCT S2 Series 6"),
        ("PGL Wallachia S6 - Playoffs", "PGL Wallachia S6"),
        ("BLAST Slam V - Play-In", "BLAST Slam V"),
        ("Jr. CCT S2 Series 6 - Group B", "Jr. CCT S2 Series 6"),
        ("BLAST Slam V - November 28-B", "BLAST Slam V"),
        ("BLAST Slam V", "BLAST Slam V"),  # Без изменений
        ("DreamLeague S25", "DreamLeague S25"),  # Без изменений
        ("", ""),  # Пустая строка
        ("Tournament Name - Some Other Stuff", "Tournament Name"),  # Общий случай
    ]
    
    print("🧪 Тестирование функции очистки турниров:")
    print("=" * 60)
    
    all_passed = True
    
    for original, expected in test_cases:
        result = clean_tournament_name(original)
        status = "✅" if result == expected else "❌"
        
        if result != expected:
            all_passed = False
            
        print(f"{status} '{original}' -> '{result}' (ожидалось: '{expected}')")
    
    print("=" * 60)
    if all_passed:
        print("✅ Все тесты пройдены успешно!")
    else:
        print("❌ Некоторые тесты не пройдены!")
    
    return all_passed

if __name__ == "__main__":
    test_tournament_cleanup()
