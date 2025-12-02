#!/usr/bin/env python3
"""
Скрипт для очистки проекта от лишних файлов и обновления механизма доставки на сервер
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

def run_command(cmd, check=True):
    """Выполнить команду и вернуть результат"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    except subprocess.CalledProcessError as e:
        return "", e.stderr.strip(), e.returncode

def analyze_project():
    """Анализ текущего состояния проекта"""
    print("🔍 АНАЛИЗ ТЕКУЩЕГО СОСТОЯНИЯ ПРОЕКТА")
    print("=" * 60)
    
    # Подсчет файлов
    all_files = list(Path('.').glob('*.py'))
    sh_files = list(Path('.').glob('*.sh'))
    tar_files = list(Path('.').glob('*.tar.gz'))
    
    print(f"📊 Python файлов: {len(all_files)}")
    print(f"📊 Shell скриптов: {len(sh_files)}")
    print(f"📊 Tar архивов: {len(tar_files)}")
    
    # Проверка main.py
    if Path('main.py').exists():
        main_size = Path('main.py').stat().st_size
        print(f"📋 main.py размер: {main_size} байт")
    
    # Проверка дубликатов main
    main_duplicates = []
    for pattern in ['main_*.py', 'main-*.py']:
        main_duplicates.extend(Path('.').glob(pattern))
    
    print(f"⚠️ Найдено дубликатов main.py: {len(main_duplicates)}")
    for dup in main_duplicates:
        print(f"   - {dup.name}")
    
    return len(main_duplicates)

def cleanup_files():
    """Удаление лишних файлов"""
    print("\n🧹 ОЧИСТКА ФАЙЛОВ")
    print("=" * 60)
    
    # Файлы для удаления
    files_to_remove = [
        # Дубликаты main.py
        'main_fixed.py',
        'main_final.py', 
        'main_final_fixed.py',
        
        # Диагностические и тестовые файлы
        'diagnostic_check.py',
        'comprehensive_diagnostic.py',
        'test_debug.py',
        'test_html_structure.py',
        'test_parser.py',
        'test_tournament_debug.py',
        'test_tournament_cleanup.py',
        
        # Файлы исправлений (все объединены в main.py)
        'fix_on_conflict.py',
        'fix_database_constraints.py',
        'fix_tournaments.py',
        'emergency_fix.py',
        'final_complete_fix.py',
        'final_solution.py',
        'final_transaction_fix.py',
        'emergency_reset.py',
        
        # Миграционные файлы (выполнены)
        'migrate_to_new_schema.py',
        'migrate_database_schema.py',
        'refactor_tournaments_cleanup.py',
        
        # Служебные файлы
        'cleanup_duplicates.py',
        'cleanup_placeholders.py',
        'find_match_urls.py',
        'update_scores.py',
        'today_matches.py',
        
        # Старые скрипты деплоя (объединим в новый)
        'deploy_solution.sh',
        'final_fix.sh',
        'final_server_fix.py',
        'restart_service.sh',
        'update_server_code.sh',
        'update_systemd_services.sh',
    ]
    
    removed_count = 0
    for file_path in files_to_remove:
        if Path(file_path).exists():
            try:
                if file_path.endswith('.py'):
                    os.remove(file_path)
                else:
                    os.remove(file_path)
                print(f"  ✅ Удален: {file_path}")
                removed_count += 1
            except Exception as e:
                print(f"  ❌ Ошибка удаления {file_path}: {e}")
    
    print(f"📊 Удалено файлов: {removed_count}")
    return removed_count

def create_new_deploy_script():
    """Создание нового скрипта деплоя"""
    print("\n🚀 СОЗДАНИЕ НОВОГО СКРИПТА ДЕПЛОЯ")
    print("=" * 60)
    
    deploy_script = """#!/bin/bash
# Новый скрипт деплоя CyberMatches

set -e  # Выход при ошибке

echo "🚀 НАЧИНАЕМ ДЕПЛОЙ CyberMatches"
echo "======================================"

# Проверка переменных окружения
if [ -z "$SERVER_USER" ] || [ -z "$SERVER_HOST" ] || [ -z "$SERVER_PATH" ]; then
    echo "❌ Ошибка: Установите переменные окружения:"
    echo "   SERVER_USER - пользователь сервера"
    echo "   SERVER_HOST - хост сервера" 
    echo "   SERVER_PATH - путь на сервере"
    exit 1
fi

echo "📋 Параметры деплоя:"
echo "   Сервер: $SERVER_USER@$SERVER_HOST"
echo "   Путь: $SERVER_PATH"

# Создание архива с необходимыми файлами
echo "📦 Создание архива..."
tar -czf cybermatches_deploy.tar.gz \
    main.py \
    improved_parser.py \
    api.py \
    cyber_telegram_bot.py \
    requirements.txt \
    .env \
    --exclude='*.pyc' \
    --exclude='__pycache__' \
    --exclude='.git' \
    --exclude='.venv' \
    --exclude='logs' \
    --exclude='data' \
    2>/dev/null || true

# Копирование на сервер
echo "📤 Копирование файлов на сервер..."
scp cybermatches_deploy.tar.gz $SERVER_USER@$SERVER_HOST:/tmp/

# Развертывание на сервере
echo "🔧 Развертывание на сервере..."
ssh $SERVER_USER@$SERVER_HOST << 'ENDSSH'
    set -e
    
    echo "   Остановка сервисов..."
    sudo systemctl stop cybermatches || true
    sudo systemctl stop cybermatches-bot || true
    
    echo "   Резервное копирование..."
    cd /root/cybermatches
    cp -r . /root/cybermatches_backup_$(date +%Y%m%d_%H%M%S) || true
    
    echo "   Распаковка новых файлов..."
    rm -rf /tmp/cybermatches_new
    mkdir -p /tmp/cybermatches_new
    cd /tmp/cybermatches_new
    tar -xzf /tmp/cybermatches_deploy.tar.gz
    
    echo "   Обновление кода..."
    rsync -av --delete /tmp/cybermatches_new/ /root/cybermatches/
    
    echo "   Установка зависимостей..."
    cd /root/cybermatches
    source .venv/bin/activate
    pip install -r requirements.txt
    
    echo "   Запуск сервисов..."
    sudo systemctl start cybermatches
    sudo systemctl start cybermatches-bot
    
    echo "   Проверка статуса..."
    sleep 5
    sudo systemctl status cybermatches --no-pager
    sudo systemctl status cybermatches-bot --no-pager
ENDSSH

# Удаление временных файлов
rm -f cybermatches_deploy.tar.gz

echo "✅ ДЕПЛОЙ ЗАВЕРШЕН УСПЕШНО!"
echo "======================================"
"""

    with open('deploy_new.sh', 'w') as f:
        f.write(deploy_script)
    
    # Делаем исполняемым
    os.chmod('deploy_new.sh', 0o755)
    print("  ✅ Создан новый скрипт деплоя: deploy_new.sh")
    
    return True

def create_deployment_package():
    """Создание финального пакета для деплоя"""
    print("\n📦 СОЗДАНИЕ ФИНАЛЬНОГО ПАКЕТА")
    print("=" * 60)
    
    # Создаем архив только с необходимыми файлами
    files_to_package = [
        'main.py',
        'improved_parser.py', 
        'api.py',
        'cyber_telegram_bot.py',
        'requirements.txt',
        '.env',
        'setup_services.sh',
        'systemd_services.tar.gz'
    ]
    
    # Проверяем наличие файлов
    missing_files = []
    for file in files_to_package:
        if not Path(file).exists():
            missing_files.append(file)
    
    if missing_files:
        print(f"⚠️ Отсутствуют файлы: {missing_files}")
        return False
    
    # Создаем архив
    try:
        import tarfile
        with tarfile.open('cybermatches_final.tar.gz', 'w:gz') as tar:
            for file in files_to_package:
                tar.add(file)
        print("  ✅ Создан финальный архив: cybermatches_final.tar.gz")
        
        # Показываем размер
        size = Path('cybermatches_final.tar.gz').stat().st_size
        print(f"  📊 Размер архива: {size} байт ({size/1024:.1f} КБ)")
        
        return True
    except Exception as e:
        print(f"  ❌ Ошибка создания архива: {e}")
        return False

def verify_main_py():
    """Проверка, что main.py содержит актуальный код"""
    print("\n🔍 ПРОВЕРКА main.py")
    print("=" * 60)
    
    if not Path('main.py').exists():
        print("  ❌ main.py не найден!")
        return False
    
    with open('main.py', 'r') as f:
        content = f.read()
    
    # Проверяем ключевые признаки актуального кода
    checks = [
        ("Обработка дубликатов", "UniqueViolation"),
        ("Улучшенный парсер", "improved_parser"),
        ("Очистка турниров", "clean_tournament_name"),
        ("Новая система идентификации", "liquipedia_match_id"),
    ]
    
    all_good = True
    for check_name, check_text in checks:
        if check_text in content:
            print(f"  ✅ {check_name}: найдено")
        else:
            print(f"  ❌ {check_name}: не найдено")
            all_good = False
    
    # Проверяем размер
    size = len(content)
    if size > 10000:  # Должен быть большим файлом
        print(f"  ✅ Размер файла: {size} байт (нормально)")
    else:
        print(f"  ⚠️ Размер файла: {size} байт (маловато)")
    
    return all_good

def main():
    """Главная функция очистки и подготовки"""
    print("🧹 КОМПЛЕКСНАЯ ОЧИСТКА И ПОДГОТОВКА ПРОЕКТА")
    print("=" * 70)
    
    # 1. Анализ
    duplicates = analyze_project()
    
    # 2. Очистка
    removed = cleanup_files()
    
    # 3. Проверка main.py
    main_ok = verify_main_py()
    
    # 4. Создание нового деплоя
    deploy_ok = create_new_deploy_script()
    
    # 5. Создание финального пакета
    package_ok = create_deployment_package()
    
    # Итог
    print("\n" + "=" * 70)
    print("📋 ИТОГИ ОЧИСТКИ:")
    print(f"  Удалено дубликатов main.py: {duplicates}")
    print(f"  Удалено файлов: {removed}")
    print(f"  Проверка main.py: {'✅ OK' if main_ok else '❌ Ошибка'}")
    print(f"  Новый скрипт деплоя: {'✅ OK' if deploy_ok else '❌ Ошибка'}")
    print(f"  Финальный пакет: {'✅ OK' if package_ok else '❌ Ошибка'}")
    
    if all([main_ok, deploy_ok, package_ok]):
        print("\n🎉 ПРОЕКТ УСПЕШНО ОЧИЩЕН И ГОТОВ К ДЕПЛОЮ!")
        print("   Используйте: ./deploy_new.sh для деплоя на сервер")
        print("   Или распакуйте cybermatches_final.tar.gz вручную")
    else:
        print("\n⚠️ Есть проблемы, которые нужно исправить")
    
    return all([main_ok, deploy_ok, package_ok])

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
