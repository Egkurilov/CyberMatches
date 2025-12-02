#!/bin/bash
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
tar -czf cybermatches_deploy.tar.gz     main.py     improved_parser.py     api.py     cyber_telegram_bot.py     requirements.txt     .env     --exclude='*.pyc'     --exclude='__pycache__'     --exclude='.git'     --exclude='.venv'     --exclude='logs'     --exclude='data'     2>/dev/null || true

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
