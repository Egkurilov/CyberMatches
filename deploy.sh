#!/bin/bash
set -e

# 🚀 Скрипт ручного деплоя CyberMatches на сервер 45.10.245.84

# Конфигурация
SERVER_HOST="45.10.245.84"
SERVER_USER="root"
REMOTE_DIR="/root/cybermatches"
BACKUP_DIR="/root/cybermatches-backup-$(date +%Y%m%d-%H%M%S)"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функции для вывода
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Проверка зависимостей
check_dependencies() {
    log_info "Проверка зависимостей..."
    
    if ! command -v ssh &> /dev/null; then
        log_error "SSH не установлен"
        exit 1
    fi
    
    if ! command -v scp &> /dev/null; then
        log_error "SCP не установлен"
        exit 1
    fi
    
    if ! command -v tar &> /dev/null; then
        log_error "TAR не установлен"
        exit 1
    fi
    
    log_info "Все зависимости установлены"
}

# Создание деплоймент пакета
create_deployment_package() {
    log_info "Создание деплоймент пакета..."
    
    # Проверка существования requirements.txt
    if [ ! -f "requirements.txt" ]; then
        log_error "requirements.txt не найден"
        exit 1
    fi
    
    # Создание архива
    tar -czf cybermatches-deploy.tar.gz \
      --exclude='.git' \
      --exclude='__pycache__' \
      --exclude='*.pyc' \
      --exclude='.env' \
      --exclude='logs/*.log' \
      --exclude='.github' \
      --exclude='test_*.py' \
      --exclude='migrate_*.py' \
      --exclude='refactor_*.py' \
      --exclude='deploy.sh' \
      --exclude='DEPLOYMENT_SETUP.md' \
      .
    
    if [ $? -eq 0 ]; then
        log_info "Деплоймент пакет создан успешно"
    else
        log_error "Ошибка при создании деплоймент пакета"
        exit 1
    fi
}

# Копирование на сервер
copy_to_server() {
    log_info "Копирование на сервер ${SERVER_HOST}..."
    
    scp cybermatches-deploy.tar.gz ${SERVER_USER}@${SERVER_HOST}:/tmp/
    
    if [ $? -eq 0 ]; then
        log_info "Файлы скопированы успешно"
    else
        log_error "Ошибка при копировании файлов на сервер"
        exit 1
    fi
}

# Деплой на сервере
deploy_on_server() {
    log_info "Деплой на сервере..."
    
    ssh ${SERVER_USER}@${SERVER_HOST} << 'ENDSSH'
    set -e
    
    echo "⏹️ Остановка сервисов..."
    sudo systemctl stop cybermatches.service || true
    sudo systemctl stop cybermatches-api.service || true
    sudo systemctl stop cyber_telegram_bot.service || true
    
    echo "💾 Создание бэкапа..."
    if [ -d "/root/cybermatches" ]; then
        sudo mv /root/cybermatches /root/cybermatches-backup-$(date +%Y%m%d-%H%M%S)
        echo "✅ Бэкап создан"
    else
        echo "ℹ️ Нет существующей установки для бэкапа"
    fi
    
    echo "📦 Извлечение нового деплоя..."
    sudo rm -rf /root/cybermatches
    sudo mkdir -p /root/cybermatches
    sudo tar -xzf /tmp/cybermatches-deploy.tar.gz -C /root/cybermatches
    sudo chown -R root:root /root/cybermatches
    
    echo "📁 Создание директории для логов..."
    sudo mkdir -p /root/cybermatches/logs
    
    echo "🔐 Копирование .env файла из бэкапа..."
    LATEST_BACKUP=$(ls -dt /root/cybermatches-backup-* 2>/dev/null | head -n1 || echo "")
    if [ -n "$LATEST_BACKUP" ] && [ -f "$LATEST_BACKUP/.env" ]; then
        sudo cp "$LATEST_BACKUP/.env" /root/cybermatches/.env
        echo "✅ .env файл скопирован из бэкапа"
    else
        echo "⚠️ .env файл не найден в бэкапе, нужно создать вручную"
    fi
    
    echo "🔧 Установка прав доступа..."
    sudo chmod 600 /root/cybermatches/.env || true
    
    echo "🔄 Установка зависимостей..."
    cd /root/cybermatches
    pip3 install -r requirements.txt
    pip3 install uvicorn
    
    echo "🚀 Перезапуск сервисов..."
    sudo systemctl restart cybermatches.service
    sudo systemctl restart cybermatches-api.service
    sudo systemctl restart cyber_telegram_bot.service
    
    echo "🔍 Проверка статуса сервисов..."
    echo "Parser service:"
    sudo systemctl is-active cybermatches.service
    
    echo "API service:"
    sudo systemctl is-active cybermatches-api.service
    
    echo "Bot service:"
    sudo systemctl is-active cyber_telegram_bot.service
    
    echo "🧹 Очистка временных файлов..."
    sudo rm -f /tmp/cybermatches-deploy.tar.gz
    
    echo "✅ Деплой завершен успешно!"
ENDSSH
    
    if [ $? -eq 0 ]; then
        log_info "Деплой на сервере завершен успешно"
    else
        log_error "Ошибка при деплое на сервере"
        exit 1
    fi
}

# Очистка локальных файлов
cleanup_local() {
    log_info "Очистка локальных файлов..."
    rm -f cybermatches-deploy.tar.gz
}

# Проверка деплоя
verify_deployment() {
    log_info "Проверка деплоя..."
    
    ssh ${SERVER_USER}@${SERVER_HOST} << 'ENDSSH'
    echo "📊 Статус сервисов:"
    echo "Parser: $(sudo systemctl is-active cybermatches.service)"
    echo "API: $(sudo systemctl is-active cybermatches-api.service)"
    echo "Bot: $(sudo systemctl is-active cyber_telegram_bot.service)"
    
    echo ""
    echo "📈 Последние строки логов:"
    echo "Parser log:"
    sudo tail -n 5 /root/cybermatches/logs/parser.log 2>/dev/null || echo "Лог парсера не найден"
    
    echo ""
    echo "API log:"
    sudo tail -n 5 /root/cybermatches/logs/api.log 2>/dev/null || echo "Лог API не найден"
    
    echo ""
    echo "Bot log:"
    sudo tail -n 5 /root/cybermatches/logs/bot.log 2>/dev/null || echo "Лог бота не найден"
ENDSSH
}

# Главная функция
main() {
    log_info "🚀 Начало ручного деплоя CyberMatches на сервер ${SERVER_HOST}"
    
    check_dependencies
    create_deployment_package
    copy_to_server
    deploy_on_server
    cleanup_local
    verify_deployment
    
    log_info "🎉 Деплой завершен успешно!"
    log_info "📍 Проверьте логи сервисов для подтверждения корректной работы"
}

# Обработка аргументов командной строки
case "${1:-}" in
    --help|-h)
        echo "Использование: $0 [опции]"
        echo ""
        echo "Опции:"
        echo "  --help, -h     Показать эту справку"
        echo "  --version, -v  Показать версию скрипта"
        echo ""
        echo "Пример использования:"
        echo "  $0              Запустить деплой"
        echo "  $0 --help       Показать справку"
        exit 0
        ;;
    --version|-v)
        echo "CyberMatches Deploy Script v1.0"
        exit 0
        ;;
    "")
        # Запуск основной функции
        main
        ;;
    *)
        log_error "Неизвестная опция: $1"
        echo "Используйте $0 --help для справки"
        exit 1
        ;;
esac
