#!/bin/bash
# Скрипт для деплоя решения на сервере

echo "🚀 Начинаем деплой решения для парсера CyberMatches..."

# 1. Останавливаем сервис
echo "⏹️ Останавливаем сервис..."
sudo systemctl stop cybermatches.service

# 2. Копируем обновленные файлы
echo "📁 Копируем обновленные файлы..."
sudo cp /root/cybermatches/main.py /root/cybermatches/main.py.backup
sudo cp /root/cybermatches/improved_parser.py /root/cybermatches/improved_parser.py.backup

# 3. Очищаем дубликаты в БД (если есть)
echo "🧹 Очищаем дубликаты в БД..."
cd /root/cybermatches
python3 cleanup_duplicates.py

# 4. Добавляем правильные constraints
echo "🔧 Добавляем constraints в БД..."
python3 fix_database_constraints.py

# 5. Проверяем и обновляем схему БД при необходимости
echo "🗄️ Проверяем схему БД..."
python3 migrate_database_schema.py

# 6. Запускаем сервис
echo "▶️ Запускаем сервис..."
sudo systemctl start cybermatches.service

# 7. Проверяем статус
echo "📊 Проверяем статус сервиса..."
sleep 5
sudo systemctl status cybermatches.service

# 8. Проверяем логи
echo "📋 Проверяем последние логи..."
sudo journalctl -u cybermatches.service -n 50 --no-pager

echo "✅ Деплой завершен! Проверьте логи выше на наличие ошибок."
