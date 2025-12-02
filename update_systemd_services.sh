#!/bin/bash
# Скрипт для обновления systemd сервисов на сервере

echo "🔄 Обновляем systemd сервисы..."

# Останавливаем сервисы
sudo systemctl stop cyber_telegram_bot.service || true

# Обновляем сервис телеграм-бота
sudo tee /etc/systemd/system/cyber_telegram_bot.service > /dev/null <<'EOF'
[Unit]
Description=CyberMatches Telegram Bot
After=network.target postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/root/cybermatches
Environment=PYTHONPATH=/root/cybermatches
ExecStart=/root/cybermatches/.venv/bin/python /root/cybermatches/cyber_telegram_bot_refactored.py
Restart=always
RestartSec=5
StandardOutput=append:/root/cybermatches/logs/bot.log
StandardError=append:/root/cybermatches/logs/bot.log

[Install]
WantedBy=multi-user.target
EOF

# Перезагружаем systemd
sudo systemctl daemon-reload

# Запускаем обновленный сервис
sudo systemctl start cyber_telegram_bot.service

echo "✅ Сервис телеграм-бота обновлен и запущен"

# Проверяем статус
sleep 2
sudo systemctl status cyber_telegram_bot.service --no-pager -l

echo "✅ Обновление systemd сервисов завершено"
