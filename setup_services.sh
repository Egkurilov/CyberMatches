#!/bin/bash
# Скрипт для настройки systemd сервисов CyberMatches

echo "🔧 Настройка systemd сервисов..."

# Создаем директорию для логов
sudo mkdir -p /root/cybermatches/logs

# Parser Service
sudo tee /etc/systemd/system/cybermatches.service > /dev/null <<'EOF'
[Unit]
Description=CyberMatches Liquipedia Dota2 scraper
After=network.target postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/root/cybermatches
Environment=PYTHONPATH=/root/cybermatches
ExecStart=/usr/bin/python3 /root/cybermatches/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# API Service
sudo tee /etc/systemd/system/cybermatches-api.service > /dev/null <<'EOF'
[Unit]
Description=CyberMatches API Service
After=network.target postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/root/cybermatches
Environment=PYTHONPATH=/root/cybermatches
ExecStart=/usr/bin/python3 -m uvicorn api:app --host 0.0.0.0 --port 8050
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Telegram Bot Service
sudo tee /etc/systemd/system/cyber_telegram_bot.service > /dev/null <<'EOF'
[Unit]
Description=CyberMatches Telegram Bot
After=network.target postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/root/cybermatches
Environment=PYTHONPATH=/root/cybermatches
ExecStart=/usr/bin/python3 /root/cybermatches/cyber_telegram_bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

echo "✅ Service файлы созданы"
echo "🔧 Перезагрузка systemd..."
sudo systemctl daemon-reload

echo "✅ Включение сервисов..."
sudo systemctl enable cybermatches.service cybermatches-api.service cyber_telegram_bot.service

echo "🚀 Готово! Service файлы настроены."
echo ""
echo "💡 Примечание: Сервисы используют системный Python3"
echo "   Для использования виртуального окружения, запустите setup_venv.sh после создания .venv"
