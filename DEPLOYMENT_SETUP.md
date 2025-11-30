# 🚀 Настройка автоматического деплоя CyberMatches

## 📋 Содержание
1. [Настройка сервера](#настройка-сервера)
2. [Настройка GitHub](#настройка-github)
3. [Проверка деплоя](#проверка-деплоя)
4. [Ручной деплой](#ручной-деплой)
5. [Откат изменений](#откат-изменений)

---

## 🔧 Настройка сервера (45.10.245.84)

### 1. Создание systemd service файлов

#### Parser Service (`/etc/systemd/system/cybermatches.service`)
```ini
[Unit]
Description=CyberMatches Parser Service
After=network.target postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/root/cybermatches
Environment=PYTHONPATH=/root/cybermatches
ExecStart=/usr/bin/python3 /root/cybermatches/main.py
Restart=always
RestartSec=10
StandardOutput=append:/root/cybermatches/logs/parser.log
StandardError=append:/root/cybermatches/logs/parser.log

[Install]
WantedBy=multi-user.target
```

#### API Service (`/etc/systemd/system/cybermatches-api.service`)
```ini
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
StandardOutput=append:/root/cybermatches/logs/api.log
StandardError=append:/root/cybermatches/logs/api.log

[Install]
WantedBy=multi-user.target
```

#### Telegram Bot Service (`/etc/systemd/system/cyber_telegram_bot.service`)
```ini
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
RestartSec=10
StandardOutput=append:/root/cybermatches/logs/bot.log
StandardError=append:/root/cybermatches/logs/bot.log

[Install]
WantedBy=multi-user.target
```

### 2. Установка и настройка

```bash
# Создать директорию для проекта
sudo mkdir -p /root/cybermatches/logs

# Установить зависимости
cd /root/cybermatches
pip3 install -r requirements.txt

# Установить uvicorn для API
pip3 install uvicorn

# Создать .env файл (заполнить реальными данными)
cat > /root/cybermatches/.env << 'EOF'
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cybermatches
DB_USER=postgres
DB_PASSWORD=your_password

# Parser
SCRAPE_INTERVAL_SECONDS=600

# Telegram Bot
TELEGRAM_BOT_TOKEN=your_bot_token
MATCHES_API_URL=http://45.10.245.84:8050/dota/matches/today
POLL_INTERVAL_SECONDS=60
EOF

# Установить права
chmod 600 /root/cybermatches/.env

# Перезагрузить systemd
sudo systemctl daemon-reload

# Включить автозапуск
sudo systemctl enable cybermatches.service
sudo systemctl enable cybermatches-api.service
sudo systemctl enable cyber_telegram_bot.service

# Запустить сервисы
sudo systemctl start cybermatches.service
sudo systemctl start cybermatches-api.service
sudo systemctl start cyber_telegram_bot.service

# Проверить статус
sudo systemctl status cybermatches.service
sudo systemctl status cybermatches-api.service
sudo systemctl status cyber_telegram_bot.service
```

### 3. Настройка firewall (если нужно)
```bash
# Открыть порт для API
sudo ufw allow 8050/tcp

# Проверить статус
sudo ufw status
```

---

## 🔐 Настройка GitHub

### 1. Добавьте Secrets в репозиторий

Перейдите в Settings → Secrets and variables → Actions → New repository secret

Добавьте следующие secrets:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `SERVER_HOST` | IP адрес сервера | `45.10.245.84` |
| `SERVER_USER` | Пользователь SSH | `root` |
| `SERVER_PASSWORD` | Пароль пользователя | `your_password` |
| `SERVER_PORT` | Порт SSH (опционально) | `22` |

### 2. Настройка веток
Убедитесь, что workflow настроен на нужные ветки в файле `.github/workflows/deploy.yml`:
```yaml
on:
  push:
    branches: [ main, master ]
```

---

## ✅ Проверка деплоя

### 1. Проверка логов GitHub Actions
- Перейдите в раздел Actions вашего репозитория
- Найдите последний workflow run
- Проверьте логи на наличие ошибок

### 2. Проверка на сервере
```bash
# Проверить логи деплоя
sudo journalctl -u cybermatches.service -f
sudo journalctl -u cybermatches-api.service -f
sudo journalctl -u cyber_telegram_bot.service -f

# Проверить статус сервисов
sudo systemctl status cybermatches.service
sudo systemctl status cybermatches-api.service
sudo systemctl status cyber_telegram_bot.service

# Проверить API
curl http://45.10.245.84:8050/dota/matches/today
```

### 3. Проверка логов приложения
```bash
# Логи парсера
tail -f /root/cybermatches/logs/parser.log

# Логи API
tail -f /root/cybermatches/logs/api.log

# Логи бота
tail -f /root/cybermatches/logs/bot.log
```

---

## 🔄 Ручной деплой (если GitHub Actions не работает)

### Скрипт ручного деплоя (`deploy.sh`)
```bash
#!/bin/bash
set -e

echo "🚀 Starting manual deployment..."

# Configuration
SERVER_HOST="45.10.245.84"
SERVER_USER="root"
REMOTE_DIR="/root/cybermatches"
BACKUP_DIR="/root/cybermatches-backup-$(date +%Y%m%d-%H%M%S)"

# Create deployment package
echo "📦 Creating deployment package..."
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
  .

# Copy to server
echo "📤 Copying to server..."
scp cybermatches-deploy.tar.gz ${SERVER_USER}@${SERVER_HOST}:/tmp/

# Deploy on server
echo "🎯 Deploying on server..."
ssh ${SERVER_USER}@${SERVER_HOST} << 'ENDSSH'
  set -e
  
  echo "Stopping services..."
  sudo systemctl stop cybermatches.service || true
  sudo systemctl stop cybermatches-api.service || true
  sudo systemctl stop cyber_telegram_bot.service || true
  
  echo "Creating backup..."
  if [ -d "/root/cybermatches" ]; then
    sudo mv /root/cybermatches /root/cybermatches-backup-$(date +%Y%m%d-%H%M%S)
  fi
  
  echo "Extracting new deployment..."
  sudo rm -rf /root/cybermatches
  sudo mkdir -p /root/cybermatches
  sudo tar -xzf /tmp/cybermatches-deploy.tar.gz -C /root/cybermatches
  sudo chown -R root:root /root/cybermatches
  
  echo "Setting up environment..."
  sudo mkdir -p /root/cybermatches/logs
  sudo chmod 600 /root/cybermatches/.env || true
  
  echo "Restarting services..."
  sudo systemctl restart cybermatches.service
  sudo systemctl restart cybermatches-api.service
  sudo systemctl restart cyber_telegram_bot.service
  
  echo "Checking service status..."
  sudo systemctl is-active cybermatches.service
  sudo systemctl is-active cybermatches-api.service
  sudo systemctl is-active cyber_telegram_bot.service
  
  echo "Cleaning up..."
  sudo rm -f /tmp/cybermatches-deploy.tar.gz
  
  echo "✅ Manual deployment completed!"
ENDSSH

# Clean up local files
rm -f cybermatches-deploy.tar.gz

echo "🎉 Manual deployment finished successfully!"
```

Сделайте скрипт исполняемым:
```bash
chmod +x deploy.sh
```

Использование:
```bash
./deploy.sh
```

---

## 🔄 Откат изменений

### 1. Автоматический откат (если деплой не удался)
GitHub Actions автоматически остановит сервисы, если деплой не удался.

### 2. Ручной откат
```bash
# На сервере
cd /root

# Найти последний бэкап
LATEST_BACKUP=$(ls -dt cybermatches-backup-* | head -n1)

# Остановить текущие сервисы
sudo systemctl stop cybermatches.service
sudo systemctl stop cybermatches-api.service
sudo systemctl stop cyber_telegram_bot.service

# Восстановить из бэкапа
sudo rm -rf /root/cybermatches
sudo mv "$LATEST_BACKUP" /root/cybermatches

# Перезапустить сервисы
sudo systemctl restart cybermatches.service
sudo systemctl restart cybermatches-api.service
sudo systemctl restart cyber_telegram_bot.service

# Проверить статус
sudo systemctl status cybermatches.service
sudo systemctl status cybermatches-api.service
sudo systemctl status cyber_telegram_bot.service
```

---

## 🛠️ Устранение неполадок

### Проблема: Сервисы не запускаются
```bash
# Проверить логи
sudo journalctl -u cybermatches.service -n 50

# Проверить конфигурацию
sudo systemctl cat cybermatches.service

# Проверить зависимости
sudo systemctl list-dependencies cybermatches.service
```

### Проблема: Ошибки в приложении
```bash
# Проверить логи приложения
tail -n 100 /root/cybermatches/logs/parser.log
tail -n 100 /root/cybermatches/logs/api.log
tail -n 100 /root/cybermatches/logs/bot.log

# Проверить права на файлы
ls -la /root/cybermatches/
ls -la /root/cybermatches/logs/
```

### Проблема: GitHub Actions не работает
1. Проверьте Secrets в репозитории
2. Проверьте логи workflow в GitHub
3. Убедитесь, что сервер доступен по SSH
4. Проверьте firewall на сервере

---

## 📞 Поддержка

Если возникли проблемы:
1. Проверьте логи сервисов
2. Убедитесь, что все зависимости установлены
3. Проверьте настройки в `.env` файле
4. Используйте ручной деплой при необходимости

Для помощи обратитесь к системному администратору или разработчику проекта.
