# ⚡ Быстрый старт - Настройка деплоя CyberMatches

## 🎯 Что нужно сделать

### 1. На сервере 45.10.245.84
```bash
# Скопировать service файлы
sudo nano /etc/systemd/system/cybermatches.service
# (вставить содержимое из DEPLOYMENT_SETUP.md)

sudo nano /etc/systemd/system/cybermatches-api.service
# (вставить содержимое из DEPLOYMENT_SETUP.md)

sudo nano /etc/systemd/system/cyber_telegram_bot.service
# (вставить содержимое из DEPLOYMENT_SETUP.md)

# Перезагрузить systemd
sudo systemctl daemon-reload
```

### 2. В GitHub репозитории
Перейдите в Settings → Secrets and variables → Actions → New repository secret

Добавьте:
- `SERVER_HOST`: `45.10.245.84`
- `SERVER_USER`: `root`  
- `SERVER_PASSWORD`: `your_password`

### 3. Проверка
```bash
# Ручной деплой для теста
./deploy.sh

# Или просто запушьте в main ветку
git push origin main
```

## 📋 Проверка после деплоя

```bash
# На сервере проверьте статус
sudo systemctl status cybermatches.service
sudo systemctl status cybermatches-api.service
sudo systemctl status cyber_telegram_bot.service

# Проверьте API
curl http://45.10.245.84:8050/dota/matches/today
```

## 🔧 Если что-то пошло не так

1. Проверьте логи:
```bash
# Логи деплоя
sudo journalctl -u cybermatches.service -f

# Логи приложения
tail -f /root/cybermatches/logs/parser.log
```

2. Откатитесь:
```bash
# Ручной откат к последнему бэкапу
cd /root
LATEST_BACKUP=$(ls -dt cybermatches-backup-* | head -n1)
sudo systemctl stop cybermatches.service cybermatches-api.service cyber_telegram_bot.service
sudo rm -rf /root/cybermatches
sudo mv "$LATEST_BACKUP" /root/cybermatches
sudo systemctl restart cybermatches.service cybermatches-api.service cyber_telegram_bot.service
```

## 📞 Помощь

Полная инструкция: `DEPLOYMENT_SETUP.md`
Скрипт ручного деплоя: `./deploy.sh`

**Готово!** 🎉 Теперь каждый push в main ветку будет автоматически деплоиться на сервер.
