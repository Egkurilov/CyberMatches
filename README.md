# CyberMatches

Программа для мониторинга и уведомлений о матчах Dota 2 и Counter-Strike 2 с Liquipedia.

## Компоненты

- **Парсеры**: Сбор матчей с Liquipedia (Dota2, CS2, команды).
  - `main.py` - парсер матчей Dota 2 (обёртка над `cybermatches.parsers.dota`)
  - `cs2_main.py` - парсер матчей CS2 (обёртка над `cybermatches.parsers.cs2`)
  - `teams_parser.py` - парсер команд Dota 2 (обёртка над `cybermatches.teams.dota`)

- **API**: REST API на FastAPI для получения матчей.
  - `api.py` - сервер API с эндпоинтами для Dota2/CS2 (обёртка над `cybermatches.api.app`)

- **Telegram бот**: Уведомляет о матчах, подписки, фильтры.
  - `cyber_telegram_bot.py` - бот для Telegram (обёртка над `cybermatches.bot.app`)

- **Пакет**: общая логика и модули.
  - `cybermatches/common` - общие утилиты (time/text)
  - `cybermatches/parsers` - парсеры Dota/CS2
  - `cybermatches/api` - API-приложение
  - `cybermatches/bot` - Telegram-бот
  - `cybermatches/teams` - парсер команд

## База данных

PostgreSQL с таблицами:
- `dota_matches` - матчи Dota 2
- `cs2_matches` - матчи CS2
- `dota_teams` - команды Dota 2
- `cs2_teams` - команды CS2
- `matches_bot_*` - состояние бота

## Запуск

1. Установить зависимости: `pip install -r requirements.txt`
2. Настроить `.env` по `.env.example`
3. Запустить парсеры или API/бот по необходимости

Основные команды:
- `/home/littleauto/cyberboohta/.venv/bin/python main.py` - однократный парсинг Dota
- Аналогично для cs2_main.py и api.py

## Мониторинг

- Логи: `logs/`
- Службы: systemd (service/*.txt)
- Метрики Prometheus: `http://<host>:9108/metrics` (настройки: `METRICS_ADDR`, `METRICS_PORT`)

## Scheduler (постоянный процесс)

Для постоянного экспорта метрик и периодического запуска парсеров:

```
./.venv/bin/python scheduler.py
```

Переменные:
- `DOTA_INTERVAL_SECONDS` (по умолчанию 600)
- `CS2_INTERVAL_SECONDS` (по умолчанию 600)
