# CyberMatches

Программа для мониторинга и уведомлений о матчах Dota 2 и Counter-Strike 2 с Liquipedia.

## Компоненты

- **Парсеры**: Сбор матчей с Liquipedia (Dota2, CS2, команды).
  - `main.py` - парсер матчей Dota 2
  - `cs2_main.py` - парсер матчей CS2
  - `teams_parser.py` - парсер команд Dota 2 и CS2

- **API**: REST API на FastAPI для получения матчей.
  - `api.py` - сервер API с эндпоинтами для Dota2/CS2

- **Telegram бот**: Уведомляет о матчах, подписки, фильтры.
  - `cyber_telegram_bot.py` - бот для Telegram

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
