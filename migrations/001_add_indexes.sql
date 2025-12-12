-- Migration 001: Add database indexes for performance optimization
-- Created: 2025-12-12
-- Purpose: Fix N+1 query problems and improve query performance

-- Индекс 1: Оптимизация запросов по дате (самые частые запросы)
-- Используется в: api.py get_matches_for_date(), get_matches_with_tournament_filter()
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dota_matches_time_msk_date
ON dota_matches ((match_time_msk AT TIME ZONE 'Europe/Moscow')::date);

-- Индекс 2: Фильтрация по статусу + времени (для обновления счета live матчей)
-- Используется в: main.py для поиска активных матчей
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dota_matches_status_time
ON dota_matches (status, match_time_msk)
WHERE status IN ('live', 'upcoming');

-- Индекс 3: Агрегация по турнирам
-- Используется в: api.py для статистики и фильтрации
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dota_matches_tournament
ON dota_matches (tournament)
WHERE tournament IS NOT NULL;

-- Индекс 4: Запросы бота по chat_id
-- Используется в: cyber_telegram_bot.py для поиска подписчиков
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bot_subscribers_chat_id
ON dota_bot_subscribers (chat_id);

-- Индекс 5: Сообщения бота по chat_id + день
-- Используется в: cyber_telegram_bot.py для отслеживания сообщений
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bot_today_messages_chat_day
ON dota_bot_today_messages (chat_id, day);

-- Индекс 6: Поиск команд по имени (case-insensitive)
-- Используется в: api.py get_team_urls_batch() для batch загрузки URL
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dota_teams_name_lower
ON dota_teams (LOWER(name));

-- Проверка созданных индексов
-- Запустите после применения миграции:
-- SELECT schemaname, tablename, indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename IN ('dota_matches', 'dota_bot_subscribers', 'dota_bot_today_messages', 'dota_teams')
-- ORDER BY tablename, indexname;
