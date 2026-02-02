-- Migration 002: Add missing indexes for CS2 performance optimization
-- Created: 2025-12-19
-- Purpose: Speed up CS2 API queries similar to Dota 2

-- Migration 002: Add missing indexes for CS2 - без CONCURRENTLY для скорости
-- В production лучше CONCURRENTLY, чтобы не блокировать reads/writes,
-- но на больших данных может зависать из-за конфликтов с DML.
-- Если зависает - используйте обычный CREATE INDEX (быстрее, но блокирует таблицу)

-- Индекс 1: Оптимизация по дате (date() функция вместо ::date)
CREATE INDEX IF NOT EXISTS idx_cs2_matches_time_msk_date
ON cs2_matches (date(match_time_msk AT TIME ZONE 'Europe/Moscow'));

-- Индекс 2: Фильтрация по статусу + времени для live матчей
CREATE INDEX IF NOT EXISTS idx_cs2_matches_status_time
ON cs2_matches (status, match_time_msk)
WHERE status IN ('live', 'upcoming');

-- Индекс 3: Агрегация по турнирам для статистики
CREATE INDEX IF NOT EXISTS idx_cs2_matches_tournament
ON cs2_matches (tournament)
WHERE tournament IS NOT NULL;

-- Индекс 4: Комбинированный date + status для оптимизации API
CREATE INDEX IF NOT EXISTS idx_cs2_matches_date_status
ON cs2_matches (
    date(match_time_msk AT TIME ZONE 'Europe/Moscow'),
    status
);

-- ЕСЛИ НАДО ПЕРЕСОЗДАТЬ С CONCURRENTLY (без блокировки, но долго):
-- Отделпно запустите, пока нет активности:
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cs2_matches_status_time
-- ON cs2_matches (status, match_time_msk)
-- WHERE status IN ('live', 'upcoming');

-- Так же для остальных с CONCURRENTLY.

-- Проверка индексов для CS2:
-- SELECT schemaname, tablename, indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename LIKE '%cs2%' AND indexname LIKE '%idx%'
-- ORDER BY tablename, indexname;
