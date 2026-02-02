-- Migration: Add indexes to improve query performance for match retrieval

-- Доступ к матчам по дате (match_time_msk) часто используется в запросах.
-- Добавляем индекс, позволяющий быстро отбирать записи нужного дня.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dota_matches_match_time_msk
    ON dota_matches (match_time_msk);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cs2_matches_match_time_msk
    ON cs2_matches (match_time_msk);

-- Индексы для быстрых поисков команд по имени (чувствительные к регистру)
CREATE INDEX IF NOT EXISTS idx_dota_teams_name_lower
    ON dota_teams (LOWER(name));

CREATE INDEX IF NOT EXISTS idx_cs2_teams_name_lower
    ON cs2_teams (LOWER(name));
