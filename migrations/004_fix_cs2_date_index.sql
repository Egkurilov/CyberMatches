-- Migration: Correct functional index for date extraction on cs2_matches
-- The previous index used an incorrect expression:
--   (match_time_msk AT TIME ZONE 'Europe/Moscow'::text)
-- which prevents the planner from using it for the query:
--   WHERE (match_time_msk AT TIME ZONE 'Europe/Moscow')::date = %s
-- The proper expression casts the result to DATE.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cs2_matches_match_time_msk_date
    ON public.cs2_matches ((match_time_msk AT TIME ZONE 'Europe/Moscow')::date);
