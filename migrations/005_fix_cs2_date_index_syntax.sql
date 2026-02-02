SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname='public' AND tablename='cs2_matches'
  AND indexdef LIKE '%(match_time_msk)%';