-- Migration 006: Deduplicate dota_matches by preferring lp:ID records
-- Safe, idempotent cleanup for duplicate rows created during UID migration.

BEGIN;

-- 1) Remove exact-key duplicates, keep the "best" row by priority.
WITH ranked AS (
    SELECT
        id,
        match_uid,
        match_time_msk,
        LOWER(COALESCE(team1, '')) AS team1_norm,
        LOWER(COALESCE(team2, '')) AS team2_norm,
        LOWER(COALESCE(tournament, '')) AS tournament_norm,
        COALESCE(bo, 0) AS bo_norm,
        CASE WHEN match_uid LIKE 'lp:ID_%' THEN 0 ELSE 1 END AS pr_uid,
        CASE
            WHEN COALESCE(LOWER(TRIM(team1)), '') NOT IN ('', 'tbd', 'tba', 'to be decided', 'to be determined')
             AND COALESCE(LOWER(TRIM(team2)), '') NOT IN ('', 'tbd', 'tba', 'to be decided', 'to be determined')
            THEN 0 ELSE 1
        END AS pr_full_teams,
        CASE
            WHEN COALESCE(score, '') NOT IN ('', '0:0') THEN 0 ELSE 1
        END AS pr_score,
        COALESCE(updated_at, created_at) AS ts
    FROM dota_matches
),
ranked2 AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_time_msk, team1_norm, team2_norm, tournament_norm, bo_norm
            ORDER BY pr_uid, pr_full_teams, pr_score, ts DESC, id DESC
        ) AS rn
    FROM ranked
)
DELETE FROM dota_matches d
USING ranked2 r
WHERE d.id = r.id
  AND r.rn > 1;

-- 2) Remove placeholder (TBD/TBA) rows when a real opponent exists in same slot.
DELETE FROM dota_matches d
WHERE
    (COALESCE(LOWER(TRIM(d.team1)), '') IN ('', 'tbd', 'tba', 'to be decided', 'to be determined')
     OR COALESCE(LOWER(TRIM(d.team2)), '') IN ('', 'tbd', 'tba', 'to be decided', 'to be determined'))
  AND EXISTS (
      SELECT 1
      FROM dota_matches d2
      WHERE d2.id <> d.id
        AND d2.match_time_msk = d.match_time_msk
        AND COALESCE(LOWER(d2.tournament), '') = COALESCE(LOWER(d.tournament), '')
        AND COALESCE(LOWER(TRIM(d2.team1)), '') NOT IN ('', 'tbd', 'tba', 'to be decided', 'to be determined')
        AND COALESCE(LOWER(TRIM(d2.team2)), '') NOT IN ('', 'tbd', 'tba', 'to be decided', 'to be determined')
        AND (
            (COALESCE(LOWER(TRIM(d.team1)), '') NOT IN ('', 'tbd', 'tba', 'to be decided', 'to be determined')
             AND (LOWER(d2.team1) = LOWER(d.team1) OR LOWER(d2.team2) = LOWER(d.team1)))
         OR (COALESCE(LOWER(TRIM(d.team2)), '') NOT IN ('', 'tbd', 'tba', 'to be decided', 'to be determined')
             AND (LOWER(d2.team1) = LOWER(d.team2) OR LOWER(d2.team2) = LOWER(d.team2)))
        )
  );

COMMIT;
