-- Stage aging metrics: average & median-ish (SQLite doesn't have true median easily)
-- We'll compute average days in stage and also list stale candidates (> 7 days) by stage.

WITH base AS (
  SELECT
    candidate_id,
    stage_name,
    entered_stage_at,
    exited_stage_at,
    (julianday(exited_stage_at) - julianday(entered_stage_at)) AS duration_days
  FROM pipeline_events
  WHERE exited_stage_at IS NOT NULL AND exited_stage_at != ''
    AND entered_stage_at IS NOT NULL AND entered_stage_at != ''
    AND stage_name NOT IN ('Hired','Rejected','Withdrawn') -- terminal stages excluded
    AND stage_name != 'InvalidStage'
),
stage_avg AS (
  SELECT
    stage_name,
    COUNT(*) AS n_events,
    ROUND(AVG(duration_days), 2) AS avg_days_in_stage,
    ROUND(MIN(duration_days), 2) AS min_days,
    ROUND(MAX(duration_days), 2) AS max_days
  FROM base
  GROUP BY stage_name
),
stale AS (
  SELECT
    candidate_id,
    stage_name,
    ROUND(duration_days, 2) AS duration_days,
    entered_stage_at,
    exited_stage_at
  FROM base
  WHERE duration_days > 7
)
SELECT 'STAGE_AVG' AS section, * FROM stage_avg
UNION ALL
SELECT 'STALE' AS section,
       stage_name,
       candidate_id AS n_events,
       duration_days AS avg_days_in_stage,
       NULL AS min_days,
       NULL AS max_days
FROM stale
ORDER BY section, stage_name;
