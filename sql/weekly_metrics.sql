-- Weekly metrics based on entered_stage_at of pipeline events
-- Week bucket: year-week (YYYY-WW)

WITH base AS (
  SELECT
    candidate_id,
    stage_name,
    date(entered_stage_at) AS entered_date,
    strftime('%Y-%W', entered_stage_at) AS year_week
  FROM pipeline_events
  WHERE entered_stage_at IS NOT NULL AND entered_stage_at != ''
    AND stage_name != 'InvalidStage'
),
weekly AS (
  SELECT
    year_week,
    COUNT(DISTINCT CASE WHEN stage_name='Sourced' THEN candidate_id END) AS sourced,
    COUNT(DISTINCT CASE WHEN stage_name='Contacted' THEN candidate_id END) AS contacted,
    COUNT(DISTINCT CASE WHEN stage_name='Recruiter Screen' THEN candidate_id END) AS screen,
    COUNT(DISTINCT CASE WHEN stage_name IN ('Hiring Manager','Panel') THEN candidate_id END) AS interview,
    COUNT(DISTINCT CASE WHEN stage_name='Offer' THEN candidate_id END) AS offer,
    COUNT(DISTINCT CASE WHEN stage_name='Hired' THEN candidate_id END) AS hired,
    COUNT(DISTINCT CASE WHEN stage_name='Rejected' THEN candidate_id END) AS rejected,
    COUNT(DISTINCT CASE WHEN stage_name='Withdrawn' THEN candidate_id END) AS withdrawn
  FROM base
  GROUP BY year_week
)
SELECT *
FROM weekly
ORDER BY year_week;
