-- Funnel metrics (reach-based) using pipeline_events
-- A candidate is counted in a stage if they have at least one event with that stage_name.

WITH reached AS (
  SELECT
    candidate_id,
    MAX(CASE WHEN stage_name = 'Sourced' THEN 1 ELSE 0 END) AS reached_sourced,
    MAX(CASE WHEN stage_name = 'Contacted' THEN 1 ELSE 0 END) AS reached_contacted,
    MAX(CASE WHEN stage_name = 'Recruiter Screen' THEN 1 ELSE 0 END) AS reached_screen,
    MAX(CASE WHEN stage_name IN ('Hiring Manager','Panel') THEN 1 ELSE 0 END) AS reached_interview,
    MAX(CASE WHEN stage_name = 'Offer' THEN 1 ELSE 0 END) AS reached_offer,
    MAX(CASE WHEN stage_name = 'Hired' THEN 1 ELSE 0 END) AS reached_hired,
    MAX(CASE WHEN stage_name = 'Rejected' THEN 1 ELSE 0 END) AS reached_rejected,
    MAX(CASE WHEN stage_name = 'Withdrawn' THEN 1 ELSE 0 END) AS reached_withdrawn
  FROM pipeline_events
  GROUP BY candidate_id
),
counts AS (
  SELECT
    SUM(reached_sourced)   AS n_sourced,
    SUM(reached_contacted) AS n_contacted,
    SUM(reached_screen)    AS n_screen,
    SUM(reached_interview) AS n_interview,
    SUM(reached_offer)     AS n_offer,
    SUM(reached_hired)     AS n_hired,
    SUM(reached_rejected)  AS n_rejected,
    SUM(reached_withdrawn) AS n_withdrawn
  FROM reached
)
SELECT
  n_sourced,
  n_contacted,
  ROUND(1.0 * n_contacted / NULLIF(n_sourced,0), 3) AS sourced_to_contacted,
  n_screen,
  ROUND(1.0 * n_screen / NULLIF(n_contacted,0), 3) AS contacted_to_screen,
  n_interview,
  ROUND(1.0 * n_interview / NULLIF(n_screen,0), 3) AS screen_to_interview,
  n_offer,
  ROUND(1.0 * n_offer / NULLIF(n_interview,0), 3) AS interview_to_offer,
  n_hired,
  ROUND(1.0 * n_hired / NULLIF(n_offer,0), 3) AS offer_to_hired,
  n_rejected,
  n_withdrawn
FROM counts;
