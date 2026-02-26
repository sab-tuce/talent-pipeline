-- Data Hygiene Checks

-- 1) Missing critical fields in candidates
SELECT 'missing_owner' AS check_name, COUNT(*) AS n
FROM candidates
WHERE owner IS NULL OR TRIM(owner) = ''
UNION ALL
SELECT 'missing_target_role' AS check_name, COUNT(*) AS n
FROM candidates
WHERE target_role IS NULL OR TRIM(target_role) = ''
UNION ALL
SELECT 'missing_current_stage' AS check_name, COUNT(*) AS n
FROM candidates
WHERE current_stage IS NULL OR TRIM(current_stage) = '';

-- 2) Duplicate candidates by (full_name, current_company, target_role)
SELECT 'duplicate_candidates' AS check_name, COUNT(*) AS n
FROM (
  SELECT full_name, current_company, target_role, COUNT(*) AS c
  FROM candidates
  GROUP BY full_name, current_company, target_role
  HAVING COUNT(*) > 1
);

-- 3) Invalid stage labels (anything not in the standard set)
SELECT 'invalid_stage_labels' AS check_name, COUNT(*) AS n
FROM pipeline_events
WHERE stage_name NOT IN (
  'Sourced','Contacted','Recruiter Screen','Hiring Manager','Panel','Offer',
  'Hired','Rejected','Withdrawn'
);

-- 4) Stage date issues (exited <= entered) for non-terminal stages
SELECT 'stage_date_issues_exited_le_entered' AS check_name, COUNT(*) AS n
FROM pipeline_events
WHERE exited_stage_at IS NOT NULL AND exited_stage_at != ''
  AND entered_stage_at IS NOT NULL AND entered_stage_at != ''
  AND stage_name NOT IN ('Hired','Rejected','Withdrawn')
  AND julianday(exited_stage_at) <= julianday(entered_stage_at);

-- 5) Hired without Offer (should be 0 after your generator fix)
WITH hired AS (
  SELECT DISTINCT candidate_id FROM pipeline_events WHERE stage_name='Hired'
),
offer AS (
  SELECT DISTINCT candidate_id FROM pipeline_events WHERE stage_name='Offer'
)
SELECT 'hired_without_offer' AS check_name, COUNT(*) AS n
FROM hired h
LEFT JOIN offer o ON h.candidate_id = o.candidate_id
WHERE o.candidate_id IS NULL;

-- 6) Panel stage but no scorecards
WITH panel AS (
  SELECT DISTINCT candidate_id FROM pipeline_events WHERE stage_name='Panel'
),
scored AS (
  SELECT DISTINCT candidate_id FROM scorecards
)
SELECT 'panel_without_scorecard' AS check_name, COUNT(*) AS n
FROM panel p
LEFT JOIN scored s ON p.candidate_id = s.candidate_id
WHERE s.candidate_id IS NULL;
