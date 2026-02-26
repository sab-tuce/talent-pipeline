-- SQLite schema for Talent Pipeline MVP

PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS scorecards;
DROP TABLE IF EXISTS pipeline_events;
DROP TABLE IF EXISTS candidates;
DROP TABLE IF EXISTS roles;
DROP TABLE IF EXISTS talent_market_map;

CREATE TABLE roles (
  role_id INTEGER PRIMARY KEY,
  role_name TEXT NOT NULL,
  hiring_manager TEXT,
  priority TEXT,
  open_date TEXT,
  target_close_date TEXT,
  headcount INTEGER,
  status TEXT
);

CREATE TABLE candidates (
  candidate_id INTEGER PRIMARY KEY,
  full_name TEXT NOT NULL,
  target_role TEXT,
  current_company TEXT,
  location TEXT,
  years_experience INTEGER,
  source_channel TEXT,
  skills_text TEXT,
  compensation_expectation INTEGER,
  current_stage TEXT,
  owner TEXT,
  applied_at TEXT
);

CREATE TABLE pipeline_events (
  event_id INTEGER PRIMARY KEY,
  candidate_id INTEGER NOT NULL,
  stage_name TEXT NOT NULL,
  entered_stage_at TEXT,
  exited_stage_at TEXT,
  outcome TEXT,
  owner TEXT,
  FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id)
);

CREATE TABLE scorecards (
  scorecard_id INTEGER PRIMARY KEY,
  candidate_id INTEGER NOT NULL,
  interviewer_name TEXT,
  interview_stage TEXT,
  competency TEXT,
  score INTEGER,
  notes TEXT,
  submitted_at TEXT,
  FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id)
);

CREATE TABLE talent_market_map (
  company TEXT,
  role_family TEXT,
  location TEXT,
  est_comp_low INTEGER,
  est_comp_high INTEGER,
  hiring_signal TEXT,
  notes TEXT
);

-- Helpful indexes
CREATE INDEX idx_pipeline_events_candidate ON pipeline_events(candidate_id);
CREATE INDEX idx_pipeline_events_stage ON pipeline_events(stage_name);
CREATE INDEX idx_scorecards_candidate ON scorecards(candidate_id);
