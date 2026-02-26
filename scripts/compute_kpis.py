from pathlib import Path
import sqlite3
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "talentops.db"
OUT_DIR = PROJECT_ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def scalar(conn: sqlite3.Connection, sql: str) -> float:
    return pd.read_sql_query(sql, conn).iloc[0, 0]

def main():
    conn = sqlite3.connect(DB_PATH)

    # --- Core counts ---
    open_roles = scalar(conn, "SELECT COUNT(*) FROM roles WHERE status='Open';")
    total_candidates = scalar(conn, "SELECT COUNT(*) FROM candidates;")
    offers = scalar(conn, "SELECT COUNT(DISTINCT candidate_id) FROM pipeline_events WHERE stage_name='Offer';")
    hired = scalar(conn, "SELECT COUNT(DISTINCT candidate_id) FROM pipeline_events WHERE stage_name='Hired';")
    rejected = scalar(conn, "SELECT COUNT(DISTINCT candidate_id) FROM pipeline_events WHERE stage_name='Rejected';")
    withdrawn = scalar(conn, "SELECT COUNT(DISTINCT candidate_id) FROM pipeline_events WHERE stage_name='Withdrawn';")

    offer_acceptance_rate = (hired / offers) if offers else None

    # --- Invalid stage events (data quality) ---
    invalid_stage_events = scalar(conn, "SELECT COUNT(*) FROM pipeline_events WHERE stage_name='InvalidStage';")

    # --- Stale candidates (>7 days) using ONLY date-valid events ---
    stale_candidates = scalar(conn, """
        WITH base AS (
          SELECT
            candidate_id,
            stage_name,
            (julianday(exited_stage_at) - julianday(entered_stage_at)) AS duration_days
          FROM pipeline_events
          WHERE exited_stage_at IS NOT NULL AND exited_stage_at != ''
            AND entered_stage_at IS NOT NULL AND entered_stage_at != ''
            AND stage_name NOT IN ('Hired','Rejected','Withdrawn')
            AND stage_name != 'InvalidStage'
            AND julianday(exited_stage_at) > julianday(entered_stage_at) -- date-valid only
        )
        SELECT COUNT(DISTINCT candidate_id)
        FROM base
        WHERE duration_days > 7;
    """)

    # --- Scorecard completion rate (process quality) ---
    panel_candidates = scalar(conn, "SELECT COUNT(DISTINCT candidate_id) FROM pipeline_events WHERE stage_name='Panel';")
    panel_with_scorecards = scalar(conn, """
        WITH panel AS (SELECT DISTINCT candidate_id FROM pipeline_events WHERE stage_name='Panel'),
             scored AS (SELECT DISTINCT candidate_id FROM scorecards)
        SELECT COUNT(*)
        FROM panel p JOIN scored s ON p.candidate_id = s.candidate_id;
    """)
    scorecard_completion_rate = (panel_with_scorecards / panel_candidates) if panel_candidates else None

    conn.close()

    kpis = pd.DataFrame(
        [
            {"metric": "open_roles", "value": int(open_roles)},
            {"metric": "total_candidates", "value": int(total_candidates)},
            {"metric": "offers", "value": int(offers)},
            {"metric": "hired", "value": int(hired)},
            {"metric": "rejected", "value": int(rejected)},
            {"metric": "withdrawn", "value": int(withdrawn)},
            {"metric": "offer_acceptance_rate", "value": round(offer_acceptance_rate, 3) if offer_acceptance_rate is not None else None},
            {"metric": "stale_candidates_gt_7d", "value": int(stale_candidates)},
            {"metric": "invalid_stage_events", "value": int(invalid_stage_events)},
            {"metric": "panel_candidates", "value": int(panel_candidates)},
            {"metric": "scorecard_completion_rate", "value": round(scorecard_completion_rate, 3) if scorecard_completion_rate is not None else None},
        ]
    )

    out_path = OUT_DIR / "kpi_summary.csv"
    kpis.to_csv(out_path, index=False)

    print("Wrote:", out_path)
    print(kpis.to_string(index=False))

if __name__ == "__main__":
    main()
