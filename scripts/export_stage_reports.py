from pathlib import Path
import sqlite3
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "talentops.db"
OUT_DIR = PROJECT_ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    conn = sqlite3.connect(DB_PATH)

    # 1) Stage metrics (avg/min/max)
    stage_metrics = pd.read_sql_query(
        """
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
        )
        SELECT
          stage_name,
          COUNT(*) AS n_events,
          ROUND(AVG(duration_days), 2) AS avg_days_in_stage,
          ROUND(MIN(duration_days), 2) AS min_days,
          ROUND(MAX(duration_days), 2) AS max_days
        FROM base
        GROUP BY stage_name
        ORDER BY stage_name;
        """,
        conn,
    )

    # 2) Stale candidates list (>7 days)
    stale = pd.read_sql_query(
        """
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
            AND stage_name NOT IN ('Hired','Rejected','Withdrawn')
            AND stage_name != 'InvalidStage'
        )
        SELECT
          candidate_id,
          stage_name,
          ROUND(duration_days, 2) AS duration_days,
          entered_stage_at,
          exited_stage_at
        FROM base
        WHERE duration_days > 7
        ORDER BY stage_name, duration_days DESC;
        """,
        conn,
    )

    conn.close()

    stage_metrics_path = OUT_DIR / "stage_metrics.csv"
    stale_path = OUT_DIR / "stage_aging_report.csv"

    stage_metrics.to_csv(stage_metrics_path, index=False)
    stale.to_csv(stale_path, index=False)

    print("Wrote:", stage_metrics_path, "rows=", len(stage_metrics))
    print("Wrote:", stale_path, "rows=", len(stale))

if __name__ == "__main__":
    main()
