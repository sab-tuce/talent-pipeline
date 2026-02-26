from pathlib import Path
import sqlite3
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "talentops.db"

def q(conn, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)

def main():
    conn = sqlite3.connect(DB_PATH)

    # 1) stage distribution
    df1 = q(conn, """
    SELECT stage_name, COUNT(*) AS n_events
    FROM pipeline_events
    GROUP BY stage_name
    ORDER BY n_events DESC;
    """)
    print("\n--- Events by stage ---")
    print(df1.to_string(index=False))

    # 2) candidates by role
    df2 = q(conn, """
    SELECT target_role, COUNT(*) AS n_candidates
    FROM candidates
    GROUP BY target_role
    ORDER BY n_candidates DESC;
    """)
    print("\n--- Candidates by target_role ---")
    print(df2.to_string(index=False))

    # 3) join example: hired count by role (based on terminal stage event)
    df3 = q(conn, """
    SELECT c.target_role, COUNT(*) AS hired_count
    FROM pipeline_events e
    JOIN candidates c ON c.candidate_id = e.candidate_id
    WHERE e.stage_name = 'Hired'
    GROUP BY c.target_role
    ORDER BY hired_count DESC;
    """)
    print("\n--- Hired count by role (from events) ---")
    print(df3.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    main()
