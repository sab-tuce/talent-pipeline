from pathlib import Path
import sqlite3
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "talentops.db"
SQL_PATH = PROJECT_ROOT / "sql" / "weekly_metrics.sql"
OUT_PATH = PROJECT_ROOT / "data" / "processed" / "weekly_metrics.csv"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def main():
    sql = SQL_PATH.read_text(encoding="utf-8")
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(sql, conn)
    finally:
        conn.close()

    df.to_csv(OUT_PATH, index=False)
    print("Wrote:", OUT_PATH, "rows=", len(df))
    print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
