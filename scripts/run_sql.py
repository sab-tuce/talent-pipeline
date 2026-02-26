from pathlib import Path
import sqlite3
import pandas as pd
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "talentops.db"

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_sql.py sql/your_query.sql")
        raise SystemExit(1)

    sql_path = PROJECT_ROOT / sys.argv[1]
    sql = sql_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(sql, conn)
    finally:
        conn.close()

    print(df.to_string(index=False))

if __name__ == "__main__":
    main()
