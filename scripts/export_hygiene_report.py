from pathlib import Path
import sqlite3
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "talentops.db"
SQL_PATH = PROJECT_ROOT / "sql" / "data_hygiene_checks.sql"
OUT_PATH = PROJECT_ROOT / "data" / "processed" / "data_hygiene_report.csv"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def main():
    sql = SQL_PATH.read_text(encoding="utf-8")

    conn = sqlite3.connect(DB_PATH)
    try:
        # executescript doesn't return results, so we split by ';' and run SELECT blocks
        blocks = [b.strip() for b in sql.split(";") if b.strip()]
        frames = []
        for b in blocks:
            df = pd.read_sql_query(b, conn)
            frames.append(df)
        out = pd.concat(frames, ignore_index=True)
    finally:
        conn.close()

    out.to_csv(OUT_PATH, index=False)
    print("Wrote:", OUT_PATH)
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()
