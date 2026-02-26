from pathlib import Path
import sqlite3
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
DB_PATH = PROJECT_ROOT / "talentops.db"
SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"

TABLE_FILES = {
    "roles": "roles_mock.csv",
    "candidates": "candidates_mock.csv",
    "pipeline_events": "pipeline_events_mock.csv",
    "scorecards": "scorecards_mock.csv",
    "talent_market_map": "talent_market_map_mock.csv",
}

def run_schema(conn: sqlite3.Connection) -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema_sql)

def load_csvs(conn: sqlite3.Connection) -> None:
    for table, filename in TABLE_FILES.items():
        path = RAW_DIR / filename
        df = pd.read_csv(path)

        # write into SQLite
        df.to_sql(table, conn, if_exists="append", index=False)
        print(f"Loaded {table}: {len(df)} rows from {filename}")

def quick_checks(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for table in TABLE_FILES.keys():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"DB count {table}: {count}")

def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        run_schema(conn)
        load_csvs(conn)
        quick_checks(conn)
    finally:
        conn.close()

    print(f"\nDone. SQLite DB at: {DB_PATH}")

if __name__ == "__main__":
    main()
