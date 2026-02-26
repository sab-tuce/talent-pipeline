from pathlib import Path
import pandas as pd
from scripts.constants import STAGES

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUT_DIR = PROJECT_ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

VALID_STAGES = set(STAGES)

def clean_candidates(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    report = {}

    # normalize text columns
    for col in ["full_name", "target_role", "current_company", "location", "source_channel", "skills_text", "current_stage", "owner"]:
        if col in df.columns:
            df[col] = df[col].astype("string").fillna("").str.strip()

    # remove exact duplicate candidates by (full_name, current_company, target_role)
    before = len(df)
    df = df.sort_values("candidate_id").drop_duplicates(subset=["full_name", "current_company", "target_role"], keep="first")
    report["candidates_duplicates_removed"] = before - len(df)

    # basic type fixes
    if "years_experience" in df.columns:
        df["years_experience"] = pd.to_numeric(df["years_experience"], errors="coerce")

    if "compensation_expectation" in df.columns:
        df["compensation_expectation"] = pd.to_numeric(df["compensation_expectation"], errors="coerce")

    return df, report

def clean_events(df: pd.DataFrame) -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    report = {}

    # trim text
    for col in ["stage_name", "outcome", "owner"]:
        if col in df.columns:
            df[col] = df[col].astype("string").fillna("").str.strip()

    # identify invalid stages (keep them in a separate table)
    invalid_mask = ~df["stage_name"].isin(VALID_STAGES)
    invalid_df = df.loc[invalid_mask].copy()
    report["invalid_stage_events"] = int(invalid_mask.sum())

    # keep only valid stages in clean events
    clean_df = df.loc[~invalid_mask].copy()

    return clean_df, report, invalid_df

def main():
    candidates = pd.read_csv(RAW_DIR / "candidates_mock.csv")
    events = pd.read_csv(RAW_DIR / "pipeline_events_mock.csv")

    candidates_clean, rep_c = clean_candidates(candidates)
    events_clean, rep_e, invalid_events = clean_events(events)

    candidates_path = OUT_DIR / "candidates_clean.csv"
    events_path = OUT_DIR / "pipeline_events_clean.csv"
    invalid_path = OUT_DIR / "invalid_stage_events.csv"

    candidates_clean.to_csv(candidates_path, index=False)
    events_clean.to_csv(events_path, index=False)
    invalid_events.to_csv(invalid_path, index=False)

    print("Wrote:", candidates_path, "rows=", len(candidates_clean))
    print("Wrote:", events_path, "rows=", len(events_clean))
    print("Wrote:", invalid_path, "rows=", len(invalid_events))
    print("Report:", {**rep_c, **rep_e})

if __name__ == "__main__":
    main()
