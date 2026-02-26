from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import random

import pandas as pd
from faker import Faker

from scripts.constants import SOURCE_CHANNELS, OWNERS, LOCATIONS


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


@dataclass
class RoleSpec:
    role_name: str
    hiring_manager: str
    priority: str
    headcount: int
    status: str
    open_offset_days: int
    close_offset_days: int


def generate_roles(seed: int = 42) -> pd.DataFrame:
    random.seed(seed)

    role_specs = [
        RoleSpec("Computer Vision Engineer", "HM-1", "High", 1, "Open", 35, 70),
        RoleSpec("ML Engineer (Edge)", "HM-1", "High", 1, "Open", 28, 60),
        RoleSpec("Solutions Engineer", "HM-2", "Medium", 1, "Open", 21, 55),
        RoleSpec("BizOps Analyst", "HM-2", "High", 1, "Open", 14, 45),
        RoleSpec("Deployment Engineer", "HM-1", "Medium", 1, "Hold", 10, 40),
    ]

    today = date.today()
    rows = []
    for i, spec in enumerate(role_specs, start=1):
        open_date = today - timedelta(days=spec.open_offset_days)
        target_close = today + timedelta(days=spec.close_offset_days)
        rows.append(
            {
                "role_id": i,
                "role_name": spec.role_name,
                "hiring_manager": spec.hiring_manager,
                "priority": spec.priority,
                "open_date": open_date.isoformat(),
                "target_close_date": target_close.isoformat(),
                "headcount": spec.headcount,
                "status": spec.status,
            }
        )

    return pd.DataFrame(rows)


def generate_candidates(
    roles_df: pd.DataFrame,
    n_candidates: int = 120,
    seed: int = 42,
) -> pd.DataFrame:
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)

    role_names = roles_df["role_name"].tolist()

    # simple realistic skill pools by role
    skill_bank = {
        "Computer Vision Engineer": ["OpenCV", "PyTorch", "Computer Vision", "CNN", "Docker", "Linux", "CUDA"],
        "ML Engineer (Edge)": ["TensorRT", "ONNX", "PyTorch", "Model Optimization", "C++", "Edge Deployment", "Linux"],
        "Solutions Engineer": ["Customer Facing", "Python", "APIs", "Troubleshooting", "SQL", "Systems Thinking", "Communication"],
        "BizOps Analyst": ["SQL", "Excel", "Power BI", "KPI Reporting", "Process Improvement", "Stakeholder Management", "Automation"],
        "Deployment Engineer": ["Docker", "Kubernetes", "CI/CD", "Linux", "Networking", "Monitoring", "Incident Response"],
    }

    companies = ["Sephora", "Under Armour", "Petco", "Shopify", "Amazon", "Google", "Microsoft", "Stripe", "Uber", "Intel"]
    stages = ["Sourced", "Contacted", "Recruiter Screen", "Hiring Manager", "Panel", "Offer"]

    today = datetime.now()

    rows = []
    for cid in range(1, n_candidates + 1):
        target_role = random.choice(role_names)
        full_name = fake.name()

        years_exp = max(0, int(random.gauss(4.5, 2.0)))  # around 2–7 typical
        source = random.choices(SOURCE_CHANNELS, weights=[45, 15, 20, 20], k=1)[0]
        location = random.choice(LOCATIONS)
        current_company = random.choice(companies)

        # compensation (synthetic)
        base = 90000 + years_exp * 7000 + random.randint(-8000, 12000)
        comp = int(max(65000, min(base, 220000)))

        # stage + applied_at
        current_stage = random.choices(stages, weights=[30, 20, 20, 15, 10, 5], k=1)[0]
        applied_days_ago = random.randint(0, 45)
        applied_at = (today - timedelta(days=applied_days_ago, hours=random.randint(0, 23))).isoformat(timespec="seconds")

        # owner sometimes missing (for hygiene checks later)
        owner = random.choice(OWNERS)
        if random.random() < 0.08:
            owner = ""  # intentional missing owner

        # role sometimes missing (rare, intentional)
        if random.random() < 0.03:
            target_role = ""

        # stage sometimes missing (rare, intentional)
        if random.random() < 0.02:
            current_stage = ""

        skills = skill_bank.get(target_role, ["Python", "SQL", "Communication"])
        skills_text = ", ".join(random.sample(skills, k=min(5, len(skills))))

        rows.append(
            {
                "candidate_id": cid,
                "full_name": full_name,
                "target_role": target_role,
                "current_company": current_company,
                "location": location,
                "years_experience": years_exp,
                "source_channel": source,
                "skills_text": skills_text,
                "compensation_expectation": comp,
                "current_stage": current_stage,
                "owner": owner,
                "applied_at": applied_at,
            }
        )

    df = pd.DataFrame(rows)

    # Add a few intentional duplicates (same name+company+role)
    dup_n = 5
    dup_rows = df.sample(dup_n, random_state=seed).copy()
    dup_rows["candidate_id"] = range(n_candidates + 1, n_candidates + dup_n + 1)
    df = pd.concat([df, dup_rows], ignore_index=True)

    return df

def generate_pipeline_events(candidates_df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """
    Creates a realistic stage history per candidate.
    Each candidate gets multiple rows (events) with entered/exited timestamps.
    """
    random.seed(seed)

    stage_flow = [
        "Sourced",
        "Contacted",
        "Recruiter Screen",
        "Hiring Manager",
        "Panel",
        "Offer",
    ]
    terminal_outcomes = ["Hired", "Rejected", "Withdrawn"]

    rows = []
    event_id = 1
    now = datetime.now()

    for _, c in candidates_df.iterrows():
        cid = int(c["candidate_id"])
        owner = c.get("owner", "")
        applied_at = pd.to_datetime(c["applied_at"], errors="coerce")

        # start date around applied_at
        start_dt = applied_at if pd.notna(applied_at) else (now - timedelta(days=random.randint(5, 30)))

        # how far this candidate progresses in the funnel
        max_stage_index = random.choices(
            population=list(range(0, len(stage_flow))),
            weights=[25, 20, 18, 15, 12, 10],
            k=1,
        )[0]

        current_dt = start_dt

        # create events through stages up to max_stage_index
        for i in range(0, max_stage_index + 1):
            stage = stage_flow[i]
            entered = current_dt

            # duration in stage (1 to 6 days typical)
            duration_days = max(0, int(random.gauss(2.5, 1.5)))
            # sometimes intentionally stale (for hygiene/KPI)
            if random.random() < 0.08:
                duration_days += random.randint(7, 14)

            exited = entered + timedelta(days=duration_days, hours=random.randint(0, 20))

            rows.append(
                {
                    "event_id": event_id,
                    "candidate_id": cid,
                    "stage_name": stage,
                    "entered_stage_at": entered.isoformat(timespec="seconds"),
                    "exited_stage_at": exited.isoformat(timespec="seconds"),
                    "outcome": "",
                    "owner": owner,
                }
            )
            event_id += 1
            current_dt = exited

        offer_idx = stage_flow.index("Offer")

        # If candidate did NOT reach Offer, they cannot be Hired (realistic ATS logic)
        if max_stage_index < offer_idx:
            outcome = random.choices(["Rejected", "Withdrawn"], weights=[80, 20], k=1)[0]
        else:
            # If they reached Offer, allow Hired as an outcome
            outcome = random.choices(["Hired", "Rejected", "Withdrawn"], weights=[55, 30, 15], k=1)[0]

        rows.append(
            {
                "event_id": event_id,
                "candidate_id": cid,
                "stage_name": outcome,
                "entered_stage_at": current_dt.isoformat(timespec="seconds"),
                "exited_stage_at": "",
                "outcome": outcome,
                "owner": owner,
            }
        )
        event_id += 1

    df = pd.DataFrame(rows)

    # Intentional data issues (for hygiene checks):
    # 1) Some invalid stage labels
    if len(df) > 0:
        bad_idx = df.sample(frac=0.01, random_state=seed).index
        df.loc[bad_idx, "stage_name"] = "InvalidStage"

    # 2) Some date issues: exited before entered
    if len(df) > 0:
        bad_idx2 = df.sample(frac=0.01, random_state=seed + 1).index
        df.loc[bad_idx2, "exited_stage_at"] = df.loc[bad_idx2, "entered_stage_at"]

    return df

def sync_current_stage_from_events(candidates_df: pd.DataFrame, pipeline_events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Set candidates.current_stage to the latest stage in pipeline_events (per candidate).
    Latest is determined by entered_stage_at timestamp.
    """
    ev = pipeline_events_df.copy()
    ev["entered_stage_at"] = pd.to_datetime(ev["entered_stage_at"], errors="coerce")

    latest = (
        ev.sort_values(["candidate_id", "entered_stage_at"])
          .groupby("candidate_id", as_index=False)
          .tail(1)[["candidate_id", "stage_name"]]
          .rename(columns={"stage_name": "current_stage_from_events"})
    )

    out = candidates_df.merge(latest, on="candidate_id", how="left")
    out["current_stage"] = out["current_stage_from_events"].fillna(out["current_stage"])
    out = out.drop(columns=["current_stage_from_events"])
    return out

def generate_scorecards(pipeline_events_df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    random.seed(seed)
    fake = Faker()
    Faker.seed(seed)

    competencies = [
        "Communication",
        "Problem Solving",
        "Ownership",
        "Systems Thinking",
        "Execution",
    ]

    panel_cids = (
        pipeline_events_df.loc[pipeline_events_df["stage_name"] == "Panel", "candidate_id"]
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )

    rows = []
    scorecard_id = 1
    now = datetime.now()

    for cid in panel_cids:
        # intentionally missing scorecards for some candidates
        if random.random() < 0.18:
            continue

        n_interviewers = random.randint(2, 4)
        for _ in range(n_interviewers):
            interviewer = fake.name()
            submitted_at = now - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))

            comp_list = competencies.copy()
            # intentionally partial scorecards sometimes
            if random.random() < 0.12:
                comp_list.pop(random.randrange(len(comp_list)))

            for comp in comp_list:
                rows.append(
                    {
                        "scorecard_id": scorecard_id,
                        "candidate_id": cid,
                        "interviewer_name": interviewer,
                        "interview_stage": "Panel",
                        "competency": comp,
                        "score": random.randint(1, 5),
                        "notes": fake.sentence(nb_words=10),
                        "submitted_at": submitted_at.isoformat(timespec="seconds"),
                    }
                )
                scorecard_id += 1

    return pd.DataFrame(rows)

def generate_talent_market_map(seed: int = 42) -> pd.DataFrame:
    random.seed(seed)

    rows = [
        {"company": "Shopify", "role_family": "Data/Analytics", "location": "Remote (Canada)", "est_comp_low": 90000, "est_comp_high": 140000, "hiring_signal": "Medium", "notes": "Growth team hiring"},
        {"company": "Amazon", "role_family": "ML/AI", "location": "Vancouver, BC", "est_comp_low": 120000, "est_comp_high": 190000, "hiring_signal": "High", "notes": "Multiple openings"},
        {"company": "Microsoft", "role_family": "Solutions/Engineering", "location": "Toronto, ON", "est_comp_low": 110000, "est_comp_high": 175000, "hiring_signal": "Medium", "notes": "Backfills + new reqs"},
        {"company": "Stripe", "role_family": "Engineering", "location": "Remote (Canada)", "est_comp_low": 130000, "est_comp_high": 210000, "hiring_signal": "Low", "notes": "Selective hiring"},
        {"company": "Uber", "role_family": "Engineering", "location": "Toronto, ON", "est_comp_low": 120000, "est_comp_high": 200000, "hiring_signal": "Medium", "notes": "Platform roles"},
        {"company": "Sephora", "role_family": "BizOps", "location": "Vancouver, BC", "est_comp_low": 80000, "est_comp_high": 125000, "hiring_signal": "Low", "notes": "Occasional openings"},
        {"company": "Under Armour", "role_family": "Analytics", "location": "Remote (Canada)", "est_comp_low": 85000, "est_comp_high": 130000, "hiring_signal": "Low", "notes": "Project-based hiring"},
        {"company": "Petco", "role_family": "Operations", "location": "Remote (Canada)", "est_comp_low": 75000, "est_comp_high": 115000, "hiring_signal": "Medium", "notes": "Ops analytics push"},
    ]

    return pd.DataFrame(rows)

def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    roles_df = generate_roles(seed=42)
    roles_df.to_csv(RAW_DIR / "roles_mock.csv", index=False)

    candidates_df = generate_candidates(roles_df, n_candidates=120, seed=42)
    candidates_df.to_csv(RAW_DIR / "candidates_mock.csv", index=False)

    print("Wrote:", RAW_DIR / "roles_mock.csv")
    print("Wrote:", RAW_DIR / "candidates_mock.csv")
    print("\nroles preview:")
    print(roles_df.head(10).to_string(index=False))
    print("\ncandidates preview:")
    print(candidates_df.head(10).to_string(index=False))

    pipeline_events_df = generate_pipeline_events(candidates_df, seed=42)
    pipeline_events_df.to_csv(RAW_DIR / "pipeline_events_mock.csv", index=False)
    print("Wrote:", RAW_DIR / "pipeline_events_mock.csv")

    candidates_df = sync_current_stage_from_events(candidates_df, pipeline_events_df)
    candidates_df.to_csv(RAW_DIR / "candidates_mock.csv", index=False)
    print("Synced + rewrote:", RAW_DIR / "candidates_mock.csv")

    scorecards_df = generate_scorecards(pipeline_events_df, seed=42)
    scorecards_df.to_csv(RAW_DIR / "scorecards_mock.csv", index=False)
    print("Wrote:", RAW_DIR / "scorecards_mock.csv")

    market_df = generate_talent_market_map(seed=42)
    market_df.to_csv(RAW_DIR / "talent_market_map_mock.csv", index=False)
    print("Wrote:", RAW_DIR / "talent_market_map_mock.csv")

if __name__ == "__main__":
    main()
