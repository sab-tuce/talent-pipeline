from pathlib import Path
import subprocess
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def run(cmd: list[str]) -> None:
    print("\n$", " ".join(cmd))
    r = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if r.returncode != 0:
        raise SystemExit(r.returncode)

def main():
    # Re-generate all processed outputs from the current DB
    run([sys.executable, "scripts/export_stage_reports.py"])
    run([sys.executable, "scripts/export_hygiene_report.py"])
    run([sys.executable, "-m", "scripts.compute_kpis"])
    run([sys.executable, "scripts/export_role_funnel.py"])
    run([sys.executable, "scripts/export_weekly_metrics.py"])

    print("\nAll reports exported to data/processed/ ✅")

if __name__ == "__main__":
    main()
