"""
run_pipeline.py — GSF Semantic Pipeline Orchestrator

Runs any combination of the seven pipeline phases in order.

Usage:
    python run_pipeline.py                        # phases 1 3 4 5 6 (default)
    python run_pipeline.py --phases 3 4 5 6       # skip generation
    python run_pipeline.py --source s3            # Bronze from S3 external stage
    python run_pipeline.py --dry-run              # Phase 6 without Cortex API calls
    python run_pipeline.py --phases 1 2 3 4 5 6 7 --launch-app  # full run + Streamlit

Phases:
    1  Generate seed data        (generator_v2)
    2  Deliver to S3             (delivery/deliver.py — requires AWS credentials)
    3  Load Bronze               (pipeline_naive/load_bronze.py)
    4  dbt transforms            (dbt seed + dbt run + dbt test)
    5  Stage semantic YAMLs      (pipeline_semantic/load_gold.py)
    6  Variance comparison       (variance/runner.py)
    7  Launch Streamlit app      (app/streamlit_app.py — omit for instructions only)
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.resolve()
DBT_DIR = PROJECT_ROOT / "dbt"

DEFAULT_PHASES = [1, 3, 4, 5, 6]

PHASE_LABELS = {
    1: "Generate Seed Data",
    2: "S3 Delivery",
    3: "Bronze Ingest",
    4: "dbt Transforms (Silver → Naive Gold + Semantic Gold)",
    5: "Stage Cortex Analyst YAML Files",
    6: "Variance Comparison",
    7: "Streamlit App",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _banner(phase_num: int, detail: str = "") -> None:
    label = PHASE_LABELS[phase_num]
    suffix = f" ({detail})" if detail else ""
    print()
    print("=" * 62)
    print(f"  PHASE {phase_num}: {label}{suffix}")
    print("=" * 62)


def run_phase(phase_num: int, cmd: list, cwd: Path = None, detail: str = "") -> None:
    """Print a phase banner, run cmd as a subprocess, abort on failure."""
    _banner(phase_num, detail)
    t0 = time.time()
    result = subprocess.run(cmd, cwd=cwd)
    elapsed = time.time() - t0
    if result.returncode != 0:
        print()
        print(f"  [ABORT] Phase {phase_num} failed (exit code {result.returncode})")
        print(f"  Fix the issue above and re-run with --phases {phase_num}+")
        sys.exit(1)
    print(f"  Phase {phase_num} complete ({elapsed:.1f}s)")


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

def _preflight_phase2() -> None:
    """Abort with a clear message if AWS credentials are not configured."""
    has_env = bool(os.environ.get("AWS_ACCESS_KEY_ID"))
    has_file = (Path.home() / ".aws" / "credentials").exists()
    if not has_env and not has_file:
        print()
        print("  [ABORT] Phase 2 requires AWS credentials.")
        print("  Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in .env,")
        print("  or configure ~/.aws/credentials via `aws configure`.")
        sys.exit(1)


def _preflight_phase3(source: str, data_dir: str) -> None:
    """Abort if local seed CSVs are missing when --source local."""
    if source != "local":
        return
    seed_dir = PROJECT_ROOT / data_dir
    expected = seed_dir / "positions_topaz.csv"
    if not expected.exists():
        print()
        print(f"  [ABORT] Phase 3 (local) requires seed CSVs in {seed_dir}")
        print("  Run Phase 1 first:  python run_pipeline.py --phases 1")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_pipeline.py",
        description="GSF Semantic Pipeline — end-to-end orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--phases",
        type=int,
        nargs="+",
        default=DEFAULT_PHASES,
        metavar="N",
        help=(
            "Phases to run (1-7). Default: 1 3 4 5 6. "
            "Phase 2 requires AWS credentials. "
            "Phase 7 prints launch instructions unless --launch-app is set."
        ),
    )
    parser.add_argument(
        "--source",
        choices=["local", "s3"],
        default="local",
        help="Phase 3 Bronze source: local files (default) or S3 external stage",
    )
    parser.add_argument(
        "--data-dir",
        default="data/seed_v2",
        help="Seed CSV directory for Phases 1, 2, 3 (default: data/seed_v2)",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="S3 bucket for Phase 2 (default: uses delivery/config.py default)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Phase 6: print ground truth only, skip Cortex Analyst API calls",
    )
    parser.add_argument(
        "--launch-app",
        action="store_true",
        help="Phase 7: launch Streamlit (blocking). Without this flag, prints instructions.",
    )
    return parser


# ---------------------------------------------------------------------------
# Phase dispatch
# ---------------------------------------------------------------------------

def phase_1(data_dir: str) -> None:
    run_phase(
        1,
        cmd=[sys.executable, "-m", "generator_v2.generator", "--validate"],
        cwd=PROJECT_ROOT,
    )


def phase_2(data_dir: str, bucket: str | None) -> None:
    _preflight_phase2()
    cmd = [sys.executable, str(PROJECT_ROOT / "delivery" / "deliver.py"),
           "--data-dir", data_dir]
    if bucket:
        cmd += ["--bucket", bucket]
    run_phase(2, cmd=cmd, cwd=PROJECT_ROOT)


def phase_3(source: str, data_dir: str) -> None:
    _preflight_phase3(source, data_dir)
    run_phase(
        3,
        cmd=[sys.executable, str(PROJECT_ROOT / "pipeline_naive" / "load_bronze.py"),
             "--source", source, "--data-dir", data_dir],
        cwd=PROJECT_ROOT,
        detail=f"source={source}",
    )


def phase_4() -> None:
    """Run dbt seed → dbt run → dbt test from the dbt/ subdirectory."""
    if not DBT_DIR.exists():
        print()
        print(f"  [ABORT] dbt project directory not found: {DBT_DIR}")
        sys.exit(1)

    _banner(4)
    t0 = time.time()

    for sub_cmd in [["dbt", "seed"], ["dbt", "run"], ["dbt", "test"]]:
        print(f"\n  Running: {' '.join(sub_cmd)}")
        result = subprocess.run(sub_cmd, cwd=DBT_DIR)
        if result.returncode != 0:
            print()
            print(f"  [ABORT] Phase 4 failed at `{' '.join(sub_cmd)}` "
                  f"(exit code {result.returncode})")
            print("  Fix the issue above and re-run with --phases 4+")
            sys.exit(1)

    elapsed = time.time() - t0
    print(f"\n  Phase 4 complete ({elapsed:.1f}s)")


def phase_5() -> None:
    run_phase(
        5,
        cmd=[sys.executable, str(PROJECT_ROOT / "pipeline_semantic" / "load_gold.py")],
        cwd=PROJECT_ROOT,
    )


def phase_6(dry_run: bool) -> None:
    cmd = [sys.executable, str(PROJECT_ROOT / "variance" / "runner.py")]
    if dry_run:
        cmd.append("--dry-run")
    run_phase(6, cmd=cmd, cwd=PROJECT_ROOT, detail="dry-run" if dry_run else "")


def phase_7(launch_app: bool) -> None:
    _banner(7)
    if launch_app:
        print("  Launching Streamlit app (Ctrl+C to stop)...")
        subprocess.run(
            ["streamlit", "run", str(PROJECT_ROOT / "app" / "streamlit_app.py")],
            cwd=PROJECT_ROOT,
        )
    else:
        results_dir = PROJECT_ROOT / "variance" / "results"
        latest = ""
        if results_dir.exists():
            jsons = sorted(results_dir.glob("*.json"))
            if jsons:
                latest = f"\n  Results: {jsons[-1].relative_to(PROJECT_ROOT)}"
        print()
        print("  Pipeline complete. To visualize results:")
        print()
        print("    streamlit run app/streamlit_app.py")
        if latest:
            print(latest)
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Validate phase numbers
    invalid = [p for p in args.phases if p not in range(1, 8)]
    if invalid:
        parser.error(f"Invalid phase(s): {invalid}. Phases must be 1-7.")

    phases = sorted(set(args.phases))

    print()
    print("  GSF Semantic Pipeline")
    print(f"  Phases: {phases}")
    print(f"  Source: {args.source}  |  Dry-run: {args.dry_run}")

    dispatch = {
        1: lambda: phase_1(args.data_dir),
        2: lambda: phase_2(args.data_dir, args.bucket),
        3: lambda: phase_3(args.source, args.data_dir),
        4: phase_4,
        5: phase_5,
        6: lambda: phase_6(args.dry_run),
        7: lambda: phase_7(args.launch_app),
    }

    try:
        for p in phases:
            dispatch[p]()
    except KeyboardInterrupt:
        print("\n\n  [INTERRUPTED] Pipeline stopped by user.")
        sys.exit(130)


if __name__ == "__main__":
    main()
