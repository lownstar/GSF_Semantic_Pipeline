"""
Seed data generator — main entrypoint.

Usage:
    python generator/generator.py
    python generator/generator.py --output-dir data/seed --seed 42 --validate

Generates ~102k rows of synthetic financial portfolio data across 9 CSV files.
The output is fully deterministic for a given seed value.
"""

import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

# Allow running from project root or from inside generator/
sys.path.insert(0, str(Path(__file__).parent.parent))

from generator import config
from generator.models.dimensions import (
    generate_dim_date,
    generate_dim_asset_class,
    generate_dim_benchmark,
    generate_dim_security,
    generate_dim_account,
)
from generator.models.facts import (
    generate_fact_benchmark_return,
    generate_fact_position,
    generate_fact_transaction,
    generate_fact_return,
)
from generator.utils.date_utils import get_month_end_dates, get_business_days


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic financial seed data")
    parser.add_argument("--output-dir", default=config.OUTPUT_DIR,
                        help=f"Output directory for CSV files (default: {config.OUTPUT_DIR})")
    parser.add_argument("--seed", type=int, default=config.RANDOM_SEED,
                        help=f"Random seed for reproducibility (default: {config.RANDOM_SEED})")
    parser.add_argument("--validate", action="store_true",
                        help="Run post-generation validation checks")
    return parser.parse_args()


def validate(tables: dict[str, pd.DataFrame]) -> list[str]:
    """Run spot-check validations. Returns list of failure messages (empty = all pass)."""
    failures = []

    # FK checks
    acc_ids = set(tables["dim_account"]["account_id"])
    sec_ids = set(tables["dim_security"]["security_id"])
    bm_ids  = set(tables["dim_benchmark"]["benchmark_id"])
    ac_ids  = set(tables["dim_asset_class"]["asset_class_id"])

    for col, parent in [("account_id", acc_ids), ("security_id", sec_ids)]:
        if col in tables["fact_position"].columns:
            bad = set(tables["fact_position"][col]) - parent
            if bad:
                failures.append(f"fact_position.{col}: {len(bad)} unknown values")

    for col, parent in [("account_id", acc_ids), ("security_id", sec_ids)]:
        if col in tables["fact_transaction"].columns:
            bad = set(tables["fact_transaction"][col]) - parent
            if bad:
                failures.append(f"fact_transaction.{col}: {len(bad)} unknown values")

    if set(tables["dim_account"]["benchmark_id"]) - bm_ids:
        failures.append("dim_account: unknown benchmark_id values")
    if set(tables["dim_security"]["asset_class_id"]) - ac_ids:
        failures.append("dim_security: unknown asset_class_id values")

    # Settlement date must be >= trade date
    tx = tables["fact_transaction"]
    bad_settlement = tx[pd.to_datetime(tx["settlement_date"]) < pd.to_datetime(tx["trade_date"])]
    if not bad_settlement.empty:
        failures.append(f"fact_transaction: {len(bad_settlement)} rows where settlement_date < trade_date")

    # Net return < gross return for all fact_return rows
    fr = tables["fact_return"]
    bad_return = fr[fr["return_net"] > fr["return_gross"] + 0.0001]
    if not bad_return.empty:
        failures.append(f"fact_return: {len(bad_return)} rows where return_net > return_gross (expected fee drag)")

    # Each account should have at least one YTD return row
    ytd_accounts = set(fr[fr["return_period"] == "YTD"]["account_id"])
    missing_ytd = acc_ids - ytd_accounts
    if missing_ytd:
        failures.append(f"fact_return: {len(missing_ytd)} accounts missing YTD return rows")

    return failures


def write_csv(df: pd.DataFrame, path: Path, table_name: str) -> None:
    df.to_csv(path, index=False)
    size_kb = path.stat().st_size / 1024
    print(f"  {table_name:<30} {len(df):>8,} rows  ->  {path}  ({size_kb:,.0f} KB)")


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rng   = np.random.default_rng(args.seed)
    faker = Faker()
    Faker.seed(args.seed)

    total_start = time.time()
    print(f"\nGenerating seed data (seed={args.seed}) ...\n")

    # ── Dimensions ──────────────────────────────────────────────────────────
    t = time.time()
    print("Generating dimensions...")
    dim_date        = generate_dim_date(config.DATE_START, config.DATE_END)
    dim_asset_class = generate_dim_asset_class()
    dim_benchmark   = generate_dim_benchmark()
    dim_security    = generate_dim_security(rng, faker, dim_asset_class, dim_benchmark)
    dim_account     = generate_dim_account(rng, faker, dim_benchmark)
    print(f"  Done in {time.time() - t:.1f}s\n")

    # ── Date helpers ─────────────────────────────────────────────────────────
    month_end_dates = get_month_end_dates(config.DATE_START, config.DATE_END)
    business_days   = get_business_days(config.DATE_START, config.DATE_END)

    # ── Facts ────────────────────────────────────────────────────────────────
    t = time.time()
    print("Generating fact_benchmark_return...")
    fact_bm_return = generate_fact_benchmark_return(dim_benchmark, month_end_dates, rng)
    print(f"  Done in {time.time() - t:.1f}s\n")

    t = time.time()
    print("Generating fact_position (largest table — may take ~30s)...")
    fact_position = generate_fact_position(dim_account, dim_security, month_end_dates, rng)
    print(f"  Done in {time.time() - t:.1f}s\n")

    t = time.time()
    print("Generating fact_transaction...")
    fact_transaction = generate_fact_transaction(dim_account, dim_security, business_days, rng)
    print(f"  Done in {time.time() - t:.1f}s\n")

    t = time.time()
    print("Generating fact_return (may take ~60s)...")
    fact_return = generate_fact_return(dim_account, month_end_dates, fact_bm_return, rng)
    print(f"  Done in {time.time() - t:.1f}s\n")

    # ── Write CSVs ───────────────────────────────────────────────────────────
    tables = {
        "dim_date":              dim_date,
        "dim_asset_class":       dim_asset_class,
        "dim_benchmark":         dim_benchmark,
        "dim_security":          dim_security,
        "dim_account":           dim_account,
        "fact_benchmark_return": fact_bm_return,
        "fact_position":         fact_position,
        "fact_transaction":      fact_transaction,
        "fact_return":           fact_return,
    }

    print("Writing CSV files...\n")
    for name, df in tables.items():
        write_csv(df, output_dir / f"{name}.csv", name)

    total_rows = sum(len(df) for df in tables.values())
    print(f"\n{'-' * 60}")
    print(f"  Total rows generated: {total_rows:,}")
    print(f"  Total time:           {time.time() - total_start:.1f}s")
    print(f"  Output directory:     {output_dir.resolve()}")

    # ── Validation ───────────────────────────────────────────────────────────
    if args.validate:
        print(f"\nRunning validation checks...")
        failures = validate(tables)
        if failures:
            print("  FAILED:")
            for f in failures:
                print(f"    ✗ {f}")
            sys.exit(1)
        else:
            print("  All checks passed.")

    print()


if __name__ == "__main__":
    main()
