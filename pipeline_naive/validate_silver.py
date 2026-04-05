"""
Naive Pipeline — Silver Validator
==================================
Compares SILVER.POSITIONS_INTEGRATED in Snowflake against the reference CSV
(data/seed_v2/positions_integrated.csv) to confirm the ETL produced the
expected output.

Checks:
  SC1  Total row count = 22,160 (12,388 Topaz + 4,886 Emerald + 4,886 Ruby)
  SC2  Row count by source_system matches reference CSV breakdown
  SC3  NULL security_master_id fraction ≈ 15% (Ambiguity A8)
  SC4  NULL unrealized_gl fraction ≈ 22% — all Ruby rows (Ambiguity A11)
  SC5  All source_system values are exactly: TOPAZ, EMERALD, RUBY
  SC6  NULL asset_class fraction matches NULL security_master_id fraction (A10)
  SC7  Topaz record_ids are all LOT-* prefixed
  SC8  Emerald record_ids are all POS-* prefixed
  SC9  Ruby record_ids are all NAV-* prefixed

Usage:
  python pipeline_naive/validate_silver.py [--data-dir data/seed_v2]
"""

import argparse
import os
import sys

import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_der_private_key
from dotenv import load_dotenv

load_dotenv()


def get_connection() -> snowflake.connector.SnowflakeConnection:
    account  = os.environ.get("SNOWFLAKE_ACCOUNT")
    user     = os.environ.get("SNOWFLAKE_USER")
    key_file = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")

    if not account or not user:
        print("ERROR: SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER are required")
        sys.exit(1)

    connect_kwargs = dict(
        account   = account,
        user      = user,
        role      = os.getenv("SNOWFLAKE_ROLE",      "GSF_ROLE"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "GSF_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE",  "GSF_DEMO"),
    )

    if key_file:
        with open(key_file, "rb") as f:
            private_key = load_der_private_key(f.read(), password=None)
        connect_kwargs["private_key"] = private_key
    else:
        pw = os.getenv("SNOWFLAKE_PASSWORD")
        if not pw:
            print("ERROR: set SNOWFLAKE_PRIVATE_KEY_FILE or SNOWFLAKE_PASSWORD in .env")
            sys.exit(1)
        connect_kwargs["password"] = pw

    return snowflake.connector.connect(**connect_kwargs)


def fetch_scalar(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


def run(data_dir: str) -> None:
    ref_path = os.path.join(data_dir, "positions_integrated.csv")
    if not os.path.exists(ref_path):
        print(f"ERROR: reference CSV not found: {ref_path}")
        print("  Run: python -m generator_v2.generator")
        sys.exit(1)

    print(f"\nNaive Pipeline — Silver Validation")
    print(f"Reference: {ref_path}")
    print(f"Target:    SILVER.POSITIONS_INTEGRATED\n")

    ref = pd.read_csv(ref_path)

    conn = get_connection()
    cur  = conn.cursor()

    errors = []

    try:
        cur.execute("USE ROLE GSF_ROLE")
        cur.execute("USE WAREHOUSE GSF_WH")
        cur.execute("USE DATABASE GSF_DEMO")

        # ── SC1: Total row count ──────────────────────────────────────────────
        total_sf  = fetch_scalar(cur, "SELECT COUNT(*) FROM SILVER.POSITIONS_INTEGRATED")
        total_ref = len(ref)
        if total_sf == total_ref:
            print(f"  OK   SC1  Total rows: {total_sf:,}")
        else:
            errors.append(f"SC1 FAIL: Snowflake={total_sf:,}, reference={total_ref:,}")
            print(f"  FAIL SC1  Total rows: Snowflake={total_sf:,}, reference={total_ref:,}")

        # ── SC2: Row count by source_system ───────────────────────────────────
        cur.execute("""
            SELECT SOURCE_SYSTEM, COUNT(*)
            FROM SILVER.POSITIONS_INTEGRATED
            GROUP BY SOURCE_SYSTEM
            ORDER BY SOURCE_SYSTEM
        """)
        sf_by_source = {row[0]: row[1] for row in cur.fetchall()}
        ref_by_source = ref.groupby("source_system").size().to_dict()

        for src in ["TOPAZ", "EMERALD", "RUBY"]:
            sf_n  = sf_by_source.get(src, 0)
            ref_n = ref_by_source.get(src, 0)
            if sf_n == ref_n:
                print(f"  OK   SC2  {src} rows: {sf_n:,}")
            else:
                errors.append(f"SC2 FAIL {src}: Snowflake={sf_n:,}, reference={ref_n:,}")
                print(f"  FAIL SC2  {src}: Snowflake={sf_n:,}, reference={ref_n:,}")

        # ── SC3: NULL security_master_id fraction ≈ 15% ───────────────────────
        null_master = fetch_scalar(
            cur,
            "SELECT COUNT(*) FROM SILVER.POSITIONS_INTEGRATED WHERE SECURITY_MASTER_ID IS NULL"
        )
        null_frac = null_master / total_sf if total_sf else 0
        tolerance = 0.05
        ref_null_frac = ref["security_master_id"].isna().mean()
        if abs(null_frac - ref_null_frac) <= tolerance:
            print(f"  OK   SC3  NULL security_master_id: {null_frac:.1%} (ref={ref_null_frac:.1%})")
        else:
            errors.append(f"SC3 FAIL: NULL security_master_id={null_frac:.1%}, ref={ref_null_frac:.1%}")
            print(f"  FAIL SC3  NULL security_master_id: {null_frac:.1%} (ref={ref_null_frac:.1%})")

        # ── SC4: NULL unrealized_gl ≈ 22% (all Ruby rows) ────────────────────
        null_gl = fetch_scalar(
            cur,
            "SELECT COUNT(*) FROM SILVER.POSITIONS_INTEGRATED WHERE UNREALIZED_GL IS NULL"
        )
        null_gl_frac = null_gl / total_sf if total_sf else 0
        ref_gl_frac  = ref["unrealized_gl"].isna().mean()
        if abs(null_gl_frac - ref_gl_frac) <= tolerance:
            print(f"  OK   SC4  NULL unrealized_gl: {null_gl_frac:.1%} (ref={ref_gl_frac:.1%})")
        else:
            errors.append(f"SC4 FAIL: NULL unrealized_gl={null_gl_frac:.1%}, ref={ref_gl_frac:.1%}")
            print(f"  FAIL SC4  NULL unrealized_gl: {null_gl_frac:.1%} (ref={ref_gl_frac:.1%})")

        # ── SC5: source_system values are exactly TOPAZ / EMERALD / RUBY ──────
        cur.execute(
            "SELECT DISTINCT SOURCE_SYSTEM FROM SILVER.POSITIONS_INTEGRATED ORDER BY 1"
        )
        sources = {row[0] for row in cur.fetchall()}
        expected_sources = {"TOPAZ", "EMERALD", "RUBY"}
        if sources == expected_sources:
            print(f"  OK   SC5  source_system values: {sorted(sources)}")
        else:
            errors.append(f"SC5 FAIL: source_system values={sources}, expected={expected_sources}")
            print(f"  FAIL SC5  source_system values: {sources}")

        # ── SC6: NULL asset_class fraction matches NULL security_master_id ─────
        null_ac = fetch_scalar(
            cur,
            "SELECT COUNT(*) FROM SILVER.POSITIONS_INTEGRATED WHERE ASSET_CLASS IS NULL"
        )
        if null_ac == null_master:
            print(f"  OK   SC6  NULL asset_class rows = NULL security_master_id rows ({null_ac:,})")
        else:
            errors.append(
                f"SC6 FAIL: NULL asset_class={null_ac:,}, NULL security_master_id={null_master:,} (should match)"
            )
            print(f"  FAIL SC6  NULL asset_class={null_ac:,} ≠ NULL security_master_id={null_master:,}")

        # ── SC7–SC9: record_id prefix conventions ─────────────────────────────
        for source, prefix, check in [
            ("TOPAZ",   "LOT-", "SC7"),
            ("EMERALD", "POS-", "SC8"),
            ("RUBY",    "NAV-", "SC9"),
        ]:
            cur.execute(f"""
                SELECT COUNT(*)
                FROM SILVER.POSITIONS_INTEGRATED
                WHERE SOURCE_SYSTEM = '{source}'
                  AND RECORD_ID NOT LIKE '{prefix}%'
            """)
            bad = cur.fetchone()[0]
            if bad == 0:
                print(f"  OK   {check}  All {source} RECORD_IDs start with '{prefix}'")
            else:
                errors.append(f"{check} FAIL: {bad} {source} rows with unexpected RECORD_ID prefix")
                print(f"  FAIL {check}  {bad} {source} rows with RECORD_ID not starting '{prefix}'")

    finally:
        cur.close()
        conn.close()

    n_checks = 9 + 2  # SC2 counted as 3 sub-checks but logically one; keep simple
    if errors:
        print(f"\nValidation FAILED ({len(errors)} error(s)):")
        for e in errors:
            print(f"  ✗ {e}")
        sys.exit(1)
    else:
        print(f"\nValidation PASSED — all Silver checks OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate SILVER.POSITIONS_INTEGRATED against reference CSV")
    parser.add_argument(
        "--data-dir",
        default="data/seed_v2",
        help="Directory containing reference CSVs (default: data/seed_v2)",
    )
    args = parser.parse_args()
    run(args.data_dir)


if __name__ == "__main__":
    main()
