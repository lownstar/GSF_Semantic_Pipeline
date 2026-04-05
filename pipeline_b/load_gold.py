"""
Pipeline B — Gold Loader
========================
Stages all four DW seed CSVs from data/seed_v2/ into Snowflake and
COPY INTOs the corresponding Gold tables:

  GOLD.DW_ACCOUNT    <- dw_account.csv    (  100 rows)
  GOLD.DW_SECURITY   <- dw_security.csv   (  200 rows)
  GOLD.DW_POSITION   <- dw_position.csv   (4,886 rows)
  GOLD.DW_TRADE_LOT  <- dw_trade_lot.csv  (12,388 rows)

Also stages the semantic model YAML to @GSF_GOLD_STAGE/semantic/:

  @GSF_GOLD_STAGE/semantic/positions.yaml <- semantic_model/positions.yaml

Load order: DW_ACCOUNT and DW_SECURITY first (FK parents), then DW_POSITION
and DW_TRADE_LOT (FK children).

Prerequisites:
  1. Run pipeline_b/setup_gold.sql in Snowflake
  2. Copy .env.example to .env and fill in your credentials

Usage:
  python pipeline_b/load_gold.py [--data-dir data/seed_v2] [--semantic-dir semantic_model]
"""

import argparse
import os
import sys

import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_der_private_key
from dotenv import load_dotenv

load_dotenv()

# ── Expected row counts (from generator_v2 deterministic output) ───────────────
EXPECTED_COUNTS = {
    "GOLD.DW_ACCOUNT":   100,
    "GOLD.DW_SECURITY":  200,
    "GOLD.DW_POSITION":  4_886,
    "GOLD.DW_TRADE_LOT": 12_388,
}

# ── (table, csv_filename) load order — parents before children ────────────────
LOAD_PLAN = [
    ("GOLD.DW_ACCOUNT",   "dw_account.csv"),
    ("GOLD.DW_SECURITY",  "dw_security.csv"),
    ("GOLD.DW_POSITION",  "dw_position.csv"),
    ("GOLD.DW_TRADE_LOT", "dw_trade_lot.csv"),
]

STAGE_NAME   = "GOLD.GSF_GOLD_STAGE"
SEMANTIC_KEY = "semantic/positions.yaml"


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
        key_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), key_file) \
                   if not os.path.isabs(key_file) else key_file
        with open(key_path, "rb") as f:
            private_key = load_der_private_key(f.read(), password=None)
        connect_kwargs["private_key"] = private_key
    elif os.getenv("SNOWFLAKE_PASSWORD"):
        connect_kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        print("ERROR: set SNOWFLAKE_PRIVATE_KEY_FILE or SNOWFLAKE_PASSWORD in .env")
        sys.exit(1)

    return snowflake.connector.connect(**connect_kwargs)


def load_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    table: str,
    csv_path: str,
) -> int:
    """PUT a local CSV to the stage, COPY INTO the target table. Returns rows loaded."""
    filename = os.path.basename(csv_path)
    abs_path = os.path.abspath(csv_path).replace("\\", "/")

    print(f"\n  Loading {table} <- {filename}")

    cur.execute(f"TRUNCATE TABLE {table}")

    cur.execute(
        f"PUT 'file://{abs_path}' @{STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )
    put_result = cur.fetchone()
    print(f"    PUT: {put_result[0]} status={put_result[6]}")

    cur.execute(f"""
        COPY INTO {table}
        FROM @{STAGE_NAME}/{filename}.gz
        FILE_FORMAT = (
            TYPE                         = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER                  = 1
            NULL_IF                      = ('', 'None', 'nan')
            EMPTY_FIELD_AS_NULL          = TRUE
            DATE_FORMAT                  = 'YYYY-MM-DD'
            TIMESTAMP_FORMAT             = 'YYYY-MM-DD HH24:MI:SS'
        )
        PURGE = TRUE
        ON_ERROR = 'ABORT_STATEMENT'
    """)
    rows_loaded = sum(row[3] for row in cur.fetchall())
    print(f"    COPY: {rows_loaded:,} rows loaded")
    return rows_loaded


def stage_semantic_model(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    yaml_path: str,
) -> None:
    """PUT the semantic model YAML into @GSF_GOLD_STAGE/semantic/."""
    abs_path = os.path.abspath(yaml_path).replace("\\", "/")
    print(f"\n  Staging semantic model <- {yaml_path}")

    # PUT to the semantic/ prefix — Snowflake uses the filename; the prefix is
    # part of the stage path used at Cortex query time.
    cur.execute(
        f"PUT 'file://{abs_path}' @{STAGE_NAME}/semantic/ "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    put_result = cur.fetchone()
    print(f"    PUT: {put_result[0]} status={put_result[6]}")
    print(f"    Staged at: @{STAGE_NAME}/semantic/{os.path.basename(yaml_path)}")


def verify_counts(cur: snowflake.connector.cursor.SnowflakeCursor) -> bool:
    """Verify row counts match expected values from the generator."""
    print("\nVerifying row counts...")
    ok = True
    for table, expected in EXPECTED_COUNTS.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        actual = cur.fetchone()[0]
        status = "OK  " if actual == expected else "FAIL"
        print(f"  {status}  {table}: {actual:,} rows (expected {expected:,})")
        if actual != expected:
            ok = False
    return ok


def run(data_dir: str, semantic_dir: str) -> None:
    print(f"\nPipeline B — Gold Load")
    print(f"Data directory:     {data_dir}")
    print(f"Semantic directory: {semantic_dir}\n")

    conn = get_connection()
    cur  = conn.cursor()

    try:
        cur.execute("USE ROLE GSF_ROLE")
        cur.execute("USE WAREHOUSE GSF_WH")
        cur.execute("USE DATABASE GSF_DEMO")

        # ── Load DW tables ────────────────────────────────────────────────────
        total_loaded = 0
        for table, filename in LOAD_PLAN:
            csv_path = os.path.join(data_dir, filename)
            if not os.path.exists(csv_path):
                print(f"  ERROR: seed CSV not found: {csv_path}")
                print("  Run: python -m generator_v2.generator")
                sys.exit(1)
            total_loaded += load_table(cur, table, csv_path)

        print(f"\nTotal rows loaded: {total_loaded:,}")

        ok = verify_counts(cur)
        if not ok:
            print("\nRow count verification FAILED — check seed CSVs and DDL.")
            sys.exit(1)

        print("\nGold tables loaded — all row counts verified.")

        # ── Stage semantic model YAMLs ────────────────────────────────────────
        yaml_files = ["positions.yaml", "positions_silver.yaml"]
        staged_any = False
        for yaml_name in yaml_files:
            yaml_path = os.path.join(semantic_dir, yaml_name)
            if os.path.exists(yaml_path):
                stage_semantic_model(cur, yaml_path)
                staged_any = True
            else:
                print(f"\n  WARNING: semantic model not found: {yaml_path}")
        if staged_any:
            print("\nSemantic models staged.")

        print("\nPipeline B — Gold load complete.")

    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Gold DW tables and stage semantic model")
    parser.add_argument(
        "--data-dir",
        default="data/seed_v2",
        help="Directory containing DW seed CSVs (default: data/seed_v2)",
    )
    parser.add_argument(
        "--semantic-dir",
        default="semantic_model",
        help="Directory containing positions.yaml (default: semantic_model)",
    )
    args = parser.parse_args()
    run(args.data_dir, args.semantic_dir)


if __name__ == "__main__":
    main()
