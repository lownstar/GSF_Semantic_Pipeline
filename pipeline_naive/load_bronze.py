"""
Naive Pipeline — Bronze Loader
===============================
Loads four Bronze tables into Snowflake from either local CSVs (PUT + COPY)
or from the S3 landing zone (COPY FROM external stage).

  BRONZE.TOPAZ_POSITIONS       <- positions_topaz.csv       (12,388 rows)
  BRONZE.EMERALD_POSITIONS     <- positions_emerald.csv      (4,886 rows)
  BRONZE.RUBY_POSITIONS        <- positions_ruby.csv         (4,886 rows)
  BRONZE.SECURITY_MASTER_STUB  <- security_master_stub.csv  (  170 rows)

Prerequisites:
  1. Run infrastructure/snowflake_setup.sql in Snowflake
  2. Run pipeline_naive/ddl_bronze.sql in Snowflake
  3. Copy .env.example to .env and fill in your credentials
  4. For S3 mode: run infrastructure/s3_external_stage.sql and delivery/deliver.py

Usage:
  python pipeline_naive/load_bronze.py [--data-dir data/seed_v2]
  python pipeline_naive/load_bronze.py --source s3
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
    "BRONZE.TOPAZ_POSITIONS":      12_388,
    "BRONZE.EMERALD_POSITIONS":     4_886,
    "BRONZE.RUBY_POSITIONS":        4_886,
    "BRONZE.SECURITY_MASTER_STUB":    170,
}

# ── (table, csv_filename, s3_key) load order ─────────────────────────────────
LOAD_PLAN = [
    ("BRONZE.TOPAZ_POSITIONS",      "positions_topaz.csv",      "topaz/positions_topaz.csv"),
    ("BRONZE.EMERALD_POSITIONS",    "positions_emerald.csv",    "emerald/positions_emerald.csv"),
    ("BRONZE.RUBY_POSITIONS",       "positions_ruby.csv",       "ruby/positions_ruby.csv"),
    ("BRONZE.SECURITY_MASTER_STUB", "security_master_stub.csv", "reference/security_master_stub.csv"),
]

# S3 external stage name (created by infrastructure/s3_external_stage.sql)
S3_STAGE = "BRONZE.GSF_S3_LANDING"

# Internal stage name (for local PUT + COPY)
LOCAL_STAGE = "BRONZE.GSF_BRONZE_STAGE"

# Shared CSV file format options
CSV_FORMAT = """
    FILE_FORMAT = (
        TYPE                      = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER               = 1
        NULL_IF                   = ('', 'None', 'nan')
        EMPTY_FIELD_AS_NULL       = TRUE
        DATE_FORMAT               = 'YYYY-MM-DD'
        TIMESTAMP_FORMAT          = 'YYYY-MM-DD HH24:MI:SS'
    )
"""


def get_connection() -> snowflake.connector.SnowflakeConnection:
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"]
    missing  = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"ERROR: missing environment variables: {', '.join(missing)}")
        print("  Copy .env.example to .env and fill in your credentials.")
        sys.exit(1)

    connect_kwargs = dict(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        role      = os.getenv("SNOWFLAKE_ROLE",      "GSF_ROLE"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "GSF_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE",  "GSF_DEMO"),
    )

    key_file = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
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


def load_table_local(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    table: str,
    csv_path: str,
    stage_name: str,
) -> int:
    """PUT a local CSV into an internal stage, then COPY INTO the target table."""
    filename = os.path.basename(csv_path)
    abs_path = os.path.abspath(csv_path).replace("\\", "/")

    print(f"\n  Loading {table} <- {filename} (local)")

    put_sql = f"PUT 'file://{abs_path}' @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    cur.execute(put_sql)
    put_result = cur.fetchone()
    print(f"    PUT: {put_result[0]} -> status={put_result[6]}")

    copy_sql = f"""
        COPY INTO {table}
        FROM @{stage_name}/{filename}.gz
        {CSV_FORMAT}
        PURGE = TRUE
        ON_ERROR = 'ABORT_STATEMENT'
    """
    cur.execute(copy_sql)
    rows_loaded = sum(row[3] for row in cur.fetchall())
    print(f"    COPY: {rows_loaded:,} rows loaded")
    return rows_loaded


def load_table_s3(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    table: str,
    s3_key: str,
) -> int:
    """COPY INTO the target table directly from the S3 external stage."""
    print(f"\n  Loading {table} <- @{S3_STAGE}/{s3_key} (S3)")

    copy_sql = f"""
        COPY INTO {table}
        FROM @{S3_STAGE}/{s3_key}
        {CSV_FORMAT}
        ON_ERROR = 'ABORT_STATEMENT'
    """
    cur.execute(copy_sql)
    rows_loaded = sum(row[3] for row in cur.fetchall())
    print(f"    COPY: {rows_loaded:,} rows loaded")
    return rows_loaded


def verify_counts(cur: snowflake.connector.cursor.SnowflakeCursor) -> bool:
    """Verify row counts match expected values from the generator."""
    print("\nVerifying row counts...")
    ok = True
    for table, expected in EXPECTED_COUNTS.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        actual = cur.fetchone()[0]
        status = "OK" if actual == expected else "FAIL"
        print(f"  {status}  {table}: {actual:,} rows (expected {expected:,})")
        if actual != expected:
            ok = False
    return ok


def run(data_dir: str, source: str) -> None:
    source_label = "S3 external stage" if source == "s3" else f"local ({data_dir})"
    print(f"\n=== Phase 3: Bronze Ingest ===")
    print(f"Source: {source_label}\n")

    conn = get_connection()
    cur  = conn.cursor()

    try:
        cur.execute("USE ROLE GSF_ROLE")
        cur.execute("USE WAREHOUSE GSF_WH")
        cur.execute("USE DATABASE GSF_DEMO")

        if source == "local":
            cur.execute(
                f"CREATE STAGE IF NOT EXISTS {LOCAL_STAGE} "
                f"COMMENT = 'Naive Pipeline Bronze load stage'"
            )
            print(f"Stage: {LOCAL_STAGE}")

        total_loaded = 0
        for table, filename, s3_key in LOAD_PLAN:
            if source == "s3":
                total_loaded += load_table_s3(cur, table, s3_key)
            else:
                csv_path = os.path.join(data_dir, filename)
                if not os.path.exists(csv_path):
                    print(f"  ERROR: file not found: {csv_path}")
                    print("  Run: python -m generator_v2.generator")
                    sys.exit(1)
                total_loaded += load_table_local(cur, table, csv_path, LOCAL_STAGE)

        print(f"\nTotal rows loaded: {total_loaded:,}")

        ok = verify_counts(cur)
        if not ok:
            print("\nRow count verification FAILED.")
            sys.exit(1)
        else:
            print("\nBronze load complete -- all row counts verified.")

    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Bronze tables from seed CSVs")
    parser.add_argument(
        "--data-dir",
        default="data/seed_v2",
        help="Directory containing seed CSVs (default: data/seed_v2)",
    )
    parser.add_argument(
        "--source",
        choices=["local", "s3"],
        default="local",
        help="Load from local files (default) or S3 external stage",
    )
    args = parser.parse_args()
    run(args.data_dir, args.source)


if __name__ == "__main__":
    main()
