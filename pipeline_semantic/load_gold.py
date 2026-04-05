"""
Semantic Enriched Pipeline — Gold YAML Stager
===============================================
Stages all four Cortex Analyst semantic model YAMLs to
@GOLD.GSF_GOLD_STAGE/semantic/ so Cortex can reference them at query time.

As of Step 4 (dbt integration), Gold DW table population is owned by dbt:
  dbt seed          <- loads canonical reference data (account + security master)
  dbt run           <- builds SILVER, GOLD_NAIVE, and GOLD from Bronze
  dbt test          <- validates schema contracts

This script is retained solely for semantic model staging:
  @GSF_GOLD_STAGE/semantic/positions_gold.yaml         <- Gold governed model
  @GSF_GOLD_STAGE/semantic/positions_silver.yaml       <- Silver naive model
  @GSF_GOLD_STAGE/semantic/positions_gold_naive.yaml   <- Naive Gold model
  @GSF_GOLD_STAGE/semantic/positions_bronze.yaml       <- Bronze raw model

Prerequisites:
  1. Run infrastructure/snowflake_setup.sql in Snowflake
  2. Run pipeline_semantic/setup_gold.sql in Snowflake (creates stage)
  3. Run dbt seed && dbt run to populate the DW tables
  4. Copy .env.example to .env and fill in your credentials

Usage:
  python pipeline_semantic/load_gold.py [--semantic-dir semantic_model]
"""

import argparse
import os
import sys

import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_der_private_key
from dotenv import load_dotenv

load_dotenv()

STAGE_NAME = "GOLD.GSF_GOLD_STAGE"

# All four semantic model YAMLs — staged in all-or-nothing fashion
YAML_FILES = [
    "positions_gold.yaml",       # Gold governed (Cortex target for correct answers)
    "positions_silver.yaml",    # Silver naive
    "positions_gold_naive.yaml", # Naive Gold (assumption-based)
    "positions_bronze.yaml",    # Bronze raw (fragmented)
]


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


def stage_yaml(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    yaml_path: str,
) -> None:
    """PUT a semantic model YAML into @GSF_GOLD_STAGE/semantic/."""
    abs_path = os.path.abspath(yaml_path).replace("\\", "/")
    print(f"  Staging: {os.path.basename(yaml_path)}")
    cur.execute(
        f"PUT 'file://{abs_path}' @{STAGE_NAME}/semantic/ "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    put_result = cur.fetchone()
    print(f"    -> @{STAGE_NAME}/semantic/{put_result[0]}  [{put_result[6]}]")


def run(semantic_dir: str) -> None:
    print(f"\nSemantic Enriched Pipeline — YAML Staging")
    print(f"Semantic directory: {semantic_dir}\n")

    conn = get_connection()
    cur  = conn.cursor()

    try:
        cur.execute("USE ROLE GSF_ROLE")
        cur.execute("USE WAREHOUSE GSF_WH")
        cur.execute("USE DATABASE GSF_DEMO")

        staged = 0
        for yaml_name in YAML_FILES:
            yaml_path = os.path.join(semantic_dir, yaml_name)
            if os.path.exists(yaml_path):
                stage_yaml(cur, yaml_path)
                staged += 1
            else:
                print(f"  WARNING: not found, skipping: {yaml_path}")

        print(f"\n{staged}/{len(YAML_FILES)} semantic models staged.")
        print("\nSemantic Enriched Pipeline — YAML staging complete.")

    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stage Cortex Analyst semantic model YAMLs to @GSF_GOLD_STAGE/semantic/"
    )
    parser.add_argument(
        "--semantic-dir",
        default="semantic_model",
        help="Directory containing semantic model YAML files (default: semantic_model)",
    )
    args = parser.parse_args()
    run(args.semantic_dir)


if __name__ == "__main__":
    main()
