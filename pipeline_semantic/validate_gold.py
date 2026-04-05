"""
Semantic Enriched Pipeline — Gold Validator
============================================
Validates the GOLD DW tables in Snowflake against expected row counts,
FK integrity, and NULL constraints. Confirms the governed environment is
clean — no ambiguities, no orphaned rows, no missing classifications.

Checks:
  GC1   DW_ACCOUNT row count = 100
  GC2   DW_SECURITY row count = 200 (complete master — vs 170 in Bronze stub)
  GC3   DW_POSITION row count = 4,886
  GC4   DW_TRADE_LOT row count = 12,388
  GC5   DW_POSITION: all account_ids exist in DW_ACCOUNT (FK integrity)
  GC6   DW_POSITION: all security_ids exist in DW_SECURITY (FK integrity)
  GC7   DW_TRADE_LOT: all account_ids exist in DW_ACCOUNT (FK integrity)
  GC8   DW_TRADE_LOT: all security_ids exist in DW_SECURITY (FK integrity)
  GC9   DW_POSITION: zero NULL unrealized_gain_loss (resolves A11)
  GC10  DW_SECURITY: zero NULL asset_class (resolves A10)
  GC11  DW_SECURITY: 200 rows — 30 more than Bronze stub (resolves A8)
  GC12  Semantic model staged at @GSF_GOLD_STAGE/semantic/positions.yaml

Usage:
  python pipeline_semantic/validate_gold.py
"""

import os
import sys

import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_der_private_key
from dotenv import load_dotenv

load_dotenv()

STAGE_NAME   = "GOLD.GSF_GOLD_STAGE"
SEMANTIC_KEY = "semantic/positions.yaml"

EXPECTED_COUNTS = {
    "GOLD.DW_ACCOUNT":   100,
    "GOLD.DW_SECURITY":  200,
    "GOLD.DW_POSITION":  4_886,
    "GOLD.DW_TRADE_LOT": 12_388,
}

BRONZE_STUB_COUNT = 170  # for GC11 contrast narrative


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


def fetch_scalar(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


def run() -> None:
    print("\nSemantic Enriched Pipeline — Gold Validation")
    print("Target: GOLD.DW_ACCOUNT / DW_SECURITY / DW_POSITION / DW_TRADE_LOT\n")

    conn = get_connection()
    cur  = conn.cursor()
    errors = []

    try:
        cur.execute("USE ROLE GSF_ROLE")
        cur.execute("USE WAREHOUSE GSF_WH")
        cur.execute("USE DATABASE GSF_DEMO")

        # ── GC1–GC4: Row counts ───────────────────────────────────────────────
        for table, expected in EXPECTED_COUNTS.items():
            check = f"GC{list(EXPECTED_COUNTS).index(table) + 1}"
            actual = fetch_scalar(cur, f"SELECT COUNT(*) FROM {table}")
            if actual == expected:
                print(f"  OK   {check}  {table}: {actual:,} rows")
            else:
                errors.append(f"{check} FAIL: {table} rows={actual:,}, expected={expected:,}")
                print(f"  FAIL {check}  {table}: {actual:,} rows (expected {expected:,})")

        # ── GC5: DW_POSITION account FK ───────────────────────────────────────
        orphan_acct = fetch_scalar(cur, """
            SELECT COUNT(*) FROM GOLD.DW_POSITION p
            WHERE NOT EXISTS (
                SELECT 1 FROM GOLD.DW_ACCOUNT a WHERE a.account_id = p.account_id
            )
        """)
        if orphan_acct == 0:
            print("  OK   GC5  DW_POSITION.account_id: all FKs resolve to DW_ACCOUNT")
        else:
            errors.append(f"GC5 FAIL: {orphan_acct:,} DW_POSITION rows with orphaned account_id")
            print(f"  FAIL GC5  DW_POSITION: {orphan_acct:,} orphaned account_id rows")

        # ── GC6: DW_POSITION security FK ──────────────────────────────────────
        orphan_sec = fetch_scalar(cur, """
            SELECT COUNT(*) FROM GOLD.DW_POSITION p
            WHERE NOT EXISTS (
                SELECT 1 FROM GOLD.DW_SECURITY s WHERE s.security_id = p.security_id
            )
        """)
        if orphan_sec == 0:
            print("  OK   GC6  DW_POSITION.security_id: all FKs resolve to DW_SECURITY")
        else:
            errors.append(f"GC6 FAIL: {orphan_sec:,} DW_POSITION rows with orphaned security_id")
            print(f"  FAIL GC6  DW_POSITION: {orphan_sec:,} orphaned security_id rows")

        # ── GC7: DW_TRADE_LOT account FK ──────────────────────────────────────
        orphan_lot_acct = fetch_scalar(cur, """
            SELECT COUNT(*) FROM GOLD.DW_TRADE_LOT l
            WHERE NOT EXISTS (
                SELECT 1 FROM GOLD.DW_ACCOUNT a WHERE a.account_id = l.account_id
            )
        """)
        if orphan_lot_acct == 0:
            print("  OK   GC7  DW_TRADE_LOT.account_id: all FKs resolve to DW_ACCOUNT")
        else:
            errors.append(f"GC7 FAIL: {orphan_lot_acct:,} DW_TRADE_LOT rows with orphaned account_id")
            print(f"  FAIL GC7  DW_TRADE_LOT: {orphan_lot_acct:,} orphaned account_id rows")

        # ── GC8: DW_TRADE_LOT security FK ─────────────────────────────────────
        orphan_lot_sec = fetch_scalar(cur, """
            SELECT COUNT(*) FROM GOLD.DW_TRADE_LOT l
            WHERE NOT EXISTS (
                SELECT 1 FROM GOLD.DW_SECURITY s WHERE s.security_id = l.security_id
            )
        """)
        if orphan_lot_sec == 0:
            print("  OK   GC8  DW_TRADE_LOT.security_id: all FKs resolve to DW_SECURITY")
        else:
            errors.append(f"GC8 FAIL: {orphan_lot_sec:,} DW_TRADE_LOT rows with orphaned security_id")
            print(f"  FAIL GC8  DW_TRADE_LOT: {orphan_lot_sec:,} orphaned security_id rows")

        # ── GC9: No NULL unrealized_gain_loss (resolves A11) ──────────────────
        null_gl = fetch_scalar(
            cur,
            "SELECT COUNT(*) FROM GOLD.DW_POSITION WHERE unrealized_gain_loss IS NULL"
        )
        if null_gl == 0:
            print("  OK   GC9  DW_POSITION.unrealized_gain_loss: zero NULLs (resolves A11)")
        else:
            errors.append(f"GC9 FAIL: {null_gl:,} NULL unrealized_gain_loss rows in DW_POSITION")
            print(f"  FAIL GC9  DW_POSITION: {null_gl:,} NULL unrealized_gain_loss rows")

        # ── GC10: No NULL asset_class in DW_SECURITY (resolves A10) ───────────
        null_ac = fetch_scalar(
            cur,
            "SELECT COUNT(*) FROM GOLD.DW_SECURITY WHERE asset_class IS NULL"
        )
        if null_ac == 0:
            print("  OK   GC10 DW_SECURITY.asset_class: zero NULLs (resolves A10)")
        else:
            errors.append(f"GC10 FAIL: {null_ac:,} NULL asset_class rows in DW_SECURITY")
            print(f"  FAIL GC10 DW_SECURITY: {null_ac:,} NULL asset_class rows")

        # ── GC11: DW_SECURITY has 30 more rows than Bronze stub (resolves A8) ──
        sec_count = fetch_scalar(cur, "SELECT COUNT(*) FROM GOLD.DW_SECURITY")
        gain = sec_count - BRONZE_STUB_COUNT
        if sec_count == 200 and gain == 30:
            print(f"  OK   GC11 DW_SECURITY: {sec_count} rows (+{gain} vs Bronze stub of {BRONZE_STUB_COUNT}) (resolves A8)")
        else:
            errors.append(f"GC11 FAIL: DW_SECURITY={sec_count} rows, expected 200")
            print(f"  FAIL GC11 DW_SECURITY: {sec_count} rows (expected 200)")

        # ── GC12: Semantic model staged ───────────────────────────────────────
        cur.execute(f"LIST @{STAGE_NAME}/semantic/")
        staged_files = [row[0] for row in cur.fetchall()]
        yaml_staged = any("positions.yaml" in f for f in staged_files)
        if yaml_staged:
            print(f"  OK   GC12 Semantic model staged at @{STAGE_NAME}/semantic/positions.yaml")
        else:
            errors.append(f"GC12 FAIL: positions.yaml not found in @{STAGE_NAME}/semantic/")
            print(f"  FAIL GC12 Semantic model not staged — run load_gold.py after authoring positions.yaml")

    finally:
        cur.close()
        conn.close()

    if errors:
        print(f"\nValidation FAILED ({len(errors)} error(s)):")
        for e in errors:
            print(f"  ✗ {e}")
        sys.exit(1)
    else:
        print("\nValidation PASSED — all Gold checks OK")


if __name__ == "__main__":
    run()
