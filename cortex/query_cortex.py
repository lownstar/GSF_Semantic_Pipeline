"""
Cortex Analyst Query Runner
============================
Sends a natural language question to Snowflake Cortex Analyst using the
REST API with session token authentication. Prints the generated SQL and
executes it against Snowflake to return the result set.

Four models supported:
  --model gold        uses positions_gold.yaml (governed Semantic Gold)
  --model gold_naive  uses positions_gold_naive.yaml (assumption-based Naive Gold)
  --model silver      uses positions_silver.yaml (naive Silver)
  --model bronze      uses positions_bronze.yaml (raw Bronze, fragmented)

Usage:
  python cortex/query_cortex.py --model gold
  python cortex/query_cortex.py --model gold_naive
  python cortex/query_cortex.py --model silver
  python cortex/query_cortex.py --model bronze
  python cortex/query_cortex.py --model gold --question "What is total AUM?"
  python cortex/query_cortex.py --model gold --no-execute  # print SQL only

Gate question (Epic 4 validation):
  "What is the total market value of account ACC-0042?"
"""

import argparse
import json
import os
import sys

import requests
import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_der_private_key
from dotenv import load_dotenv

load_dotenv()

GATE_QUESTION = "What is the total market value of account ACC-0042?"

STAGE_BASE = "@GSF_DEMO.GOLD.GSF_GOLD_STAGE/semantic"

MODELS = {
    "gold":       f"{STAGE_BASE}/positions_gold.yaml",
    "gold_naive": f"{STAGE_BASE}/positions_gold_naive.yaml",
    "silver":     f"{STAGE_BASE}/positions_silver.yaml",
    "bronze":     f"{STAGE_BASE}/positions_bronze.yaml",
}

MODEL_LABELS = {
    "gold":       "Semantic Gold (governed — positions_gold.yaml)",
    "gold_naive": "Naive Gold (assumption-based — positions_gold_naive.yaml)",
    "silver":     "Silver (naive — positions_silver.yaml)",
    "bronze":     "Bronze (raw/fragmented — positions_bronze.yaml)",
}

# YAML filename for each model key (used by _ensure_staged)
_MODEL_YAML = {
    "gold":       "positions_gold.yaml",
    "gold_naive": "positions_gold_naive.yaml",
    "silver":     "positions_silver.yaml",
    "bronze":     "positions_bronze.yaml",
}


# ── Snowflake connection (for executing the generated SQL) ─────────────────────

def _get_connection() -> snowflake.connector.SnowflakeConnection:
    account  = os.environ.get("SNOWFLAKE_ACCOUNT")
    user     = os.environ.get("SNOWFLAKE_USER")
    key_file = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")

    if not account or not user:
        print("ERROR: SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER required")
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


# ── Stage semantic model YAMLs on demand ─────────────────────────────────────

def _ensure_staged(model_key: str, conn: snowflake.connector.SnowflakeConnection) -> None:
    """PUT the YAML for model_key to the stage if not already there."""
    yaml_name = _MODEL_YAML[model_key]
    cur = conn.cursor()
    try:
        cur.execute("LIST @GSF_DEMO.GOLD.GSF_GOLD_STAGE/semantic/")
        staged = [row[0] for row in cur.fetchall()]
        if not any(yaml_name in f for f in staged):
            yaml_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "semantic_model", yaml_name
            )
            abs_path = os.path.abspath(yaml_path).replace("\\", "/")
            print(f"  Staging {yaml_name}...")
            cur.execute(
                f"PUT 'file://{abs_path}' "
                f"@GSF_DEMO.GOLD.GSF_GOLD_STAGE/semantic/ "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            print("  Staged.")
    finally:
        cur.close()


def _ensure_silver_staged(conn: snowflake.connector.SnowflakeConnection) -> None:
    """Backwards-compatible alias for _ensure_staged('silver', conn)."""
    _ensure_staged("silver", conn)


# ── Cortex Analyst REST call ───────────────────────────────────────────────────

def ask_cortex(question: str, model_key: str, conn: snowflake.connector.SnowflakeConnection) -> dict:
    """
    POST a question to Cortex Analyst and return the full response dict.
    Response includes: message (with SQL), confidence, etc.
    Uses the session token from an already-open connector connection.
    """
    account  = os.environ.get("SNOWFLAKE_ACCOUNT", "")
    if not account:
        print("ERROR: SNOWFLAKE_ACCOUNT required")
        sys.exit(1)

    token    = conn._rest._token
    endpoint = f"https://{account}.snowflakecomputing.com/api/v2/cortex/analyst/message"

    role = os.getenv("SNOWFLAKE_ROLE", "GSF_ROLE")
    headers = {
        "Authorization":                        f"Snowflake Token=\"{token}\"",
        "Content-Type":                         "application/json",
        "Accept":                               "application/json",
        "X-Snowflake-Authorization-Token-Type": "TOKEN",
        "Snowflake-Role":                        role,
    }

    payload = {
        "messages": [
            {
                "role":    "user",
                "content": [{"type": "text", "text": question}],
            }
        ],
        "semantic_model_file": MODELS[model_key],
    }

    response = requests.post(endpoint, headers=headers, json=payload, timeout=60)

    if response.status_code != 200:
        print(f"ERROR: Cortex Analyst API returned {response.status_code}")
        print(response.text)
        sys.exit(1)

    return response.json()


# ── Execute the generated SQL ──────────────────────────────────────────────────

def execute_sql(sql: str, conn: snowflake.connector.SnowflakeConnection) -> list[dict]:
    """Execute SQL and return rows as list of dicts."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()


# ── Pretty print ──────────────────────────────────────────────────────────────

def _print_result(rows: list[dict]) -> None:
    if not rows:
        print("  (no rows returned)")
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}
    header = "  " + "  ".join(c.ljust(widths[c]) for c in cols)
    sep    = "  " + "  ".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for row in rows[:20]:
        print("  " + "  ".join(str(row[c]).ljust(widths[c]) for c in cols))
    if len(rows) > 20:
        print(f"  ... ({len(rows)} total rows, showing first 20)")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(question: str, model_key: str, execute: bool) -> None:
    label = MODEL_LABELS[model_key]
    print(f"\n{'='*70}")
    print(f"  Cortex Analyst — {label}")
    print(f"{'='*70}")
    print(f"  Question: {question}\n")

    conn = _get_connection()

    # Ensure the requested model's YAML is staged
    _ensure_staged(model_key, conn)

    # Call Cortex Analyst
    print("  Calling Cortex Analyst API...")
    response = ask_cortex(question, model_key, conn)

    # Extract SQL from response
    sql = None
    message_content = response.get("message", {}).get("content", [])
    for block in message_content:
        if block.get("type") == "sql":
            sql = block.get("statement", "")
            break
        if block.get("type") == "text":
            print(f"  Cortex says: {block.get('text', '')}")

    if not sql:
        print("\n  WARNING: Cortex returned no SQL.")
        print("  Full response:")
        print(json.dumps(response, indent=2))
        conn.close()
        return

    print(f"\n  Generated SQL:\n")
    for line in sql.strip().splitlines():
        print(f"    {line}")

    if execute:
        print(f"\n  Result:")
        rows = execute_sql(sql, conn)
        _print_result(rows)

    conn.close()
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a question against Cortex Analyst (Gold or Silver semantic model)"
    )
    parser.add_argument(
        "--model",
        choices=["gold", "gold_naive", "silver", "bronze"],
        required=True,
        help="Which semantic model to use: gold, gold_naive, silver, or bronze",
    )
    parser.add_argument(
        "--question",
        default=GATE_QUESTION,
        help=f'Question to ask (default: "{GATE_QUESTION}")',
    )
    parser.add_argument(
        "--no-execute",
        action="store_true",
        help="Print generated SQL but do not execute it",
    )
    args = parser.parse_args()
    run(args.question, args.model, execute=not args.no_execute)


if __name__ == "__main__":
    main()
