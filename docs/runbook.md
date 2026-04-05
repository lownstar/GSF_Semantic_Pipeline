# Runbook: Running the GSF Semantic Pipeline

All scripts run from the project root. Prerequisites: Python env with `requirements.txt`
installed, Snowflake account credentials in `.env` (copy from `.env.example`).

---

## Prerequisites

### Python Environment

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Snowflake Account

Find your account identifier (run in a Snowflake worksheet):
```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();
-- Result format: myorg-myaccount  (use this as SNOWFLAKE_ACCOUNT in .env)
```

### Key-Pair Authentication (required -- Duo MFA blocks password auth)

This account uses Duo MFA. Password-based auth will trigger an MFA push on every script
run and may time out. Key-pair auth is required for all Python pipeline scripts.

**One-time setup:**

1. Generate RSA key pair (run from project root):
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -outform DER -out snowflake_rsa_key.p8
openssl rsa -inform DER -in snowflake_rsa_key.p8 -pubout -outform PEM -out snowflake_rsa_key.pub
```

2. Register the public key in Snowflake (worksheet, as the user or ACCOUNTADMIN):
```sql
ALTER USER DAVIDLOWE80NWL SET RSA_PUBLIC_KEY='<paste key body here>';
DESC USER DAVIDLOWE80NWL;  -- verify RSA_PUBLIC_KEY_FP is non-null
```

3. Add to `.env`:
```
SNOWFLAKE_PRIVATE_KEY_FILE=snowflake_rsa_key.p8
```

**Notes:**
- `snowflake_rsa_key.p8` is gitignored (`*.p8` in `.gitignore`)
- `SNOWFLAKE_USER` must be `DAVIDLOWE80NWL` exactly (JWT is case-sensitive)
- `SNOWFLAKE_ACCOUNT` must be `WYXTVOC-AEB50319` (regionless org-based format)

**Windows note:** All `PUT` commands convert backslashes to forward slashes automatically.

---

## Pipeline Steps

### Step 0 -- One-time Cortex + Horizon setup (ACCOUNTADMIN required)

```sql
-- Paste infrastructure/cortex_setup.sql into a Snowflake worksheet
```

Verify: `ENABLE_CORTEX_ANALYST = true`, Cortex roles granted, governance tags exist.

### Step 1 -- Infrastructure setup (ACCOUNTADMIN required)

```sql
-- Creates GSF_DEMO database, BRONZE/SILVER/GOLD schemas, GSF_WH, GSF_ROLE
-- Paste infrastructure/snowflake_setup.sql into a Snowflake worksheet
```

### Step 2 -- Generate seed data

```bash
python -m generator_v2.generator --validate
```

Produces 9 CSVs in `data/seed_v2/`.

### Step 3 -- Naive Pipeline: Bronze + Silver

```bash
# Create tables (worksheet or SnowSQL)
# snowsql -f pipeline_naive/ddl_bronze.sql
# snowsql -f pipeline_naive/ddl_silver.sql

# Load Bronze from seed CSVs
python pipeline_naive/load_bronze.py

# Run naive ETL
# snowsql -f pipeline_naive/etl_silver.sql

# Validate
python pipeline_naive/validate_silver.py
```

**Expected row counts:**

| Table | Rows |
|---|---|
| `BRONZE.TOPAZ_POSITIONS` | 12,388 |
| `BRONZE.EMERALD_POSITIONS` | 4,886 |
| `BRONZE.RUBY_POSITIONS` | 4,886 |
| `BRONZE.SECURITY_MASTER_STUB` | 170 |
| `SILVER.POSITIONS_INTEGRATED` | 22,160 |

### Step 4 -- Semantic Enriched Pipeline: Gold + Semantic Model

```bash
# Create GOLD tables and stage (worksheet or SnowSQL)
# snowsql -f pipeline_semantic/setup_gold.sql

# Load Gold DW tables + stage semantic model YAMLs
python pipeline_semantic/load_gold.py

# Validate
python pipeline_semantic/validate_gold.py
```

### Step 5 -- Variance Capture and Visualization

```bash
# Verify ground truth (no Snowflake needed)
python variance/runner.py --dry-run

# Run all 11 questions against both models (~2-4 min)
python variance/runner.py

# Launch Streamlit visualization
streamlit run app/streamlit_app.py
```

### Stage Access for Snowsight Cortex Analyst UI

```sql
USE ROLE GSF_ROLE;
ALTER STAGE GOLD.GSF_GOLD_STAGE REFRESH;
```

Grant SYSADMIN browsing (run once as ACCOUNTADMIN):
```sql
GRANT READ ON STAGE GSF_DEMO.GOLD.GSF_GOLD_STAGE TO ROLE SYSADMIN;
GRANT WRITE ON STAGE GSF_DEMO.GOLD.GSF_GOLD_STAGE TO ROLE SYSADMIN;
```

### SnowSQL connection

```bash
snowsql -a <orgname>-<accountname> -u <username> -f infrastructure/cortex_setup.sql
```
