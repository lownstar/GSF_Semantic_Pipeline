# Runbook: Running the GSF Semantic Pipeline

All scripts run from the project root. Prerequisites: Python env with `requirements.txt`
installed, Snowflake credentials in `.env` (copy from `.env.example`).

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
-- Result format: myorg-myaccount
```

### Key-Pair Authentication (required — Duo MFA blocks password auth)

**One-time setup:**

1. Generate RSA key pair:
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -outform DER -out snowflake_rsa_key.p8
openssl rsa -inform DER -in snowflake_rsa_key.p8 -pubout -outform PEM -out snowflake_rsa_key.pub
```

2. Register the public key in Snowflake:
```sql
ALTER USER DAVIDLOWE80NWL SET RSA_PUBLIC_KEY='<paste key body here>';
DESC USER DAVIDLOWE80NWL;  -- verify RSA_PUBLIC_KEY_FP is non-null
```

3. Add to `.env`:
```
SNOWFLAKE_PRIVATE_KEY_FILE=snowflake_rsa_key.p8
```

**Notes:**
- `snowflake_rsa_key.p8` is gitignored — never commit it
- `SNOWFLAKE_USER` must be `DAVIDLOWE80NWL` exactly (JWT is case-sensitive)
- `SNOWFLAKE_ACCOUNT` must be `WYXTVOC-AEB50319` (regionless org-based format)
- All `PUT` commands convert Windows backslashes to forward slashes automatically

### AWS Credentials (for S3 delivery — optional)

Add to `.env` (see `.env.example` for the full template):
```
AWS_ACCESS_KEY_ID=<your-access-key>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_S3_BUCKET=gsf-demo-landing
AWS_REGION=us-east-1
```

Or configure `~/.aws/credentials` — boto3 reads it automatically.

---

## Step 0 — One-time Snowflake Infrastructure (ACCOUNTADMIN required)

Run once per Snowflake account. Must be done before any pipeline steps.

```sql
-- Step 0a: Core infrastructure
-- Creates GSF_DEMO database, BRONZE/SILVER/GOLD schemas, GSF_WH, GSF_ROLE
-- Paste infrastructure/snowflake_setup.sql into a Snowflake worksheet

-- Step 0b: Cortex + Horizon setup
-- Enables Cortex Analyst, grants roles, creates governance tags
-- Paste infrastructure/cortex_setup.sql into a Snowflake worksheet
```

Verify Cortex setup: `ENABLE_CORTEX_ANALYST = true`, Cortex roles granted, tags exist.

### Optional: S3 External Stage (ACCOUNTADMIN required)

Only needed if using `--source s3` with `load_bronze.py`.

```sql
-- Paste infrastructure/s3_external_stage.sql into a Snowflake worksheet
-- Follow the IAM trust policy instructions in Section 3 of that file
```

After creating the storage integration, run `DESC INTEGRATION gsf_s3_integration`
to get `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`. Add these to your
IAM role's trust policy. See `infrastructure/s3_external_stage.sql` for the full
IAM policy reference.

---

## Step 1 — Phase 1: Generate Seed Data

```bash
python -m generator_v2.generator --validate
```

Produces 9 deterministic CSVs in `data/seed_v2/`. All 21 integrity checks must pass.

| File | Rows | Description |
|---|---|---|
| `dw_account.csv` | 100 | Canonical accounts (all 3 source keys) |
| `dw_security.csv` | 200 | Complete security master |
| `dw_position.csv` | 4,886 | Canonical position-level grain |
| `dw_trade_lot.csv` | 12,388 | Lot-level detail |
| `positions_topaz.csv` | 12,388 | Topaz feed (CUSIP, lot-level, custodian price) |
| `positions_emerald.csv` | 4,886 | Emerald feed (ticker, position-level, PM price) |
| `positions_ruby.csv` | 4,886 | Ruby feed (ISIN, position-level, NAV price) |
| `security_master_stub.csv` | 170 | Partial master — 30 securities absent (A8/A10) |
| `positions_integrated.csv` | 22,160 | Naive ETL reference (validation only) |

---

## Step 2 — Phase 2: Deliver to S3 (optional)

```bash
python delivery/deliver.py [--bucket gsf-demo-landing]
```

Uploads source system CSVs to the S3 landing zone:
```
s3://gsf-demo-landing/topaz/positions_topaz.csv
s3://gsf-demo-landing/emerald/positions_emerald.csv
s3://gsf-demo-landing/ruby/positions_ruby.csv
s3://gsf-demo-landing/reference/security_master_stub.csv
```

Skip this step if using local file loading (default mode for `load_bronze.py`).

---

## Step 3 — Phase 3: Bronze Ingest

```bash
# Create Bronze tables (worksheet or SnowSQL)
# snowsql -f pipeline_naive/ddl_bronze.sql

# Load from local files (default — no AWS required)
python pipeline_naive/load_bronze.py

# OR load from S3 external stage (requires Step 0 S3 setup + Step 2)
python pipeline_naive/load_bronze.py --source s3
```

**Expected row counts:** TOPAZ=12,388 / EMERALD=4,886 / RUBY=4,886 / STUB=170

---

## Step 4 — Phase 4: Silver Transform

```bash
# Create Silver table (worksheet or SnowSQL)
# snowsql -f pipeline_naive/ddl_silver.sql

# Run naive ETL (worksheet or SnowSQL)
# snowsql -f pipeline_naive/etl_silver.sql

# Validate
python pipeline_naive/validate_silver.py
```

**Expected output:** 22,160 rows with intentional ambiguities:

| Check | Expected | Ambiguity |
|---|---|---|
| `SECURITY_MASTER_ID IS NULL` | ~15% | A8 |
| `UNREALIZED_GL IS NULL` | ~22% | A11 |
| `ASSET_CLASS IS NULL` | ~15% | A10 |

---

## Step 5 — Phase 5: Gold Load (Semantic Enriched Pipeline)

```bash
# Create Gold tables and stage (worksheet or SnowSQL)
# snowsql -f pipeline_semantic/setup_gold.sql

# Load Gold DW tables + stage semantic model YAMLs
python pipeline_semantic/load_gold.py

# Validate
python pipeline_semantic/validate_gold.py
```

**Expected:** All GC1-GC12 checks pass — 100 accounts, 200 securities, 4,886 positions,
12,388 lots, zero NULLs, all FKs resolve, YAML staged.

---

## Step 6 — Phase 6: Cortex Analyst Query

```bash
# Gate question — governed Gold model
python cortex/query_cortex.py --model gold
# Expected: George Group Trust / $47,944,909.80

# Same question — naive Silver model
python cortex/query_cortex.py --model silver
# Expected: no data (A4 — account_ref mismatch)

# Custom question
python cortex/query_cortex.py --model gold --question "What is the total AUM?"

# Print generated SQL only (no execution)
python cortex/query_cortex.py --model gold --no-execute
```

### Snowsight Cortex Analyst UI

```sql
USE ROLE GSF_ROLE;
ALTER STAGE GOLD.GSF_GOLD_STAGE REFRESH;
```

Grant SYSADMIN browsing access (run once as ACCOUNTADMIN):
```sql
GRANT READ ON STAGE GSF_DEMO.GOLD.GSF_GOLD_STAGE TO ROLE SYSADMIN;
GRANT WRITE ON STAGE GSF_DEMO.GOLD.GSF_GOLD_STAGE TO ROLE SYSADMIN;
```

---

## Step 7 — Phase 7: Variance Comparison + Visualization

```bash
# Verify ground truth (no Snowflake needed)
python variance/runner.py --dry-run

# Run all 11 questions against both models (~2-4 min)
# Saves timestamped JSON to variance/results/
python variance/runner.py

# Run one model only
python variance/runner.py --model gold
python variance/runner.py --model silver

# Launch Streamlit app
streamlit run app/streamlit_app.py
```

The Streamlit app re-scores from raw rows on every load — comparator fixes apply
immediately without re-running Cortex.

---

## SnowSQL Reference

```bash
snowsql -a WYXTVOC-AEB50319 -u DAVIDLOWE80NWL -f infrastructure/snowflake_setup.sql
```

Or configure `~/.snowsql/config` with account credentials.
