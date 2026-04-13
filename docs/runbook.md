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

1. Generate all three key files:
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -outform DER -out snowflake_rsa_key.p8
openssl rsa -inform DER -in snowflake_rsa_key.p8 -pubout -outform PEM -out snowflake_rsa_key.pub
openssl pkcs8 -topk8 -nocrypt -in snowflake_rsa_key.p8 -inform DER -outform PEM -out snowflake_rsa_key.pem
```

Three files are produced:
- `snowflake_rsa_key.p8` — private key (DER format) used by Python scripts
- `snowflake_rsa_key.pub` — public key for Snowflake registration
- `snowflake_rsa_key.pem` — private key (PEM format) used by dbt

2. Register the public key in Snowflake:
```sql
ALTER USER DAVIDLOWE80NWL SET RSA_PUBLIC_KEY='<paste key body here>';
DESC USER DAVIDLOWE80NWL;  -- verify RSA_PUBLIC_KEY_FP is non-null
```

3. Add both key vars to `.env`:
```
SNOWFLAKE_PRIVATE_KEY_FILE=snowflake_rsa_key.p8    # Python scripts
SNOWFLAKE_PRIVATE_KEY_PEM=snowflake_rsa_key.pem    # dbt
```

**Why two vars?** Python scripts load the DER key directly via `cryptography`. dbt reads
the path via `profiles.yml` → `private_key_path` and expects PEM format. Both point to
the same underlying key pair — just different encoding formats.

**Notes:**
- All three key files are gitignored — never commit them
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

### dbt Profile

`dbt/profiles.yml` reads Snowflake credentials from `.env` via environment variables.
The key difference from the Python scripts: dbt requires `SNOWFLAKE_PRIVATE_KEY_PEM`
(the path to the PEM file), not `SNOWFLAKE_PRIVATE_KEY_FILE` (the DER/p8 file).
Both must be set in `.env` — see the Key-Pair Authentication section above.

No other dbt configuration is needed beyond the `.env` file.

---

## Step 0 — One-time Snowflake Infrastructure (ACCOUNTADMIN required)

Run once per Snowflake account. Must be done before any pipeline steps.

```sql
-- Step 0a: Core infrastructure
-- Creates GSF_DEMO database, BRONZE/SILVER/GOLD/GOLD_NAIVE schemas, GSF_WH, GSF_ROLE
-- Paste infrastructure/snowflake_setup.sql into a Snowflake worksheet

-- Step 0b: Cortex + Horizon setup
-- Enables Cortex Analyst, grants roles, creates governance tags
-- Paste infrastructure/cortex_setup.sql into a Snowflake worksheet
```

Verify Cortex setup: `ENABLE_CORTEX_ANALYST = true`, Cortex roles granted, tags exist.

### Optional: S3 External Stage (ACCOUNTADMIN required)

Only needed if using `--source s3` with `load_bronze.py` or `run_pipeline.py`.

```sql
-- Paste infrastructure/s3_external_stage.sql into a Snowflake worksheet
-- Follow the IAM trust policy instructions in Section 3 of that file
```

After creating the storage integration, run `DESC INTEGRATION gsf_s3_integration`
to get `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`. Add these to your
IAM role's trust policy. See `infrastructure/s3_external_stage.sql` for the full
IAM policy reference.

---

## Running the Pipeline

### Quickstart (orchestrator)

The easiest way to run the full pipeline is with the unified orchestrator:

```bash
# Default: phases 1, 3, 4, 5, 6 (local Bronze source)
python run_pipeline.py

# Skip generation if seed data already exists
python run_pipeline.py --phases 3 4 5 6

# Load Bronze from S3 (requires Phase 2 / AWS credentials)
python run_pipeline.py --phases 1 2 3 4 5 6 --source s3

# Validate ground truth only (no Snowflake calls)
python run_pipeline.py --phases 6 --dry-run

# Full run including Streamlit launch
python run_pipeline.py --launch-app
```

The orchestrator runs phases in ascending order, performs pre-flight checks, and aborts
with a clear message on any failure.

---

## Manual Phase Reference

Use these commands when re-running individual phases or debugging.

### Phase 1 — Generate Seed Data

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

### Phase 2 — Deliver to S3 (optional)

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

Skip this step if using local file loading (default mode).

---

### Phase 3 — Bronze Ingest

```bash
# Create Bronze tables (first run only — worksheet or SnowSQL)
# snowsql -f pipeline_naive/ddl_bronze.sql

# Load from local files (default — no AWS required)
python pipeline_naive/load_bronze.py

# OR load from S3 external stage (requires Step 0 S3 setup + Phase 2)
python pipeline_naive/load_bronze.py --source s3
```

**Expected row counts:** TOPAZ=12,388 / EMERALD=4,886 / RUBY=4,886 / STUB=170

---

### Phase 4 — dbt Transforms (Silver → Naive Gold + Semantic Gold)

```bash
cd dbt
dbt seed      # loads canonical account + security masters to GOLD schema
dbt run       # builds SILVER, GOLD_NAIVE, GOLD models
dbt test      # validates schema contracts for all three layers
cd ..
```

**What dbt builds:**

| Schema | Models | Description |
|--------|--------|-------------|
| `SILVER` | `positions_integrated` | Naive union of all 3 sources (A7-A11 embedded) |
| `GOLD_NAIVE` | `positions_naive` | Assumption-based Gold — 5 wrong assumptions |
| `GOLD` | `dw_account`, `dw_security`, `dw_position`, `dw_trade_lot` | Governed DW tables |

**Expected test results:**
- Silver: allows ~15% NULL on `security_master_id` (A8 — by design)
- Naive Gold: allows NULL `security_master_id` (dropped rows inflate some aggregates)
- Semantic Gold: zero NULLs on PKs, FKs, and `unrealized_gain_loss`

---

### Phase 5 — Stage Cortex Analyst YAML Files

```bash
python pipeline_semantic/load_gold.py
```

Uploads all four semantic model YAMLs to `@GOLD.GSF_GOLD_STAGE/semantic/`:
- `positions_bronze.yaml` — Bronze tier (thin, fragmented schemas)
- `positions_silver.yaml` — Silver tier (A7-A11 embedded)
- `positions_gold_naive.yaml` — Naive Gold tier (wrong assumptions)
- `positions_gold.yaml` — Semantic Gold tier (resolves A1-A11)

---

### Phase 6 — Cortex Analyst Query (manual)

```bash
# Gate question — governed Gold model
python cortex/query_cortex.py --model gold
# Expected: George Group Trust / $47,944,909.80

# Same question against other tiers
python cortex/query_cortex.py --model gold_naive
python cortex/query_cortex.py --model silver
python cortex/query_cortex.py --model bronze

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

### Phase 7 — Variance Comparison + Visualization

```bash
# Verify ground truth only (no Snowflake calls)
python variance/runner.py --dry-run

# Run all 11 questions against all 4 model tiers (~2-4 min)
# Saves timestamped JSON to variance/results/
python variance/runner.py

# Run specific model(s) only
python variance/runner.py --model gold
python variance/runner.py --model silver gold_naive

# Launch Streamlit app
streamlit run app/streamlit_app.py
```

The Streamlit app re-scores from raw JSON on every load — comparator fixes apply
immediately without re-running Cortex.

**Expected scorecard:**

| Tier | Score | Why |
|------|-------|-----|
| Bronze | 1/11 (9%) | Fragmented schemas — most questions unanswerable (no cross-source joins, no `account_id`) |
| Silver | 0/11 (0%) | Looks integrated but A7-A11 embedded — returns plausible but wrong answers for all 11 questions |
| Naive Gold | 0/11 (0%) | Well-structured dbt Gold layer but no semantic model — same confident failures as Silver |
| Semantic Gold | 11/11 (100%) | Semantic model resolves all 11 ambiguities explicitly |

The Silver and Naive Gold 0/11 scores are the demo's core argument: you cannot fix AI-generated errors with better ETL alone.
Governance at the semantic layer is the key variable.

---

## Recovery: Re-running After a Break

Use this section when the pipeline is in an unknown state and you need to restore a clean demo environment.

### What to re-run and when

| Symptom | Fix |
|---------|-----|
| Bronze tables empty or wrong row counts | Re-run Phase 3: `python pipeline_naive/load_bronze.py` |
| Silver / Gold tables missing or stale | Re-run Phase 4: `cd dbt && dbt run` |
| dbt tests fail | Diagnose with `dbt test --select <model_name>` then re-run `dbt run` |
| Cortex returns wrong answers | Re-run Phase 5 (restage YAMLs): `python pipeline_semantic/load_gold.py` |
| Variance scores look wrong | Re-run Phase 7: `python variance/runner.py` |
| Everything is suspect | Full reset — see below |

### Full reset (clean slate)

Drops and rebuilds all Snowflake tables. Run from the project root with `.venv` active and `.env` loaded.

```bash
# Step 1 — Regenerate seed data (skip if data/seed_v2/ already exists)
python -m generator_v2.generator --validate

# Step 2 — Reload Bronze
python pipeline_naive/load_bronze.py

# Step 3 — Rebuild Silver, Naive Gold, Semantic Gold via dbt
cd dbt && dbt run && dbt test && cd ..

# Step 4 — Restage all Cortex Analyst YAML files
python pipeline_semantic/load_gold.py

# Step 5 — Validate Gold DW tables (row counts, FK integrity, NULL checks)
python pipeline_semantic/validate_gold.py

# Step 6 — Re-run variance comparison
python variance/runner.py

# Step 7 — Launch Streamlit
streamlit run app/streamlit_app.py
```

Or use the orchestrator to run phases 3-7 in one command:

```bash
python run_pipeline.py --phases 3 4 5 6
```

### Diagnosing dbt failures

```bash
cd dbt

# See which tests are failing
dbt test

# Re-run a single model
dbt run --select dw_position

# Check compiled SQL (what dbt actually sent to Snowflake)
cat target/compiled/gsf_demo/models/gold_semantic/dw_position.sql

# Full parse (clears dbt's partial-parse cache)
dbt run --no-partial-parse
```

### Checking Snowflake directly

```sql
-- Quick row count audit
SELECT 'BRONZE.TOPAZ_POSITIONS'   , COUNT(*) FROM GSF_DEMO.BRONZE.TOPAZ_POSITIONS   UNION ALL
SELECT 'BRONZE.EMERALD_POSITIONS' , COUNT(*) FROM GSF_DEMO.BRONZE.EMERALD_POSITIONS UNION ALL
SELECT 'BRONZE.RUBY_POSITIONS'    , COUNT(*) FROM GSF_DEMO.BRONZE.RUBY_POSITIONS    UNION ALL
SELECT 'BRONZE.SECURITY_MASTER_STUB', COUNT(*) FROM GSF_DEMO.BRONZE.SECURITY_MASTER_STUB UNION ALL
SELECT 'SILVER.POSITIONS_INTEGRATED', COUNT(*) FROM GSF_DEMO.SILVER.POSITIONS_INTEGRATED UNION ALL
SELECT 'GOLD.DW_ACCOUNT'          , COUNT(*) FROM GSF_DEMO.GOLD.DW_ACCOUNT          UNION ALL
SELECT 'GOLD.DW_SECURITY'         , COUNT(*) FROM GSF_DEMO.GOLD.DW_SECURITY         UNION ALL
SELECT 'GOLD.DW_POSITION'         , COUNT(*) FROM GSF_DEMO.GOLD.DW_POSITION         UNION ALL
SELECT 'GOLD.DW_TRADE_LOT'        , COUNT(*) FROM GSF_DEMO.GOLD.DW_TRADE_LOT        UNION ALL
SELECT 'GOLD_NAIVE.POSITIONS_NAIVE', COUNT(*) FROM GSF_DEMO.GOLD_NAIVE.POSITIONS_NAIVE;

-- Expected: 12388 / 4886 / 4886 / 170 / 22160 / 100 / 200 / 4886 / 12388 / 12393

-- Check what's staged in the semantic model stage
LIST @GSF_DEMO.GOLD.GSF_GOLD_STAGE/semantic/;
-- Expected: 4 YAML files (positions_bronze, positions_silver, positions_gold_naive, positions_gold)
```

---

## SnowSQL Reference

```bash
snowsql -a WYXTVOC-AEB50319 -u DAVIDLOWE80NWL -f infrastructure/snowflake_setup.sql
```

Or configure `~/.snowsql/config` with account credentials.
