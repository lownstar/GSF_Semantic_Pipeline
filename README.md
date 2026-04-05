# GSF Semantic Pipeline

A portfolio demonstration proving that the Snowflake semantic layer is not an optional
convenience — it is a **bias control mechanism** for AI-generated results.

Three synthetic legacy source systems (Topaz, Emerald, Ruby) each produce position data
in their own physical schema with different identifiers, column names, price sources, and
grains. Both pipelines process the same data through the same Bronze and Silver layers,
then diverge: the Naive pipeline builds Gold on assumptions, the Semantic Enriched
pipeline builds Gold governed by a semantic model. Cortex Analyst queries both — one
returns wrong answers, the other returns correct ones.

**Portfolio angle:** Governed data is the prerequisite for trustworthy AI.
**Snowflake features:** Cortex Analyst, semantic model (YAML), Horizon governance.

---

## Architecture

```
LEGACY SOURCE SYSTEMS
  Topaz (Custodian)     — CUSIP, lot-level, custodian EOD price
  Emerald (Portfolio)   — ticker, position-level, PM evaluated price
  Ruby (Fund Acctg)     — ISIN, position-level, NAV price
        |
        v Phase 1: Generation
  generator_v2 -> 9 seed CSVs (data/seed_v2/)
        |
        v Phase 2: Delivery
  delivery/deliver.py -> S3 landing zone (topaz/ emerald/ ruby/ reference/)
        |
        v Phase 3: Bronze Ingest (shared)
  BRONZE.TOPAZ_POSITIONS / EMERALD_POSITIONS / RUBY_POSITIONS / SECURITY_MASTER_STUB
        |
        v Phase 4: Silver Transform (shared)
  SILVER.POSITIONS_INTEGRATED — naive ETL union, looks normalized, semantically broken (A7-A11)
        |
        +--- Naive Pipeline ---------> Phase 5: GOLD_NAIVE (assumption-based DW)
        |                              Phase 6: Cortex Analyst -> confident but wrong
        |
        +--- Semantic Enriched ------> Phase 5: GOLD (governed DW + semantic model YAML)
             Pipeline                  Phase 6: Cortex Analyst -> correct answers
                                                |
                                                v Phase 7: Analysis
                                        Variance scoring + Streamlit app
```

The core demo question: without semantic governance, can you trust even your Gold layer?
The Naive Gold tables look like a proper star schema but carry Silver-layer integrity
problems forward. The Semantic Enriched pipeline resolves them. Cortex Analyst shows
the difference.

---

## Refactoring Status

This project is actively being refactored from proof-of-concept (Epics 1-5) to a
portfolio-grade pipeline demo. See [docs/refactoring_changelog.md](docs/refactoring_changelog.md).

| Step | Description | Status |
|---|---|---|
| 1 | V1 cleanup + pipeline rename (Naive / Semantic Enriched) | Complete |
| 2 | Extract infrastructure SQL + refactor docs | Complete |
| 3 | S3 delivery layer (boto3, external stage) | Complete |
| 4 | dbt integration — four-tier comparison (Bronze/Silver/Naive Gold/Semantic Gold) | Complete |
| 5 | Unified orchestrator (run_pipeline.py) | Complete |
| 6 | Documentation polish | Next |

**Epics completed (PoC phase):** 1 (data generator), 2 (Naive Pipeline), 3 (Semantic
Pipeline), 4 (Cortex Analyst), 5 (variance + Streamlit).

---

## Quick Start

### 1. Prerequisites

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in Snowflake credentials.
Key-pair auth is required (Duo MFA blocks password auth for scripted runs).
See [docs/runbook.md](docs/runbook.md) for setup.

### 2. One-time Snowflake setup (ACCOUNTADMIN)

```sql
-- Run in Snowflake worksheet:
-- infrastructure/snowflake_setup.sql   (database, schemas, warehouse, role)
-- infrastructure/cortex_setup.sql      (Cortex grants, Horizon tags)
```

### 3. Generate seed data

```bash
python -m generator_v2.generator --validate
# Produces 9 CSVs in data/seed_v2/ — all 21 integrity checks pass
```

### 4. Deliver to S3 (optional — requires AWS credentials in .env)

```bash
python delivery/deliver.py
# Uploads source CSVs to s3://gsf-demo-landing/ with per-source prefixes
```

### 5. Load Bronze + Silver

```bash
# Option A: local files (no AWS needed)
python pipeline_naive/load_bronze.py

# Option B: from S3 external stage
python pipeline_naive/load_bronze.py --source s3

# Run naive ETL (Snowflake worksheet or SnowSQL):
# snowsql -f pipeline_naive/etl_silver.sql

python pipeline_naive/validate_silver.py
```

### 6. Load Gold + semantic model

```bash
# snowsql -f pipeline_semantic/setup_gold.sql
python pipeline_semantic/load_gold.py
python pipeline_semantic/validate_gold.py
```

### 7. Run variance comparison + visualize

```bash
python variance/runner.py
streamlit run app/streamlit_app.py
```

Gate question: *"What is the total market value of account ACC-0042?"*
- Gold: George Group Trust / **$47,944,909.80** (correct)
- Silver: no data (A4 — account ID fragmentation)

---

## Project Structure

```
gsf-semantic-pipeline/
├── generator_v2/           # Phase 1: Deterministic seed data generator (seed=42)
├── delivery/               # Phase 2: S3 delivery module (boto3)
│   ├── deliver.py          # Upload source CSVs to S3 landing zone
│   └── config.py           # Bucket, prefix, file mapping config
├── pipeline_naive/         # Naive Pipeline scripts
│   ├── load_bronze.py      # Phase 3: Bronze ingest (local or S3)
│   ├── validate_silver.py  # Silver validation (SC1-SC9)
│   ├── ddl_bronze.sql      # Bronze table DDL
│   ├── ddl_silver.sql      # Silver table DDL
│   └── etl_silver.sql      # Phase 4: Naive ETL -> POSITIONS_INTEGRATED
├── pipeline_semantic/      # Semantic Enriched Pipeline scripts
│   ├── load_gold.py        # Phase 5: Gold DW load + stage YAMLs
│   ├── validate_gold.py    # Gold validation (GC1-GC12)
│   └── setup_gold.sql      # Gold table DDL + stage creation
├── infrastructure/         # One-time Snowflake setup SQL
│   ├── snowflake_setup.sql # Database, schemas, warehouse, role
│   ├── cortex_setup.sql    # Cortex grants, Horizon governance tags
│   └── s3_external_stage.sql # S3 storage integration + external stage
├── semantic_model/         # Cortex Analyst YAML files (one per tier)
│   ├── positions_bronze.yaml      # Bronze — raw, fragmented
│   ├── positions_silver.yaml      # Silver — naive, A7-A11 embedded
│   ├── positions_gold_naive.yaml  # Naive Gold — assumption-based
│   └── positions_gold.yaml        # Semantic Gold — governed, resolves A1-A11
├── cortex/                 # Phase 6: Cortex Analyst query runner
│   └── query_cortex.py     # REST API; --model gold|gold_naive|silver|bronze
├── variance/               # Phase 7: Variance comparison
│   ├── questions.py        # 11-question bank (one per ambiguity A1-A11)
│   ├── ground_truth.py     # Ground truth from seed CSVs (no Snowflake)
│   ├── comparator.py       # CORRECT/WRONG/NO_DATA/ERROR scoring
│   └── runner.py           # Orchestrates questions, saves results JSON
├── app/                    # Phase 7: Streamlit visualization
│   └── streamlit_app.py    # Scorecard, bar chart, detail tables
├── data/seed_v2/           # Generated CSVs (reproducible via generator_v2)
└── docs/                   # Project documentation
    ├── architecture.md     # Lifecycle phases, data flow, Snowflake objects
    ├── ambiguity_registry_v2.md  # All 11 ambiguities (A1-A11)
    ├── runbook.md          # Step-by-step pipeline execution
    ├── decisions.md        # Architectural decision log
    ├── epic_history.md     # Epic 1-5 completion history
    └── refactoring_changelog.md  # What changed and why
```

---

## The 11 Ambiguities

| # | Name | Layer | Demo Question |
|---|---|---|---|
| A1 | Security ID fragmentation | Raw | Total quantity in Apple across all accounts? |
| A2 | Price source divergence | Raw | Total market value of ACC-0042 at month-end? |
| A3 | Column name heterogeneity | Raw | Positions with unrealized losses > $10K? |
| A4 | Account ID fragmentation | Raw | Total market value of account ACC-0042? |
| A5 | Position grain mismatch | Raw | Total quantity of fixed income? |
| A6 | Date field semantics | Raw | All positions as of December 31, 2024? |
| A7 | Mixed-grain record IDs | Silver | Total fixed income quantity across portfolios? |
| A8 | Unmastered security IDs | Silver | Total market value of all fixed income? |
| A9 | Cost basis fragmentation | Silver | Total unrealized gain for equity sleeve? |
| A10 | Asset class classification gap | Silver | What % of AUM is fixed income? |
| A11 | NULL unrealized G/L from Ruby | Silver | Positions with unrealized losses > $10K? |

See [docs/ambiguity_registry_v2.md](docs/ambiguity_registry_v2.md) for full detail.

---

## Snowflake Objects

| Object | Name | Status |
|---|---|---|
| Database | `GSF_DEMO` | Active |
| Warehouse | `GSF_WH` | Active |
| Role | `GSF_ROLE` | Active |
| Schema | `GSF_DEMO.BRONZE` | Active |
| Schema | `GSF_DEMO.SILVER` | Active |
| Schema | `GSF_DEMO.GOLD` | Active |
| Schema | `GSF_DEMO.GOLD_NAIVE` | Planned (Step 5) |
| Stage | `@BRONZE.GSF_BRONZE_STAGE` | Active (local loads) |
| Stage | `@BRONZE.GSF_S3_LANDING` | Planned (Step 3 SQL, needs IAM setup) |
| Stage | `@GOLD.GSF_GOLD_STAGE` | Active (semantic YAMLs) |

---

## Tech Stack

| Phase | Technology |
|---|---|
| Phase 1: Generation | Python (Faker, deterministic seed=42) |
| Phase 2: Delivery | AWS S3 + boto3 |
| Phase 3-4: Ingest + Transform | Snowflake (COPY INTO, SQL ETL / dbt planned) |
| Phase 5: Gold Enrichment | Snowflake + Cortex Analyst semantic model YAML |
| Phase 6: AI Querying | Snowflake Cortex Analyst (natural language → SQL) |
| Phase 7: Analysis | Python + Streamlit |
| Hosting | AWS (planned) |
