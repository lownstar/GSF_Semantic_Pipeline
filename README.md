# GSF Semantic Pipeline

[![Live Demo](https://img.shields.io/badge/Live%20Demo-Streamlit-FF4B4B)](https://gsfsemanticpipeline-lownstar.streamlit.app/)
[![GitHub](https://img.shields.io/badge/GitHub-lownstar%2FGSF__Semantic__Pipeline-181717?logo=github)](https://github.com/lownstar/GSF_Semantic_Pipeline)

A portfolio demonstration proving that the Snowflake semantic layer is not an optional
convenience — it is a **governance mechanism** for AI-generated results.

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
| 6 | Documentation polish | Complete |

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

### 3. Run the pipeline

The orchestrator runs all phases in sequence:

```bash
# Default run — phases 1, 3, 4, 5, 6 (local Bronze source, no S3 required)
python run_pipeline.py

# Load Bronze from S3 instead (requires Phase 2 / AWS credentials)
python run_pipeline.py --phases 1 2 3 4 5 6 --source s3

# Skip generation if seed data already exists
python run_pipeline.py --phases 3 4 5 6

# Validate ground truth only (no Snowflake calls)
python run_pipeline.py --phases 6 --dry-run

# Launch Streamlit after the run
python run_pipeline.py --launch-app
```

See [docs/runbook.md](docs/runbook.md) for the full manual step-by-step reference and
one-time infrastructure setup instructions.

Gate question: *"What is the total market value of account ACC-0042?"*
- Semantic Gold: George Group Trust / **$47,944,909.80** (correct)
- Naive Gold / Silver / Bronze: wrong or no data (A4 — account ID fragmentation)

---

## Project Structure

```
gsf-semantic-pipeline/
├── run_pipeline.py         # Unified orchestrator — single entry point for all phases
├── generator_v2/           # Phase 1: Deterministic seed data generator (seed=42)
├── delivery/               # Phase 2: S3 delivery module (boto3)
│   ├── deliver.py          # Upload source CSVs to S3 landing zone
│   └── config.py           # Bucket, prefix, file mapping config
├── pipeline_naive/         # Bronze ingest scripts
│   ├── load_bronze.py      # Phase 3: Bronze ingest (PUT/COPY — local or S3)
│   ├── ddl_bronze.sql      # Bronze table DDL (reference — run once in Snowsight)
│   ├── ddl_silver.sql      # Silver table DDL (reference)
│   └── etl_silver.sql      # Legacy ETL reference (replaced by dbt Silver model)
├── dbt/                    # Phase 4: All Silver → Gold transforms
│   ├── dbt_project.yml     # Project config (schemas: SILVER / GOLD_NAIVE / GOLD)
│   ├── profiles.yml        # Snowflake connection (reads from .env)
│   ├── models/
│   │   ├── sources.yml     # Bronze source declarations
│   │   ├── silver/         # SILVER.POSITIONS_INTEGRATED (22,160 rows)
│   │   ├── gold_naive/     # GOLD_NAIVE.POSITIONS_NAIVE (assumption-based)
│   │   └── gold_semantic/  # GOLD.DW_ACCOUNT / DW_SECURITY / DW_POSITION / DW_TRADE_LOT
│   ├── seeds/              # Canonical account + security masters (loaded to GOLD)
│   └── macros/             # generate_schema_name.sql (exact schema name override)
├── pipeline_semantic/      # Phase 5: YAML staging only (dbt owns table population)
│   ├── load_gold.py        # Stage all 4 Cortex Analyst YAMLs to @GSF_GOLD_STAGE
│   ├── validate_gold.py    # Gold DW validation (row counts, FKs, NULLs)
│   └── setup_gold.sql      # Gold DDL reference (run once — dbt creates actual tables)
├── infrastructure/         # One-time Snowflake setup SQL (run as ACCOUNTADMIN)
│   ├── snowflake_setup.sql # Database, schemas, warehouse, role
│   ├── cortex_setup.sql    # Cortex grants, Horizon governance tags
│   └── s3_external_stage.sql # S3 storage integration + external stage
├── semantic_model/         # Cortex Analyst YAML files (one per tier)
│   ├── positions_bronze.yaml      # Bronze — raw, fragmented
│   ├── positions_silver.yaml      # Silver — integrated but A7-A11 embedded
│   ├── positions_gold_naive.yaml  # Naive Gold — assumption-based
│   └── positions_gold.yaml        # Semantic Gold — governed, resolves A1-A11
├── cortex/                 # Phase 6: Cortex Analyst query runner
│   └── query_cortex.py     # REST API; --model gold|gold_naive|silver|bronze
├── variance/               # Phase 7: Variance comparison
│   ├── questions.py        # 11-question bank with ground truth functions
│   ├── ground_truth.py     # Computes ground truth from seed CSVs (no Snowflake)
│   ├── comparator.py       # CORRECT/WRONG/NO_DATA/ERROR scoring
│   ├── runner.py           # Orchestrates 4-model run, saves timestamped JSON
│   ├── results/            # JSON output from each runner.py invocation
│   │   └── demo_results.json  # Canonical showcase result (gold 11/11) — committed
├── app/                    # Phase 7: Streamlit visualization
│   └── streamlit_app.py    # Four-tier scorecard; tier visibility toggles; tier-aware failure narratives
├── requirements-app.txt    # Lightweight deps for Streamlit Community Cloud deployment
├── data/seed_v2/           # Generated CSVs (reproducible via generator_v2, seed=42)
└── docs/                   # Project documentation
    ├── architecture.md          # Lifecycle phases, data flow, Snowflake objects
    ├── ambiguity_registry_v2.md # All 11 ambiguities (A1-A11)
    ├── runbook.md               # Step-by-step pipeline execution + recovery
    ├── decisions.md             # Architectural decision log
    ├── epic_history.md          # Epic 1-5 completion history (PoC phase)
    └── refactoring_changelog.md # What changed and why (PoC → portfolio)
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
| Schema | `GSF_DEMO.GOLD_NAIVE` | Active |
| Stage | `@BRONZE.GSF_BRONZE_STAGE` | Active (local loads) |
| Stage | `@BRONZE.GSF_S3_LANDING` | Active (DDL in infrastructure/s3_external_stage.sql) |
| Stage | `@GOLD.GSF_GOLD_STAGE` | Active (semantic YAMLs) |

---

## Tech Stack

| Phase | Technology |
|---|---|
| Phase 1: Generation | Python (Faker, deterministic seed=42) |
| Phase 2: Delivery | AWS S3 + boto3 |
| Phase 3: Bronze Ingest | Snowflake (PUT + COPY INTO) |
| Phase 4: Silver → Gold Transform | dbt (Silver, Naive Gold, Semantic Gold models) |
| Phase 5: Gold Enrichment | Cortex Analyst YAML (staged to Snowflake internal stage) |
| Phase 6: AI Querying | Snowflake Cortex Analyst (natural language → SQL) |
| Phase 7: Analysis | Python + Streamlit |
| Hosting | Streamlit Community Cloud (static mode — no live Snowflake) |
