# Refactoring Changelog

Documents the transition from PoC (Epics 1-5) to portfolio-grade pipeline demo.
The baseline commit (`3e6e175`) captures the full PoC state before any changes.

---

## Step 1: V1 Cleanup + Pipeline Rename (2026-04-04)

### Deleted (V1 deprecated artifacts)

| Path | What it was | Why deleted |
|---|---|---|
| `generator/` | V1 data generator (replaced by `generator_v2/`) | Dead code; V2 is the only active generator |
| `data/seed/` | V1 seed CSVs (~58 MB, 9 files) | Replaced by `data/seed_v2/`; V1 schema was different |
| `data/schema/schema_definition.md` | V1 schema documentation | Superseded by `docs/ambiguity_registry_v2.md` |
| `docs/ambiguity_registry.md` | V1 ambiguity registry | Replaced by `docs/ambiguity_registry_v2.md` (A1-A11) |

All deleted files are preserved in git history (baseline commit).

### Renamed

| From | To | Why |
|---|---|---|
| `pipeline_a/` | `pipeline_naive/` | Aligns with demo narrative: "Naive Pipeline" (no semantic governance) |
| `pipeline_b/` | `pipeline_semantic/` | Aligns with demo narrative: "Semantic Enriched Pipeline" |

### Updated references

All "Pipeline A" references changed to "Naive Pipeline" across:
- `pipeline_naive/load_bronze.py` (docstring, print statements, stage comment)
- `pipeline_naive/validate_silver.py` (docstring, print statement)
- `generator_v2/config.py` (comments)
- `generator_v2/generator.py` (comments)
- `generator_v2/models/sources.py` (comments, docstrings)
- `docs/ambiguity_registry_v2.md` (references)
- `semantic_model/positions_gold.yaml` (descriptions)

All "Pipeline B" references changed to "Semantic Enriched Pipeline" across:
- `pipeline_semantic/load_gold.py` (docstring, print statements)
- `pipeline_semantic/validate_gold.py` (docstring, print statement)
- `docs/ambiguity_registry_v2.md` (references)

`README.md` rewritten with new architecture diagram, updated project structure,
and corrected status (Epic 5 marked complete).

---

## Step 2: Extract Infrastructure + Refactor CLAUDE.md (2026-04-04)

### Moved

| From | To | Why |
|---|---|---|
| `pipeline_naive/GSF_setup.sql` | `infrastructure/snowflake_setup.sql` | Consolidate one-time setup SQL |
| `pipeline_naive/setup_cortex.sql` | `infrastructure/cortex_setup.sql` | Consolidate one-time setup SQL |

### Refactored

CLAUDE.md split from 800+ lines into focused documents:
- `CLAUDE.md` (~90 lines): project overview, conventions, quick start
- `docs/architecture.md`: lifecycle phases, data flow, Snowflake objects
- `docs/runbook.md`: step-by-step pipeline execution instructions
- `docs/decisions.md`: architectural decision log with rationale
- `docs/epic_history.md`: Epic 1-5 completion records

---

## Step 3: S3 Delivery Layer (2026-04-04)

### Created

| Path | Purpose |
|---|---|
| `delivery/__init__.py` | Package init |
| `delivery/config.py` | S3 bucket, prefix, and file mapping configuration |
| `delivery/deliver.py` | Phase 2: boto3 upload of source CSVs to S3 landing zone |
| `infrastructure/s3_external_stage.sql` | Snowflake storage integration + external stage DDL |

### Updated

| Path | Change |
|---|---|
| `pipeline_naive/load_bronze.py` | Added `--source s3\|local` flag; S3 loads via external stage COPY INTO |
| `requirements.txt` | Added `boto3>=1.34.0` |
| `.env.example` | Added AWS credential placeholders (commented out) |

---

## Step 4: dbt Integration — Four-Tier Cortex Comparison (2026-04-05)

### Architecture change

Introduced dbt as the transformation layer (Bronze → Silver → Gold), replacing the
hand-rolled SQL ETL scripts. Added a **four-tier comparison** to the demo:

| Tier | Schema | Semantic Model | Expected Cortex result |
|------|--------|---------------|----------------------|
| Bronze | BRONZE | positions_bronze.yaml | Rough — fragmented, no joins |
| Silver | SILVER | positions_silver.yaml | Better but ambiguities A7–A11 |
| Naive Gold | GOLD_NAIVE | positions_gold_naive.yaml | Assumption-based, possibly worse |
| Semantic Gold | GOLD | positions_gold.yaml | Correct answers |

The new narrative: *"Your org already has a dbt pipeline producing a Gold layer.
Here's why Cortex still gets it wrong — and what the semantic model adds."*

### Created

| Path | Purpose |
|------|---------|
| `dbt/dbt_project.yml` | dbt project config — schemas SILVER / GOLD_NAIVE / GOLD |
| `dbt/profiles.yml` | Snowflake connection (key-pair auth from env vars) |
| `dbt/macros/generate_schema_name.sql` | Overrides default schema naming to use exact schema names |
| `dbt/seeds/account_master_full.csv` | 100 canonical accounts (copy of data/seed_v2/dw_account.csv) |
| `dbt/seeds/security_master_full.csv` | 200 canonical securities (copy of data/seed_v2/dw_security.csv) |
| `dbt/seeds/schema.yml` | Seed documentation + PK/unique tests |
| `dbt/models/sources.yml` | Bronze source declarations (4 tables) |
| `dbt/models/silver/positions_integrated.sql` | Replaces pipeline_naive/etl_silver.sql |
| `dbt/models/silver/schema.yml` | Silver column docs + tests (replaces validate_silver.py) |
| `dbt/models/gold_naive/positions_naive.sql` | Assumption-based Gold — 5 wrong assumptions applied |
| `dbt/models/gold_naive/schema.yml` | Naive Gold column docs + tests |
| `dbt/models/gold_semantic/dw_account.sql` | Canonical account dimension (from seed) |
| `dbt/models/gold_semantic/dw_security.sql` | Full 200-row security master (from seed) |
| `dbt/models/gold_semantic/dw_position.sql` | Governed position fact (all ambiguities resolved) |
| `dbt/models/gold_semantic/dw_trade_lot.sql` | Lot-level detail (Topaz only) |
| `dbt/models/gold_semantic/schema.yml` | Gold column docs + FK/PK tests (replaces validate_gold.py) |
| `semantic_model/positions_bronze.yaml` | Cortex Analyst model for Bronze tier (thin, 3 tables) |
| `semantic_model/positions_gold_naive.yaml` | Cortex Analyst model for Naive Gold tier |

### Updated

| Path | Change |
|------|--------|
| `pipeline_semantic/load_gold.py` | Stripped to YAML staging only — dbt now owns all DW table population |
| `cortex/query_cortex.py` | Added bronze and gold_naive model keys; generalized _ensure_staged() |
| `variance/runner.py` | Extended to 4-model run (bronze, silver, gold_naive, gold) |
| `app/streamlit_app.py` | Four-tier scorecard, bar chart, and per-question comparison |
| `requirements.txt` | Added `dbt-snowflake>=1.11.0` |

### What dbt replaces

| Previously | Now |
|-----------|-----|
| `pipeline_naive/etl_silver.sql` (manual SQL) | `dbt/models/silver/positions_integrated.sql` |
| `pipeline_naive/validate_silver.py` (custom checks) | `dbt/models/silver/schema.yml` (dbt tests) |
| `pipeline_semantic/setup_gold.sql` (transforms) | `dbt/models/gold_semantic/*.sql` |
| `pipeline_semantic/validate_gold.py` (custom checks) | `dbt/models/gold_semantic/schema.yml` (dbt tests) |
| `pipeline_semantic/load_gold.py` (CSV loads) | `dbt seed` + `dbt run` |

### What Python still owns

- `pipeline_naive/load_bronze.py` — PUT/COPY file operations (dbt cannot do this)
- `pipeline_semantic/load_gold.py` — YAML staging to `@GSF_GOLD_STAGE/semantic/`
- `cortex/query_cortex.py` — Cortex Analyst REST API calls
- `variance/runner.py` — scoring and JSON output

### Semantic model file rename (same session)

Renamed `positions.yaml` → `positions_gold.yaml` to complete the `positions_{layer}.yaml`
naming convention across all four tiers. All references updated throughout the codebase.

| Before | After |
|--------|-------|
| `semantic_model/positions.yaml` | `semantic_model/positions_gold.yaml` |
| `semantic_model/positions_naive.yaml` | `semantic_model/positions_gold_naive.yaml` |

Full naming set:
- `positions_bronze.yaml` — Bronze tier
- `positions_silver.yaml` — Silver tier
- `positions_gold_naive.yaml` — Naive Gold tier
- `positions_gold.yaml` — Semantic Gold tier

---

## Step 5: Unified Orchestrator (2026-04-05)

### Created

| Path | Purpose |
|------|---------|
| `run_pipeline.py` | End-to-end pipeline orchestrator — single entry point for all 7 phases |

### Design

Replaces the manual command sequence in `docs/runbook.md` with a single script.
All phase scripts invoked via `subprocess` + `sys.executable` (venv-safe, loosely coupled).

```
python run_pipeline.py [--phases N ...] [--source local|s3] [--dry-run] [--launch-app]
```

Default phases: `1 3 4 5 6` (Phase 2 requires AWS credentials; Phase 7 prints Streamlit
launch instructions rather than blocking).

| Flag | Purpose |
|------|---------|
| `--phases 1 3 4 5 6` | Which phases to run (default: 1 3 4 5 6) |
| `--source local\|s3` | Phase 3 Bronze source (default: local) |
| `--dry-run` | Phase 6: ground truth only, skip Cortex API |
| `--launch-app` | Phase 7: launch Streamlit (blocking) |

Pre-flight checks: Phase 2 aborts if no AWS credentials found; Phase 3 (local) aborts if
seed CSVs are missing and suggests running Phase 1 first. Phase 4 runs `dbt seed`,
`dbt run`, `dbt test` sequentially with `cwd=dbt/`. Phases run in ascending order
regardless of CLI order.
