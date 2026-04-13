# GSF Semantic Pipeline

Portfolio demo proving the semantic layer is a **governance mechanism** for AI-generated
results. Three synthetic legacy source systems (Topaz, Emerald, Ruby) produce position data
with different schemas. Two pipelines process the same data through seven lifecycle phases:

Four-tier comparison: **Bronze → Silver → Naive Gold → Semantic Gold**

Cortex Analyst queries all four tiers. The semantic model is what separates correct answers
from confident wrong answers — even at the Gold layer.

**The narrative:** *"Your org already has a dbt pipeline producing a Gold layer. Here's why
Cortex still gets it wrong — and what the semantic model adds."*

**GitHub:** https://github.com/lownstar/GSF_Semantic_Pipeline

---

## Naming Conventions

- All Snowflake objects use the `GSF` (Gemstone Financial) prefix
- Database: `GSF_DEMO` | Warehouse: `GSF_WH` | Role: `GSF_ROLE`
- Schemas: `BRONZE`, `SILVER`, `GOLD`, `GOLD_NAIVE` (all active)
- Generator is deterministic (seed=42); ground truth always from `data/seed_v2/` CSVs

---

## Key Directories

| Directory | Phase | Purpose |
|---|---|---|
| `generator_v2/` | 1 | Deterministic seed data generator (9 CSVs, seed=42) |
| `delivery/` | 2 | S3 delivery module — boto3 upload to landing zone |
| `pipeline_naive/` | 3 | Bronze ingest (load_bronze.py — PUT/COPY only) |
| `pipeline_semantic/` | 5 | YAML staging only (load_gold.py — dbt owns table population) |
| `dbt/` | 4-5 | dbt project — Silver, Naive Gold, Semantic Gold transforms |
| `infrastructure/` | 0 | One-time Snowflake + S3 setup SQL (run as ACCOUNTADMIN) |
| `semantic_model/` | 5-6 | Cortex Analyst YAMLs (all 4 tiers: bronze, silver, naive, gold) |
| `cortex/` | 6 | Cortex Analyst REST API caller |
| `variance/` | 7 | 11-question variance comparison (questions, ground truth, runner) |
| `app/` | 7 | Streamlit visualization |

---

## Refactoring Status (PoC → Portfolio Pipeline)

| Step | Description | Status |
|---|---|---|
| 1 | V1 cleanup + pipeline rename | Done |
| 2 | Extract infrastructure SQL + refactor docs | Done |
| 3 | S3 delivery layer | Done |
| 4 | dbt integration — four-tier comparison (Bronze/Silver/Naive Gold/Semantic Gold) | Done |
| 5 | Unified orchestrator (run_pipeline.py) | Done |
| 6 | Documentation polish | Done |

---

## Project Principles

1. **Same data, different schemas, one governed answer** — the contrast is everything
2. **Ambiguity is designed in** — raw environment fails in predictable, explainable ways
3. **Architect's perspective** — explain the *why*, not just the *what*
4. **Portfolio-ready** — every component explainable to a hiring manager in 90 seconds
5. **Clean-room** — fully synthetic, no proprietary data
6. **Documented refactoring** — every change logged in `docs/refactoring_changelog.md`

---

## Running the Pipeline

See `docs/runbook.md` for full instructions including S3 setup and key-pair auth.

```bash
# Phase 1: Generate seed data
python -m generator_v2.generator --validate

# Phase 2: Deliver to S3 (requires AWS credentials)
python delivery/deliver.py

# Phase 3: Load Bronze (local or S3)
python pipeline_naive/load_bronze.py              # local
python pipeline_naive/load_bronze.py --source s3  # from S3

# Phase 4: dbt transforms (Silver → Naive Gold + Semantic Gold)
cd dbt && dbt seed          # loads canonical reference data to GOLD schema
dbt run                     # builds SILVER, GOLD_NAIVE, GOLD
dbt test                    # validates schema contracts
cd ..

# Phase 5: Stage Cortex Analyst semantic model YAMLs
python pipeline_semantic/load_gold.py

# Phase 6-7: Variance + visualization
python variance/runner.py
streamlit run app/streamlit_app.py
```

---

## Documentation Index

| Document | Contents |
|---|---|
| `docs/architecture.md` | Seven lifecycle phases, data flow, Snowflake objects |
| `docs/ambiguity_registry_v2.md` | All 11 ambiguities (A1-A11) — the design contract |
| `docs/runbook.md` | Step-by-step pipeline execution + S3 + key-pair auth |
| `docs/decisions.md` | Architectural and technical decision log |
| `docs/epic_history.md` | Epic 1-5 completion history (PoC phase) |
| `docs/refactoring_changelog.md` | What was deleted, renamed, created — and why |

---

## Snowflake Auth

Key-pair authentication required (Duo MFA blocks scripted password auth).

Two env vars required — Python scripts and dbt use different key formats:
- `SNOWFLAKE_PRIVATE_KEY_FILE=snowflake_rsa_key.p8` — DER format (Python scripts)
- `SNOWFLAKE_PRIVATE_KEY_PEM=snowflake_rsa_key.pem` — PEM format (dbt)

See `docs/runbook.md` for key generation and registration steps.

---

## Epic 6 Plug-in Point

Epic 6 is a separate project consuming `variance/results/*.json`.
Future orchestrator flag: `--with-analysis`.
