# GSF Semantic Pipeline

Portfolio demo proving the semantic layer is a **bias control mechanism** for AI-generated
results. Three synthetic legacy source systems (Topaz, Emerald, Ruby) produce position data
with different schemas. Two pipelines process the same data through seven lifecycle phases:

- **Naive Pipeline:** Bronze → Silver → Gold (assumption-based transforms, semantically broken)
- **Semantic Enriched Pipeline:** Bronze → Silver → Gold (semantic-layer-guided, correct)

Cortex Analyst queries both. Wrong answers vs correct answers. Streamlit shows the difference.

**GitHub:** https://github.com/lownstar/GSF_Semantci_Pipeline

---

## Naming Conventions

- All Snowflake objects use the `GSF` (Gemstone Financial) prefix
- Database: `GSF_DEMO` | Warehouse: `GSF_WH` | Role: `GSF_ROLE`
- Schemas: `BRONZE`, `SILVER`, `GOLD` (active), `GOLD_NAIVE` (planned — Step 5)
- Generator is deterministic (seed=42); ground truth always from `data/seed_v2/` CSVs

---

## Key Directories

| Directory | Phase | Purpose |
|---|---|---|
| `generator_v2/` | 1 | Deterministic seed data generator (9 CSVs, seed=42) |
| `delivery/` | 2 | S3 delivery module — boto3 upload to landing zone |
| `pipeline_naive/` | 3-4 | Naive Pipeline: Bronze ingest + Silver ETL scripts |
| `pipeline_semantic/` | 5 | Semantic Enriched Pipeline: Gold DW + semantic model |
| `infrastructure/` | 0 | One-time Snowflake + S3 setup SQL (run as ACCOUNTADMIN) |
| `semantic_model/` | 5-6 | Cortex Analyst YAML files (positions.yaml + positions_silver.yaml) |
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
| 4 | dbt integration (Silver + Gold models) | Next |
| 5 | Naive Gold layer (GOLD_NAIVE schema) | Planned |
| 6 | Separate semantic model staging | Planned |
| 7 | Unified orchestrator (run_pipeline.py) | Planned |
| 8 | Enhanced multi-page Streamlit app | Planned |
| 9 | Documentation polish | Planned |

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

# Phase 4: Run Silver ETL (Snowflake worksheet)
# snowsql -f pipeline_naive/etl_silver.sql

# Phase 5: Load Gold
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
Account: `WYXTVOC-AEB50319` | User: `DAVIDLOWE80NWL`
See `docs/runbook.md` for key generation and registration steps.

---

## Epic 6 Plug-in Point

The AI Bias Analysis Tool (Epic 6) is a separate project. Input contract:
`variance/results/*.json`. Future orchestrator flag: `--with-bias-analysis`.
