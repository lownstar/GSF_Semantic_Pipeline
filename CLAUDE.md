# GSF Semantic Pipeline

Portfolio demo proving the semantic layer is a **bias control mechanism** for AI-generated
results. Three synthetic legacy source systems (Topaz, Emerald, Ruby) produce position data
with different schemas. Two pipelines process the same data:

- **Naive Pipeline:** Bronze -> Silver -> Gold (assumption-based, semantically broken)
- **Semantic Enriched Pipeline:** Bronze -> Silver -> Gold (governed, correct)

Cortex Analyst queries both. The Naive pipeline produces wrong answers; the Semantic
Enriched pipeline produces correct answers. The Streamlit app visualizes the difference.

---

## Naming Conventions

- All Snowflake objects use the `GSF` (Gemstone Financial) prefix
- Database: `GSF_DEMO` | Warehouse: `GSF_WH` | Role: `GSF_ROLE`
- Schemas: `BRONZE`, `SILVER`, `GOLD`, `GOLD_NAIVE`
- Generator is deterministic (seed=42); ground truth from `data/seed_v2/` CSVs

## Key Directories

| Directory | Purpose |
|---|---|
| `generator_v2/` | Deterministic seed data generator (9 CSVs) |
| `pipeline_naive/` | Naive Pipeline: Bronze + Silver scripts |
| `pipeline_semantic/` | Semantic Enriched Pipeline: Gold + semantic model |
| `infrastructure/` | One-time Snowflake setup SQL |
| `semantic_model/` | Cortex Analyst YAML files |
| `cortex/` | Cortex Analyst REST API caller |
| `variance/` | 11-question variance comparison |
| `app/` | Streamlit visualization |

## Project Principles

1. **Same data, different schemas, one governed answer** -- the contrast is everything
2. **Ambiguity is designed in** -- raw environment fails in predictable, explainable ways
3. **Architect's perspective** -- explain the *why*, not just the *what*
4. **Portfolio-ready** -- every component explainable to a hiring manager in 90 seconds
5. **Clean-room** -- fully synthetic, no proprietary data

## Running the Pipeline

See `docs/runbook.md` for full instructions.

```bash
# Quick start
python -m generator_v2.generator --validate       # Generate seed data
python pipeline_naive/load_bronze.py              # Load Bronze
python pipeline_semantic/load_gold.py             # Load Gold + stage YAMLs
python variance/runner.py                         # Run variance comparison
streamlit run app/streamlit_app.py                # Launch visualization
```

## Documentation

| Document | Contents |
|---|---|
| `docs/architecture.md` | Data flow, lifecycle phases, Snowflake objects |
| `docs/ambiguity_registry_v2.md` | All 11 ambiguities (A1-A11) |
| `docs/runbook.md` | Step-by-step pipeline execution |
| `docs/decisions.md` | Architectural and technical decisions |
| `docs/epic_history.md` | Epic 1-5 completion history |
| `docs/refactoring_changelog.md` | What changed during the refactoring |

## Epic 6 Plug-in Point

The AI Bias Analysis Tool (Epic 6) is a separate project. It consumes
`variance/results/*.json` as its input contract. The orchestrator will support
a `--with-bias-analysis` flag when Epic 6 is ready.

## Snowflake Auth

Key-pair authentication is required (Duo MFA blocks password auth).
See `docs/runbook.md` for setup. Account: `WYXTVOC-AEB50319`, user: `DAVIDLOWE80NWL`.
