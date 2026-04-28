# Refactoring Changelog

---

## GSF Identity Network — Phase 2: Client Tier & Account Links (2026-04-22)

### Why

Cross-functional work with the GSF_Account_Network project, which visualizes the multi-source
identity ambiguity problem as the "setup" half of a two-demo portfolio narrative. Phase 2 required
a client/household tier and cross-system account relationship data to build a three-tier hierarchy
graph (Client → Account → Source Record).

### What changed

Added three new canonical data assets to the generator without disturbing any existing field values:

**New: `dw_client.csv`** — 25 clients (CLT-001 through CLT-025), each owning exactly 4 accounts.
Fields: `client_id`, `client_name`, `client_type` (Individual, Family Office, Institutional, Endowment).

**Updated: `dw_account.csv`** — two new columns added. All existing values (custodian_account_num,
portfolio_code, fund_code, etc.) are byte-identical to the prior version.
- `client_id` — FK to dw_client
- `strategy_type` — Equities (40), Fixed Income (30), Derivatives (20), Cash (10)

**New: `dw_account_links.csv`** — 20 OTC collateral links. Derivatives strategy accounts require a
separate collateral account at the custodian (Topaz) beyond the standard custody relationship.
Each Derivatives account is paired with a Cash account via round-robin.
Fields: `account_id`, `linked_account_id`, `link_type` (`otc_collateral`).

Isolation technique: new fields use `_rng_ext = random.Random(RANDOM_SEED + 100)` and a separately
seeded Faker instance so the main `rng` sequence is completely unchanged.

### Files Updated

| File | What changed |
|---|---|
| `generator_v2/config.py` | Added `NUM_CLIENTS`, `CLIENT_TYPES`, `STRATEGY_TYPES`, `STRATEGY_DIST`, `DW_CLIENT_FILE`, `DW_ACCOUNT_LINKS_FILE` |
| `generator_v2/models/canonical.py` | Added `_rng_ext`/`_fake_ext` isolated instances; added `generate_dw_client()`; updated `generate_dw_account(dw_client)` signature; added `generate_dw_account_links()` |
| `generator_v2/generator.py` | Wired new functions into orchestration, CSV writes, and `validate()` (5 new VC1–VC5 checks) |
| `data/seed_v2/dw_client.csv` | New — 25 rows |
| `data/seed_v2/dw_account.csv` | Updated — 100 rows, 2 new columns |
| `data/seed_v2/dw_account_links.csv` | New — 20 rows |

### Validation

Generator now runs 26 checks (was 21): V1–V13, VI1–VI5, VS1–VS3, VC1–VC5. All pass.
dbt pipeline (seeds, models, tests) is unaffected — `dbt/seeds/account_master_full.csv` is a
separate file and was not modified.

### Branch

`feat/gsf-identity-network-phase2` — open, pending merge.

---

## Emerald-Topaz Grain Swap (2026-04-22)

### Why

The original grain assignment (Topaz = lot-level, Emerald = position-level) was architecturally
backwards. In practice, OMS systems (Emerald / front office) track individual order executions
as acquisition lots; custodians (Topaz) send daily net position snapshots. Swapping the grain
makes the ambiguity scenario more realistic and defensible to a hiring audience.

### What changed

- **Emerald** is now lot-level (12,388 rows) — the OMS grain
- **Topaz** is now position-level (4,886 rows) — the custodian grain
- `LOT_ID` moved from Topaz schema to Emerald schema in Bronze
- `dw_trade_lot` dbt model now sources from Emerald; `dw_position` Topaz CTE simplified
- A5/A7/A9 failure narratives updated: Emerald is now the grain trap
- All seed CSVs and `demo_results.json` regenerated; dbt 67/67 tests pass
- Scores unchanged: Naive Gold 7/11, Semantic Gold 11/11

### Files Updated

Generator (`generator_v2/models/sources.py`, `generator_v2/models/canonical.py`),
Bronze DDL (`pipeline_naive/ddl_bronze.sql`, `pipeline_naive/ddl_silver.sql`, `pipeline_naive/etl_silver.sql`),
dbt models (`dbt/models/gold_semantic/dw_position.sql`, `dbt/models/gold_semantic/dw_trade_lot.sql`,
`dbt/models/gold_naive/positions_naive.sql`, all schema.yml files),
semantic YAMLs (`semantic_model/positions_bronze.yaml`, `semantic_model/positions_gold_naive.yaml`),
docs (`docs/ambiguity_registry_v2.md`, `docs/architecture.md`),
variance (`variance/questions.py`, `variance/results/demo_results.json`),
load script (`pipeline_naive/load_bronze.py`).

### Branch

`feat/emerald-topaz-lot-swap` — merged to main 2026-04-22.

---

## Naive Gold Rebuild + Spotlight UI (2026-04-18)

### Why

Two pieces of demo feedback:
1. Naive Gold (0/11) looked indistinguishable from Silver — no contrast, no compelling story.
2. Eleven questions overwhelmed viewers; the 3–5 highest-impact questions needed to surface first.

### What changed

**Naive Gold is now Ruby-authoritative.** Ruby is GSF's back office GL — the system where accounts
originate before they can be traded in Emerald. Using Ruby as the Gold source is a reasonable architectural
decision. The result is a genuine 3-table Gold layer that resolves all structural ambiguities and scores 7/11.
The 4 it misses require governance decisions no dbt model makes: price authority (A2 — NAV vs custodian EOD),
G/L completeness (A11 — fund accounting has no mark-to-market), and cost basis method (A9).

**Spotlight mode** shows 5 high-impact questions by default; the rest are in a collapsed expander.

### Files Updated

| File | What changed |
|---|---|
| `dbt/models/gold_naive/accounts_naive.sql` | New — pass-through of account_master_full seed |
| `dbt/models/gold_naive/securities_naive.sql` | New — pass-through of security_master_full seed |
| `dbt/models/gold_naive/positions_naive.sql` | Rewritten — Ruby-only source, INNER JOIN to accounts_naive on fund_code, LEFT JOIN to securities_naive on ISIN; no aggregation needed (Ruby is position-level) |
| `dbt/models/gold_naive/schema.yml` | Updated — reflects Ruby back-office narrative; documents resolved (A3/A4/A7/A8/A10) and residual (A2/A9/A11) ambiguities |
| `semantic_model/positions_gold_naive.yaml` | Rewritten — expanded from 1-table to 3-table model with relationships; no verified_queries (intentional contrast with positions_gold.yaml's 7) |
| `app/streamlit_app.py` | Added FEATURED_QUESTIONS constant + spotlight sidebar toggle + split question loop |
| `variance/results/demo_results.json` | Regenerated from fresh variance run — Naive Gold 7/11 |

### Score change

| Tier | Before | After |
|------|--------|-------|
| Silver | 0/11 | 0/11 |
| Naive Gold | 0/11 | **7/11 (64%)** |
| Semantic Gold | 11/11 | 11/11 |

Naive Gold correct: Q02, Q03, Q05, Q06, Q07, Q09, Q11 (structure, allocation, securities)
Naive Gold fails: Q01 (price variance A2), Q04/Q08/Q10 (NULL G/L A11)

---

## Tier-Aware Failure Mode Narratives (2026-04-13)

### Why

With four tiers now toggleable in the Streamlit app, the single `failure_mode_silver`
string was inaccurate when Bronze or Naive Gold were visible. A Bronze failure for Q01
(account lookup) has nothing to do with Silver's POSITIONS_INTEGRATED — it fails because
Bronze has no canonical account dimension at all. Gold_Naive fails differently again
(account_ref is unresolved in the GROUP BY).

### Files Updated

| File | What changed |
|---|---|
| `variance/questions.py` | `failure_mode_silver: str` → `failure_modes: dict` with `bronze`, `silver`, `gold_naive` keys; all 11 questions updated |
| `variance/runner.py` | Serializes `failure_modes` dict instead of `failure_mode_silver` |
| `app/streamlit_app.py` | Builds `_q_failure_modes` lookup from live QUESTIONS list; per-tier `st.info` block shown only for visible failing tiers; heading reads "Why [Tier] fails here:" |

### Behavior

- Default view (Silver + Naive Gold + Gold): two failure blocks per question, one per
  failing tier, with tier-specific explanations
- Toggle Bronze on: third block appears with Bronze-specific reasoning
- Show only Gold: no failure block (Gold answers correctly)

---

## Pre-Deployment Hardening (2026-04-13)

### Why

Preparing the repo for public GitHub exposure and Streamlit Community Cloud deployment.
Goal: no personal identifiers, no live Snowflake dependency for the deployed app, minimal
build footprint on Streamlit Cloud.

### Files Updated

| File | What changed |
|---|---|
| `CLAUDE.md` | Removed Snowflake account identifier and username |
| `docs/runbook.md` | Replaced `DAVIDLOWE80NWL` with `<your_username>` (3 locations); replaced `WYXTVOC-AEB50319` with `<orgname>-<accountname>` (2 locations) |
| `.gitignore` | Added `!variance/results/demo_results.json` (un-gitignore canonical file); added `CLAUDE.md` |
| `variance/results/demo_results.json` | New committed file: canonical gold 11/11 showcase result |
| `app/streamlit_app.py` | `_load_latest_results()` now prefers `demo_results.json`; removed live "Re-run against Snowflake" panel; removed `import subprocess` |
| `requirements-app.txt` | New file: lightweight deps (streamlit, pandas, plotly, python-dotenv) for Streamlit Cloud — excludes dbt-snowflake and all pipeline deps |

### Static Mode

The deployed app requires no Snowflake credentials. It loads `demo_results.json`,
re-scores it locally against seed CSVs, and renders the full four-tier scorecard.
Fresh runner.py runs on local dev automatically pick up newer timestamped JSON files.

---

## Streamlit Tier Visibility Checkboxes (2026-04-13)

### Why

Bronze adds significant visual clutter — it fails all 11 questions for obvious reasons
(raw, fragmented, no joins). The useful narrative contrast is Silver vs Naive Gold vs
Semantic Gold. Bronze should be available but off by default.

### Files Updated

| File | What changed |
|---|---|
| `app/streamlit_app.py` | Sidebar "Tiers to display" section with four checkboxes; Bronze=False default, others=True; `VISIBLE_MODELS` list filters all scorecard, chart, and per-question rendering |

### Behavior

- Default: Silver (naive), Naive Gold, Semantic Gold visible
- Bronze toggled on: all four tiers shown
- Show only Gold: scorecard renders with one column, no failure blocks
- Empty selection guard prevents crash when all tiers are hidden

---

## Governance Language Cleanup (2026-04-13)

### Why

"Bias" is a loaded term in AI/ML contexts — it implies a specific class of ML fairness
problems rather than the data governance and correctness issues this demo illustrates.
Replaced throughout with "governance", "correctness", and "ungoverned layers" framing.

### Files Updated

All display strings, page titles, comments, and docstrings using "bias" were replaced.
Primary files: `app/streamlit_app.py`, semantic model YAML descriptions, `README.md`.

---

## Variance Runner Scoring Fixes (2026-04-11)

### Why

After the dw_position collapse (Gold 0/11 → 7/11), four questions remained wrong
despite Gold having the correct data.

| Question | Root Cause | Fix |
|----------|-----------|-----|
| Q06, Q10 | `result_type="row_count"` returns `len(rows)` but Cortex returns COUNT(*) as a 1-row aggregate — score was always 1 | Changed to `result_type="scalar"` |
| Q08 | "total unrealized gain" caused Cortex to add a gains-only filter, excluding losses | Rephrased to "total unrealized gain/loss" |
| Q09 | Cortex generated `SUM(SUM()) OVER ()` window function over filtered Fixed Income data only | Added `verified_query` with explicit CASE WHEN SQL to `positions_gold.yaml` |

Final Gold score: 11/11.

---

## dw_position: Collapse to 1 Canonical Row per Position (2026-04-11)

### Why

Running `variance/runner.py` after the dbt integration revealed that `dw_position`
produced 3 rows per account × security × position_date — one per source system
(TOPAZ, EMERALD, RUBY) via a UNION ALL of three CTEs. Cortex Analyst generates
`SUM(market_value)` without a source_system filter, returning 3× the correct value.
Gold score: 0/11. Q01 (ACC-0042 market value) returned $143.8M vs ground truth $47.9M.

Topaz is already the authoritative source for price, market_value, cost_basis, and
quantity (custodian EOD, already position-level from Topaz). Emitting only
the Topaz-resolved row produces one canonical row at the correct grain.

### Files Updated

| File | What changed |
|---|---|
| `dbt/models/gold_semantic/dw_position.sql` | Replace UNION ALL final SELECT with Topaz-only output; remove source_system from position_id hash; update header comment |
| `dbt/models/gold_semantic/schema.yml` | Update dw_position description, position_id, position_date, market_price, cost_basis; remove source_system column |
| `semantic_model/positions_gold.yaml` | Remove source_system dimension (column no longer in table) |
| `pipeline_semantic/validate_gold.py` | No change needed — Topaz-only still produces 4,886 rows (matches seed CSV) |
| `pipeline_semantic/setup_gold.sql` | Remove source_system VARCHAR(10) from DW_POSITION DDL |

### Actual Outcome

After `dbt run` (Topaz-only emit):
- DW_POSITION: 4,886 rows — Topaz alone produces 4,886 rows, matching the seed CSV.
  The original estimate (~1,629) assumed Topaz had 1,629 canonical positions × 3 lots,
  but Topaz is position-level; the canonical position count and the Topaz row count coincide.
- Q01 (ACC-0042 market value): $47,944,909.80 (corrected from $143.8M = 3×)
- Q03 (total market value): $6.1B (corrected from $18.3B = 3×)
- Gold final score: **11/11** (after additional Q06/Q08/Q09/Q10 fixes — see scoring fixes below)

---

## A7 Framing Correction (2026-04-10)

### Why

During a code walkthrough, the A7 (Mixed-Grain Record IDs) narrative was identified as
technically incorrect. The prior framing — "summing quantity across all sources inflates
Fixed Income totals by ~2-3x" — implies SUM() is broken. It is not: Emerald lots roll up
correctly to the position total within Emerald. What A7 actually breaks is:

- **COUNT queries**: `COUNT(*)` counts lots as positions for Emerald accounts
- **Cardinality mismatches**: a list query ("which clients hold AAPL?") and a count
  query ("how many clients hold AAPL?") return different row counts against Silver with
  no error — the numbers simply don't reconcile
- **Row-level assumptions**: any consumer treating "one row = one position" gets a lot
  fragment (partial market value, not the full position) for Topaz holdings

### Files Updated

| File | What changed |
|---|---|
| `dbt/models/silver/schema.yml` | `quantity` column description |
| `dbt/models/gold_naive/schema.yml` | Model description + `total_quantity` column description |
| `dbt/models/gold_naive/positions_naive.sql` | Two inline comments |
| `semantic_model/positions_gold_naive.yaml` | Header comment + `total_quantity` description |
| `semantic_model/positions_bronze.yaml` | `units` fact description |
| `docs/ambiguity_registry_v2.md` | Summary table row + full A7 body section |
| `variance/questions.py` | `failure_mode_silver` for Q05 + `_q05_gt` docstring |

### Canonical A7 narrative going forward

> *"Grain mismatch is invisible in the schema. COUNT queries overcount Emerald lot rows
> as positions. The observable symptom: ask the same question as a list and as a count
> — you get different numbers, with no error."*

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
| `dbt/models/gold_semantic/dw_trade_lot.sql` | Lot-level detail (Emerald only) |
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
