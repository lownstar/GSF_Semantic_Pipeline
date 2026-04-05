# Semantic Layer Bias Study — Claude Code Plan

## Project Overview

A portfolio demonstration project that proves the semantic layer is not an optional
convenience but a **bias control mechanism** for AI-generated results. Three synthetic
legacy source systems — **Topaz** (custodian), **Emerald** (portfolio management), and
**Ruby** (fund accounting) — each produce their own version of position data with
different identifiers, column names, price sources, and grains. Without a semantic layer,
AI queries against these raw sources produce inconsistent, unreliable results. With one,
Cortex Analyst queries a unified, governed view and produces correct answers.

The semantic layer's job is to surface what data exists across all three sources and
translate each source's physical schema into a single normalized position model that
AI — and humans — can trust.

A second phase (R&D) builds an AI Bias Analysis Tool that dissects *why* the results
diverged.

**Audience:** Hiring teams, portfolio site visitors, financial services architects
**Portfolio angle:** Governed data is the prerequisite for trustworthy AI
**Snowflake features:** Horizon, Cortex Analyst, semantic model (YAML), data classification

---

## Snowflake Naming Conventions

All Snowflake objects use the `GSF` (Gemstone Financial) prefix. Medallion layers are
expressed as schemas within a single database — one `USE DATABASE` statement, clean IaC.

| Object | Name | Notes |
|---|---|---|
| Database | `GSF_DEMO` | Single database for the entire demo |
| Warehouse | `GSF_WH` | Single compute warehouse |
| Role | `GSF_ROLE` | Demo role with object privileges |
| Schema — Bronze | `GSF_DEMO.BRONZE` | Raw source tables, loaded as-is from seed CSVs |
| Schema — Silver | `GSF_DEMO.SILVER` | `POSITIONS_INTEGRATED` — naive ETL output |
| Schema — Gold | `GSF_DEMO.GOLD` | Governed DW tables + semantic model target |

Bronze/Silver/Gold map directly to the three-act demo narrative (Act 1 / Act 2 / Act 3).

---

## Architecture

```
LEGACY SOURCE SYSTEMS (three synthetic feeds, each with their own schema)
  ┌─────────────────────────────────────────────────────────────────┐
  │  Topaz (Custodian)        positions_topaz      CUSIP, ACCT_NUM  │
  │  Emerald (Portfolio Mgmt) positions_emerald    ticker, port_id  │
  │  Ruby (Fund Accounting)   positions_ruby       ISIN, fund_code  │
  └─────────────────────────────────────────────────────────────────┘
        │                                         │
        ▼                                         ▼
  Pipeline A (GSF_DEMO)                     Pipeline B (GSF_DEMO)
  ─────────────────────────────────         ──────────────────────────────────
  BRONZE: 3 raw source tables               GOLD: Normalized DW tables
    TOPAZ_POSITIONS                           DW_POSITION
    EMERALD_POSITIONS                         DW_ACCOUNT  (all 3 source keys)
    RUBY_POSITIONS                            DW_SECURITY (CUSIP+ISIN+ticker)
        │                                     DW_TRADE_LOT
        ▼                                         │
  SILVER: Naive ETL integration             Snowflake Semantic Model (YAML)
    POSITIONS_INTEGRATED                    maps physical → logical fields,
    (one table, looks normalized,           resolves IDs, enforces grain,
     semantically broken — A7–A11)         governs price sources (A1–A11)
        │                                         │
        ▼                                         ▼
  Cortex Analyst A                          Cortex Analyst B
  (confident but wrong —                    (single governed vocabulary)
   grain traps, unmastered NULLs,               │
   mixed price sources)                    AI Results B ──► Variance Report
        │                                                    (demo punchline)
        ▼
  AI Results A ────────────────────────────────────────────────────►
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Seed data generation | Python (Faker + custom financial logic) |
| Pipelines | Python scripts (or dbt if warranted) |
| Data warehouse | Snowflake (`GSF_DEMO` — single database, Bronze/Silver/Gold schemas) |
| Semantic layer | Snowflake semantic model (YAML) + Horizon governance |
| AI querying | Cortex Analyst (natural language → SQL) |
| Variance capture | Python comparison logic |
| Visualization | Streamlit or simple web UI (TBD) |
| IaC / repeatability | Snowflake SQL scripts + Python setup scripts |
| Portfolio hosting | AWS (existing portfolio site strategy) |

---

## Epics & Tasks

### Epic 1: Multi-Source Position Data Design and Generator
**Goal:** Three synthetic legacy source systems — Topaz, Emerald, Ruby — each producing
position data in their own physical schema. A normalized DW position schema serves as
the canonical target. The semantic layer maps source → DW and resolves inter-source
ambiguities. This is the foundation that makes the Env A vs Env B comparison meaningful.

> **Status:** Complete — all sub-tasks done. V2 is the only active demo track.
> V1 generator (`generator/`) and seed data (`data/seed/`) are **deprecated**:
> code preserved as reference, not loaded into any pipeline. All work in
> `generator_v2/` and `data/seed_v2/`. V1 ambiguity registry deprecated;
> see `docs/ambiguity_registry_v2.md` (now covers A1–A11).

#### 1a. Normalized DW Schema (canonical target)
- [x] Design `DW_POSITION` — canonical position table
  - Columns: `position_id`, `account_id`, `security_id`, `lot_id`, `position_date`,
    `quantity`, `market_price`, `market_value`, `cost_basis`, `unrealized_gain_loss`,
    `currency`, `source_system`
- [x] Design `DW_ACCOUNT` — canonical account/portfolio master
  - Columns: `account_id`, `account_name`, `account_type`, `base_currency`,
    `custodian_account_num` (Topaz key), `portfolio_code` (Emerald key),
    `fund_code` (Ruby key), `is_active`
- [x] Design `DW_SECURITY` — canonical security master with all identifier types
  - Columns: `security_id`, `security_name`, `cusip`, `isin`, `ticker`,
    `asset_class`, `security_type`, `currency`
- [x] Design `DW_TRADE_LOT` — lot-level detail supporting position grain
  - Columns: `lot_id`, `account_id`, `security_id`, `acquisition_date`,
    `acquisition_price`, `original_quantity`, `remaining_quantity`, `cost_basis`

#### 1b. Source System Schemas (three legacy feeds)
- [x] Define **Topaz** schema (custodian feed — abbreviated uppercase naming)
  - Position file columns: `ACCT_NUM`, `SEC_CUSIP`, `AS_OF_DT`, `UNITS`,
    `MKT_PRC`, `MKT_VAL`, `COST_BASIS`, `UNRLZD_GL`, `LOT_ID`, `CCY`
  - Security identifier: CUSIP primary
  - Account identifier: custodian account number (`ACCT_NUM`)
  - Price source: custodian end-of-day closing price
  - Grain: lot-level (one row per account × security × lot)
- [x] Define **Emerald** schema (portfolio management system — camelCase naming)
  - Position file columns: `portfolioId`, `securityTicker`, `positionDate`,
    `quantity`, `unitPrice`, `marketValue`, `avgCostBasis`, `unrealizedPnL`, `ccy`
  - Security identifier: proprietary internal ticker (`securityTicker`)
  - Account identifier: portfolio code (`portfolioId`)
  - Price source: PM system evaluated price (may differ from custodian)
  - Grain: position-level aggregate (one row per account × security, no lot detail)
- [x] Define **Ruby** schema (fund accounting system — verbose snake_case naming)
  - Position file columns: `fund_code`, `isin_identifier`, `nav_date`,
    `shares_held`, `nav_per_share`, `total_nav_value`, `book_cost`, `currency_code`
  - Security identifier: ISIN
  - Account identifier: fund code (`fund_code`)
  - Price source: NAV-based price (may differ from both Topaz and Emerald)
  - Grain: position-level (one row per fund × security, no lot detail)

#### 1c. Cross-Source Ambiguity Registry
- [x] Document and register all inter-source ambiguities — **complete**, see `docs/ambiguity_registry_v2.md`
  - **A1** — Security ID fragmentation (CUSIP / ticker / ISIN)
  - **A2** — Price source divergence (custodian EOD / PM evaluated / NAV)
  - **A3** — Column name heterogeneity (ACCT_NUM / portfolioId / fund_code; MKT_VAL / marketValue / total_nav_value)
  - **A4** — Account identifier fragmentation (no FK linking the three source keys)
  - **A5** — Position grain mismatch (Topaz lot-level vs Emerald/Ruby position-level)
  - **A6** — Date field semantics (AS_OF_DT settlement / positionDate trade / nav_date NAV strike)
  - **A7** — Mixed-grain record IDs in naive integration table (LOT-* and POS-* coexist)
  - **A8** — Unmastered security IDs (~15% NULL security_master_id in integrated table)
  - **A9** — Cost basis semantic fragmentation (lot cost / avg cost / book cost blended)
  - **A10** — Asset class classification gap (NULL for unmastered rows; sleeve % wrong)
  - **A11** — NULL unrealized G/L from Ruby (22% of integrated rows silently excluded)

#### 1d. Generator Build
- [x] Build `generator_v2/` Python package — deterministic (seed=42), same Faker patterns
  - `generate_dw_account()` — canonical account master (100 accounts)
  - `generate_dw_security()` — canonical security master (200 securities) with all IDs
  - `generate_dw_trade_lot()` — lot-level records linking to DW accounts/securities
  - `generate_dw_position()` — derived from lots, canonical grain
  - `generate_topaz_positions()` — derives from DW data, applies Topaz schema + noise
  - `generate_emerald_positions()` — derives from DW data, applies Emerald schema + noise
    (collapses lots to position-level, uses ticker not CUSIP, applies price variance)
  - `generate_ruby_positions()` — derives from DW data, applies Ruby schema + noise
    (fund-level, ISIN only, NAV pricing)
  - `generate_integrated_positions()` — naive ETL union of all three sources with
    semantic gaps: ~15% unmastered securities (NULL security_master_id), NULL unrealized
    G/L for all Ruby rows, mixed-grain record IDs, blended cost basis methods (A7–A11)
  - `generate_security_master_stub()` — 170-row subset of DW_SECURITY (30 unmastered
    absent) used by `etl_silver.sql` LEFT JOIN to produce A8/A10 NULLs naturally
- [x] Output CSVs to `data/seed_v2/` — 9 files total (4 DW + 3 source + 1 stub + 1 integrated)
- [x] Validate: all 21 integrity checks pass (V1–V13 canonical + VI1–VI5 integrated + VS1–VS3 stub)

**Key design principle:** Every source file is derivable from the canonical DW schema
with deterministic transformations applied — renamed columns, substituted identifiers,
price variance noise. This means ground truth is always knowable, making variance
measurement in Epic 5 objective.

---

### Epic 2: Pipeline A — Naive Integration (No Semantic Layer)
**Goal:** Demonstrate the two-stage failure: first the raw heterogeneous sources (obviously
broken), then a naive ETL integration that looks normalized but hides all the semantic
problems inside a single table. This is the "we already integrated it" state that most
organizations actually reach before governance.

> **Status:** Complete (2026-03-27) — Bronze loaded and verified; `etl_silver.sql` run
> via Snowflake worksheet; `validate_silver.py` passed SC1–SC9 (22,160 rows, 15.5% NULL
> security_master_id, 22.0% NULL unrealized_gl, all record_id prefix checks OK).

**Snowflake objects (`GSF_DEMO`):**
- **`BRONZE` schema** — three raw source tables loaded as-is from `data/seed_v2/` CSVs,
  plus `BRONZE.SECURITY_MASTER_STUB` (170 of 200 securities — 30 absent to produce
  A8/A10 NULLs via LEFT JOIN)
- **`SILVER` schema** — `POSITIONS_INTEGRATED` created by a Snowflake SQL script that
  unions the Bronze tables. Real pipeline artifact, not a CSV load. Unmastered NULLs
  emerge naturally from failed LEFT JOINs in the SQL.
  `data/seed_v2/positions_integrated.csv` serves as the validation reference only.

**Three-act demo structure:**
- **Act 1 (Bronze)**: Three raw source tables — Cortex fails obviously (no shared keys,
  three schemas, no canonical field names)
- **Act 2 (Silver)**: `POSITIONS_INTEGRATED` — Cortex generates confident SQL that returns
  wrong answers. The insidious failure: one table, looks done, semantically broken (A7–A11)
- **Act 3 (Gold)**: `GSF_DEMO.GOLD` semantic model — Cortex answers correctly

**Pipeline A scripts (`pipeline_a/`):**
- [x] `setup.sql` — CREATE DATABASE `GSF_DEMO`, SCHEMAS (`BRONZE`, `SILVER`, `GOLD`), WAREHOUSE `GSF_WH`, ROLE `GSF_ROLE`
- [x] `ddl_bronze.sql` — CREATE TABLE for `BRONZE.TOPAZ_POSITIONS`, `BRONZE.EMERALD_POSITIONS`, `BRONZE.RUBY_POSITIONS`, `BRONZE.SECURITY_MASTER_STUB`
- [x] `ddl_silver.sql` — CREATE TABLE for `SILVER.POSITIONS_INTEGRATED` (columns annotated with ambiguity codes)
- [x] `load_bronze.py` — Python: stage + COPY INTO all four Bronze tables from seed CSVs; verifies row counts
- [x] `etl_silver.sql` — Naive ETL: three normalizing CTEs + UNION ALL + LEFT JOIN stub → `SILVER.POSITIONS_INTEGRATED`; posts summary of A7–A11 NULL counts
- [x] `validate_silver.py` — SC1–SC9 checks: row counts, NULL fractions, source values, record_id prefix conventions

**Row count targets:** Bronze TOPAZ=12,388 / EMERALD=4,886 / RUBY=4,886 / STUB=170 · Silver INTEGRATED=22,160
**Constraints:** No semantic model, no governed views, no column-level policies in Bronze or Silver

---

### Epic 3: Pipeline B — Semantic Model (Cortex Analyst YAML)
**Goal:** Load the normalized DW schema into Snowflake Gold and deploy a semantic model
that explicitly resolves all eleven cross-source ambiguities (A1–A11). This is the
governed layer that makes Cortex Analyst queries reliable.

**Portfolio pitch:** The semantic model must exist — how it is created can vary (hand-authored
by an architect, generated from a data catalog, produced by a governance tool). What matters
is that it exists, it is governed, and AI cannot produce trustworthy results without it.

**Semantic model approach: Cortex Analyst YAML (stage-based)**
`semantic_model/positions.yaml` is the primary artifact — a structured YAML file in
Snowflake's Cortex Analyst semantic model format. The pipeline stages it to an internal
Snowflake stage (`@GSF_STAGE/semantic/`). Cortex Analyst reads it directly at query time
by stage path reference. This makes the semantic model a visible, reviewable, portable
artifact — the thing you show a hiring manager and say "this is what governance looks like."

> **Future enhancement:** The same YAML could drive a `CREATE SEMANTIC VIEW` DDL
> generator (Native Semantic Views, GA 2025) for deeper Horizon lineage integration.
> For this demo, the stage-based approach keeps the pipeline transparent and the
> semantic artifact front-and-center.

**Pipeline B scripts (`pipeline_b/`):**
- [x] `setup_gold.sql` — CREATE TABLE for `GOLD.DW_POSITION`, `GOLD.DW_ACCOUNT`, `GOLD.DW_SECURITY`, `GOLD.DW_TRADE_LOT`; CREATE STAGE `@GSF_GOLD_STAGE`
- [x] `load_gold.py` — Python: stage + COPY INTO GOLD DW tables from seed CSVs; PUT `positions.yaml` to `@GSF_GOLD_STAGE/semantic/`
- [x] `validate_gold.py` — GC1–GC12 checks: row counts, FK integrity, NULL checks, semantic model staged

**Semantic model artifact** (`semantic_model/positions.yaml`) — **COMPLETE**:
Generated via Snowflake Semantic Model Generator (Snowsight → AI & ML → Semantic Model
→ Create), enriched with A1–A11 descriptions, synonyms, and verified queries. Staged to
`@GSF_GOLD_STAGE/semantic/positions.yaml`. Validated in Snowsight Cortex Analyst UI.

- **Tables**: `account`, `security`, `position`, `trade_lot` — logical names over
  `DW_ACCOUNT`, `DW_SECURITY`, `DW_POSITION`, `DW_TRADE_LOT`
- **Relationships**: 4 explicit join paths (position↔account, position↔security,
  trade_lot↔account, trade_lot↔security)
- **Dimensions** (resolves A1/A3/A4/A6): all three source security identifiers
  named with synonyms; all three source account keys named; `position_date`
  as `time_dimension` with trade-date basis documented
- **Facts** (resolves A2/A5/A7/A9/A10/A11): `market_value`, `market_price`,
  `quantity`, `cost_basis`, `unrealized_gain_loss` — NOTE: Snowflake uses `facts:`
  not `measures:` for numeric columns in the Cortex Analyst YAML protobuf format
- **Filters**: pre-built filter expressions for common query patterns
- **Metrics**: pre-computed aggregates (total AUM, allocation %, etc.)
- **Verified queries**: 6 pre-validated SQL examples using fully qualified table names
  (`GSF_DEMO.GOLD.*`) — required because Cortex Analyst has no default database context

> **Validated (2026-03-27):** GC1–GC12 all pass — 100 accounts, 200 securities
> (+30 vs Bronze stub), 4,886 positions, 12,388 lots; zero NULL unrealized_gain_loss
> or asset_class; all FKs resolve; YAML staged.

> **Validated (2026-03-30):** Cortex Analyst gate question passed in Snowsight UI.
> Gold model: "What is the total market value of account ACC-0042?" →
> George Group Trust / $47,944,909.80 (correct). See Epic 4 for Silver comparison.

> **Note:** Re-run `python pipeline_b/load_gold.py` after any change to either
> YAML file. The script truncates tables before loading (idempotent) and
> stages both `positions.yaml` and `positions_silver.yaml` automatically.

- [x] Validate: Cortex Analyst (stage-based) resolves the Epic 4 gate question correctly

---

### Epic 4: Cortex Analyst Configuration
**Goal:** Enable natural language querying against both environments — Silver (naive,
wrong answers) and Gold (governed, correct answers) — using the same Cortex Analyst
engine pointed at different semantic models.

> **Status: COMPLETE (2026-03-30)**

**Key architectural insight (discovered 2026-03-28):**
Cortex Analyst requires a semantic model file for every query — there is no "no model"
mode. Both environments need a YAML:
- **Gold (governed):** `semantic_model/positions.yaml` — resolves all A1–A11 ambiguities
- **Silver (naive):** `semantic_model/positions_silver.yaml` — exposes POSITIONS_INTEGRATED
  as-is, no disambiguation; Cortex generates confident but wrong SQL

**What exists (`cortex/`):**
- [x] `cortex/query_cortex.py` — Cortex Analyst REST API caller; `--model gold|silver`
      Auth uses session token from `conn._rest._token` (key-pair JWT approach was removed —
      it caused stage 404 errors due to role context issues)

**YAML format lessons learned (2026-03-28 through 2026-03-30):**
- `primary_entity` is NOT valid → must be `primary_key`
- `primary_key` requires `columns:` nested field, not a bare list
- `measures:` is NOT valid → must be `facts:` for numeric columns
- `verified_queries` SQL must use fully qualified table names (`GSF_DEMO.GOLD.*`) —
  Cortex Analyst session has no default database context
- Hand-authoring against Snowflake's undocumented protobuf spec is fragile
- **Correct approach:** Snowsight → AI & ML → Semantic Model → Create (legacy) →
  select GOLD tables → "Start learning" → "Generate" → accept suggestions →
  download YAML → enrich with A1–A11 content → save → `load_gold.py`

**Stage access for Snowsight Cortex Analyst UI:**
- Stage needs directory table enabled: `ALTER STAGE GOLD.GSF_GOLD_STAGE SET DIRECTORY=(ENABLE=TRUE)` ← already done
- Internal stages support READ and WRITE only (not USAGE)
- Run before each Snowsight session: `USE ROLE GSF_ROLE; ALTER STAGE GOLD.GSF_GOLD_STAGE REFRESH;`
- Grant SYSADMIN browsing access (run once as ACCOUNTADMIN):
  ```sql
  GRANT READ ON STAGE GSF_DEMO.GOLD.GSF_GOLD_STAGE TO ROLE SYSADMIN;
  GRANT WRITE ON STAGE GSF_DEMO.GOLD.GSF_GOLD_STAGE TO ROLE SYSADMIN;
  ```

**Gate question results (2026-03-30):**
- **Gold model:** "What is the total market value of account ACC-0042?"
  → George Group Trust / $47,944,909.80 ✓ (correct SQL, correct JOIN via semantic layer)
- **Silver model:** same question → no data returned
  → queried `account_ref = 'ACC-0042'` but Silver stores source keys (C-XXXXXX / PORT-XXXX / FND-XXXX)
  → demonstrates A4 (account identifier fragmentation) — the semantic layer IS the fix

**Gate question confirmed passing via Python (2026-03-30):**
```bash
python cortex/query_cortex.py --model gold
# → George Group Trust / $47,944,909.80 ✓

python cortex/query_cortex.py --model silver
# → no data returned (A4 demonstrated — account_ref mismatch) ✓
```

**Next: Epic 5**
- Design full 10–15 question set, at least one per ambiguity A1–A11
- Run both models, capture responses, pre-compute ground truth from `data/seed_v2/`
- Build Python variance comparison script
- Build Streamlit visualization (side-by-side, variance scores, summary)

---

### Epic 5: Variance Capture and Visualization
**Goal:** The demo punchline — make the difference visible and measurable.

> **Status: COMPLETE (2026-04-04)**

**What was built:**

- **11-question bank** (`variance/questions.py`) — one question per ambiguity A1–A11, each with
  an explicit `failure_mode_silver` description and a `ground_truth_fn` (pandas lambda over seed CSVs).
  Questions cover: account ID lookup (A4), unmastered security (A1+A8), total AUM (A2),
  unrealized loss (A3+A11), grain mismatch (A5+A7), date semantics (A6), Fixed Income MV (A8),
  cost basis methods (A9), allocation % (A10), Ruby NULL exclusion (A11), compound failure (A1+A4+A9).

- **Ground truth module** (`variance/ground_truth.py`) — loads `data/seed_v2/` CSVs and computes
  all 11 expected answers via pandas. Independent of Snowflake — ground truth never changes when
  Cortex changes.

- **Variance runner** (`variance/runner.py`) — asks all 11 questions against both Gold and Silver
  models, captures generated SQL + result rows, scores each answer (CORRECT / WRONG / NO_DATA / ERROR),
  and saves a timestamped JSON to `variance/results/`. CLI: `python variance/runner.py`.

- **Comparator** (`variance/comparator.py`) — scoring logic with tolerances:
  scalar ±0.01% relative, percentage ±0.1 pp absolute, row_count exact.
  Handles Snowflake `decimal.Decimal` return type (connector returns NUMBER as Decimal, not float).

- **Streamlit app** (`app/streamlit_app.py`) — full visualization:
  header scorecard, stacked status bar chart, expandable question-by-question comparison
  (generated SQL, result rows, ground truth, failure narrative), sidebar re-run button.
  App re-scores from raw rows on every load so comparator fixes take effect without re-running Cortex.

**Key implementation notes:**
- `result_type` field per question: `"scalar"` (extract first numeric from first row),
  `"row_count"` (count returned rows), `"percentage"` (scalar with pp tolerance + fraction normalization)
- Snowflake `NUMBER` columns return as `decimal.Decimal` — comparator handles this explicitly
- All print statements in runner.py are ASCII-only (Windows `cp1252` stdout compatibility)
- Runner imports from `cortex/query_cortex.py` via `sys.path` insert (no cortex package install needed)

**Ground truth values (from data/seed_v2/ CSVs):**
| Q# | Ambiguity | Expected (Gold) |
|---|---|---|
| Q01 | A4 | $47,944,909.80 (ACC-0042 market value) |
| Q02 | A1, A8 | 156,813 shares (Rasmussen LLC plc / RLP / SEC-0018) |
| Q03 | A2 | $6,107,574,036.42 (total AUM) |
| Q04 | A3, A11 | -$1,713,597,904.29 (total loss on positions with loss > $10k) |
| Q05 | A5, A7 | 3,557,137 units (Fixed Income quantity at position grain) |
| Q06 | A6 | 4,886 rows (positions as of 2024-12-31) |
| Q07 | A8 | $1,006,108,902.53 (Fixed Income market value) |
| Q08 | A9 | $36,547,881.58 (Equity unrealized gain) |
| Q09 | A10 | 16.4731% (Fixed Income allocation) |
| Q10 | A11 | 2,365 positions (count with unrealized loss > $10k) |
| Q11 | A1, A4, A9 | $932,065,151.31 (Fixed Income cost basis) |

- [x] Define variance metrics (result accuracy vs ground truth from seed CSVs)
- [x] Build comparison script (`variance/runner.py` + `variance/comparator.py`)
- [x] Build Streamlit visualization (`app/streamlit_app.py`)
- [ ] Record Loom walkthrough over live demo
- [ ] Write portfolio narrative (problem statement → architecture → results)

---

### Epic 6: AI Bias Analysis Tool (R&D Phase)
**Goal:** A standalone tool that dissects *why* AI results diverge — generalizable
beyond this demo.

> **Status:** R&D — scope to be defined after Epic 5 is complete and variance
> patterns are observed empirically.

**Hypotheses to investigate:**
- Schema ambiguity → SQL hallucination patterns
- Missing metric definitions → incorrect aggregation choices
- Lack of lineage → AI cannot resolve grain mismatches
- Column naming conventions → tokenization bias in NL→SQL

**Potential tool capabilities:**
- Accepts: two result sets + the queries that generated them
- Analyzes: structural differences in generated SQL
- Classifies: bias type (aggregation error, grain mismatch, column confusion, etc.)
- Outputs: bias report with root cause hypothesis

**Format TBD:** Python library, web UI, or both

---

## Project Principles

1. **Same underlying data, three different schemas, one governed answer** — the contrast is everything
2. **Ambiguity is designed in** — the demo only works if the raw environment
   fails in predictable, explainable ways
3. **Architect's perspective** — always explain the *why* behind design decisions,
   not just the *what*
4. **Portfolio-ready** — every component should be explainable to a non-technical
   hiring manager in 90 seconds
5. **Clean-room** — no Parametric data, no proprietary schemas, fully synthetic

---

## Running the Snowflake Pipeline

All scripts run from the project root. Prerequisites: Python env with `requirements.txt`
installed, Snowflake account credentials in `.env` (copy from `.env.example`).

**Find your account identifier** (Business Critical — run in a Snowflake worksheet):
```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();
-- Result format: myorg-myaccount  (use this as SNOWFLAKE_ACCOUNT in .env)
```

### Prerequisites — Key-Pair Authentication (required — Duo MFA blocks password auth)

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
-- Open snowflake_rsa_key.pub, copy the contents BETWEEN (not including)
-- the -----BEGIN PUBLIC KEY----- and -----END PUBLIC KEY----- lines.
ALTER USER DAVIDLOWE80NWL SET RSA_PUBLIC_KEY='<paste key body here>';

-- Verify: look for RSA_PUBLIC_KEY_FP row (non-null fingerprint confirms success)
DESC USER DAVIDLOWE80NWL;
```

3. Add to `.env` (remove or comment out `SNOWFLAKE_PASSWORD`):
```
SNOWFLAKE_PRIVATE_KEY_FILE=snowflake_rsa_key.p8
```

**Notes:**
- `snowflake_rsa_key.p8` is gitignored (`*.p8` in `.gitignore`) — never commit it
- `SNOWFLAKE_USER` must be `DAVIDLOWE80NWL` exactly — JWT auth is case-sensitive
- `SNOWFLAKE_ACCOUNT` must be `WYXTVOC-AEB50319` (regionless org-based format)
- `load_bronze.py` and `validate_silver.py` use key-pair auth when `SNOWFLAKE_PRIVATE_KEY_FILE`
  is set, fall back to `SNOWFLAKE_PASSWORD` otherwise

**Windows note:** The `PUT` command in `load_bronze.py` converts backslashes to forward
slashes automatically (`abs_path.replace("\\", "/")`) — required for Snowflake file URIs
on Windows. No manual adjustment needed.

### Step 0 — One-time Cortex + Horizon setup (ACCOUNTADMIN required)

Run once per account. Enables Cortex Analyst, grants `SNOWFLAKE.CORTEX_USER` to
`GSF_ROLE`, creates Horizon governance tags, and grants `CREATE SEMANTIC VIEW`.

```sql
-- Paste pipeline_a/setup_cortex.sql into a Snowflake worksheet
-- OR:
-- snowsql -f pipeline_a/setup_cortex.sql
```

After running, execute the verification queries in Section 4 of that script to confirm:
- `ENABLE_CORTEX_ANALYST = true`
- `SNOWFLAKE.CORTEX_USER` and `SNOWFLAKE.CORTEX_ANALYST_USER` granted to `GSF_ROLE`
- `FINANCIAL_SENSITIVITY` and `PII_CLASSIFICATION` tags exist in `GOLD`
- Cortex smoke test returns a response

### Step 1 — Infrastructure setup (ACCOUNTADMIN required)

```sql
-- Creates GSF_DEMO database, BRONZE/SILVER/GOLD schemas, GSF_WH, GSF_ROLE
-- snowsql -f pipeline_a/setup.sql
```

### Step 2 — Pipeline A: Bronze + Silver

```bash
# Generate all 9 seed CSVs (or re-run to reset ground truth)
python -m generator_v2.generator --validate

# Create Bronze and Silver tables (worksheet or SnowSQL)
# snowsql -f pipeline_a/ddl_bronze.sql
# snowsql -f pipeline_a/ddl_silver.sql

# Load Bronze tables from seed CSVs
python pipeline_a/load_bronze.py

# Run naive ETL to populate SILVER.POSITIONS_INTEGRATED
# snowsql -f pipeline_a/etl_silver.sql

# Validate Silver output against reference CSV
python pipeline_a/validate_silver.py
```

**Expected row counts:**

| Table | Rows |
|---|---|
| `BRONZE.TOPAZ_POSITIONS` | 12,388 |
| `BRONZE.EMERALD_POSITIONS` | 4,886 |
| `BRONZE.RUBY_POSITIONS` | 4,886 |
| `BRONZE.SECURITY_MASTER_STUB` | 170 |
| `SILVER.POSITIONS_INTEGRATED` | 22,160 |

**Expected Silver NULL fractions (confirm ambiguities are encoded):**

| Check | Expected |
|---|---|
| `SECURITY_MASTER_ID IS NULL` (A8) | ~15% |
| `UNREALIZED_GL IS NULL` (A11) | ~22% (all Ruby rows) |
| `ASSET_CLASS IS NULL` (A10) | ~15% (matches A8) |

### Step 3 — Pipeline B: Gold + Semantic Model (Epic 3 — not yet built)

```bash
# Create GOLD tables and internal stage (worksheet or SnowSQL)
# snowsql -f pipeline_b/setup_gold.sql

# Load GOLD DW tables from seed CSVs; PUT positions.yaml to @GSF_STAGE/semantic/
python pipeline_b/load_gold.py

# Validate GOLD table row counts and FK integrity
python pipeline_b/validate_gold.py
```

The semantic model lives in `semantic_model/positions.yaml`.
`load_gold.py` stages it automatically — no separate apply step needed.
Cortex Analyst references it as `@GSF_STAGE/semantic/positions.yaml`.

### Step 4 — Epic 5: Variance Capture and Visualization

```bash
# Verify ground truth values (no Snowflake connection needed)
python variance/runner.py --dry-run

# Run all 11 questions against both Gold and Silver models (~2–4 min)
# Saves timestamped JSON to variance/results/
python variance/runner.py

# Run one model only
python variance/runner.py --model gold
python variance/runner.py --model silver

# Launch the Streamlit visualization
streamlit run app/streamlit_app.py
```

The Streamlit app loads from `variance/results/` and re-scores on every page load —
comparator fixes apply immediately without re-running Cortex.

### SnowSQL connection (if using CLI)

```bash
snowsql -a <orgname>-<accountname> -u <username> -f pipeline_a/setup_cortex.sql
```

Or configure `~/.snowsql/config` with your account credentials and run scripts by file.

---

## Portfolio Integration

- **Demo format:** Live Streamlit app hosted on AWS (AMI snapshot strategy)
- **Video:** Loom walkthrough (live demo) + Canva/Synthesia 60–90s business opener
- **GitHub:** Public repo with IaC scripts, seed generator, semantic model YAML
- **Portfolio page structure:**
  - Problem statement: "Why AI results on raw data cannot be trusted"
  - Architecture diagram (from this plan)
  - Live demo button → Streamlit app
  - Video walkthrough
  - GitHub link

---

## Open Questions

- [x] Single Snowflake account with two schemas, or two separate trial accounts?
      **Resolved: single account (`GSF_DEMO`), three medallion schemas (BRONZE / SILVER / GOLD).**
- [ ] **Backlog: `load_gold.py` stages both Gold and Silver YAMLs** — pragmatic shortcut
      (both live in the same stage), but architecturally confusing. A hiring manager or
      student following the pipeline will reasonably ask "why does the Gold loader touch
      the Silver model?" Consider splitting into a dedicated `stage_semantic_models.py`
      or adding a `--yaml-only` flag. Low priority until Epic 5 is complete, but worth
      addressing before a live demo or public repo release.
- [ ] Should Topaz, Emerald, and Ruby source files also be loaded into `GOVERNED_ENV`
      alongside DW tables, so the semantic model can demonstrate field-level lineage
      from source column all the way to semantic metric? Or keep Env B DW-only?
- [ ] Price variance between sources: how large should the noise be to make the
      discrepancy visible in demo results without being implausible? (~0.1–0.5% variance?)
- [ ] Does the demo need a fourth "blended" source (e.g., a Bloomberg pricing override
      file) to demonstrate a price hierarchy resolution use case?
- [ ] Streamlit Cloud vs AWS for demo hosting?
- [ ] Bias tool: Python package (pip installable) or web UI?
- [ ] Should the semantic model YAML be hand-authored or generated from the DW schema
      to demonstrate a real-world onboarding workflow?

---

## Success Criteria

| Milestone | Done When |
|---|---|
| Seed data | `generator_v2/` produces 9 CSVs (4 DW + 3 source + 1 stub + 1 integrated) in `data/seed_v2/`, deterministic (seed=42), all 21 checks pass (V1–V13, VI1–VI5, VS1–VS3) — **DONE** |
| Pipeline A scripts | `pipeline_a/` contains `setup.sql`, `ddl_bronze.sql`, `ddl_silver.sql`, `load_bronze.py`, `etl_silver.sql`, `validate_silver.py` — **DONE** |
| Pipeline A — Bronze | All 4 Bronze tables loaded, row counts verified (2026-03-27) — **DONE** |
| Pipeline A — Silver | `etl_silver.sql` run, `validate_silver.py` passes SC1–SC9 — **DONE** |
| Pipeline B — Gold tables | `setup_gold.sql` run, `load_gold.py` passes row count checks, `validate_gold.py` passes GC1–GC12 — **DONE** |
| Pipeline B — Semantic model | `semantic_model/positions.yaml` generated, enriched, staged, validated in Snowsight — **DONE** |
| Epic 4 — Gate question (Snowsight) | Gold model: $47,944,909.80 ✓; Silver model: no data (A4 demonstrated) — **DONE** (2026-03-30) |
| Epic 4 — Gate question (Python) | `query_cortex.py` session token auth; Gold $47,944,909.80 ✓; Silver no data (A4) ✓ — **DONE** (2026-03-30) |
| Epic 5 — Variance runner | `python variance/runner.py` completes; `variance/results/*.json` produced with Gold/Silver scores — **DONE** (2026-04-04) |
| Epic 5 — Streamlit app | `streamlit run app/streamlit_app.py` renders scorecard, chart, and 11-question comparison — **DONE** (2026-04-04) |
| Variance | Side-by-side comparison is visually clear and measurable — **DONE** (2026-04-04) |
| Portfolio | Loom video recorded, page live on portfolio site |
| Bias tool | At least one bias classification type implemented (R&D) |