# Epic History (PoC Phase, Epics 1-5)

Historical record of Epic completion during the proof-of-concept phase (pre-refactoring).
This document preserves the detailed task breakdowns and validation results.

---

## Epic 1: Multi-Source Position Data Design and Generator

**Status:** Complete
**Generator:** `generator_v2/` (deterministic, seed=42)
**Output:** 9 CSVs in `data/seed_v2/`

### 1a. Normalized DW Schema (canonical target)
- DW_POSITION: position_id, account_id, security_id, lot_id, position_date, quantity, market_price, market_value, cost_basis, unrealized_gain_loss, currency, source_system
- DW_ACCOUNT: account_id, account_name, account_type, base_currency, custodian_account_num (Topaz), portfolio_code (Emerald), fund_code (Ruby), is_active
- DW_SECURITY: security_id, security_name, cusip, isin, ticker, asset_class, security_type, currency
- DW_TRADE_LOT: lot_id, account_id, security_id, acquisition_date, acquisition_price, original_quantity, remaining_quantity, cost_basis

### 1b. Source System Schemas
- **Topaz** (custodian): CUSIP, ACCT_NUM, lot-level grain, custodian EOD price
- **Emerald** (portfolio mgmt): ticker, portfolioId, position-level grain, PM evaluated price
- **Ruby** (fund accounting): ISIN, fund_code, position-level grain, NAV price

### 1c. Cross-Source Ambiguity Registry
A1-A11 documented in `docs/ambiguity_registry_v2.md`

### 1d. Generator
All 21 integrity checks pass (V1-V13 canonical + VI1-VI5 integrated + VS1-VS3 stub).
Key design: every source file is derivable from canonical DW with deterministic transformations.

---

## Epic 2: Naive Pipeline -- Bronze + Silver (No Semantic Layer)

**Status:** Complete (2026-03-27)
**Validation:** `validate_silver.py` passed SC1-SC9

**Row counts:** TOPAZ=12,388 / EMERALD=4,886 / RUBY=4,886 / STUB=170 / INTEGRATED=22,160

**Silver NULL fractions (ambiguities encoded):**
- SECURITY_MASTER_ID IS NULL (A8): ~15%
- UNREALIZED_GL IS NULL (A11): ~22% (all Ruby rows)
- ASSET_CLASS IS NULL (A10): ~15% (matches A8)

**Scripts (now in `pipeline_naive/`):**
- `ddl_bronze.sql`, `ddl_silver.sql`, `etl_silver.sql`, `load_bronze.py`, `validate_silver.py`

---

## Epic 3: Semantic Enriched Pipeline -- Gold + Semantic Model

**Status:** Complete (2026-03-27)
**Validation:** GC1-GC12 all pass

**Row counts:** ACCOUNT=100 / SECURITY=200 / POSITION=4,886 / TRADE_LOT=12,388

**Semantic model:** `semantic_model/positions_gold.yaml`
- Generated via Snowflake Semantic Model Generator, enriched with A1-A11 descriptions
- 4 tables, 4 relationships, dimensions with synonyms, facts, filters, metrics, 6 verified queries
- Staged to `@GSF_GOLD_STAGE/semantic/positions_gold.yaml`

**Scripts (now in `pipeline_semantic/`):**
- `setup_gold.sql`, `load_gold.py`, `validate_gold.py`

---

## Epic 4: Cortex Analyst Configuration

**Status:** Complete (2026-03-30)

**Key insight:** Cortex Analyst requires a YAML for every query. Both environments need one:
- Gold (governed): `semantic_model/positions_gold.yaml`
- Silver (naive): `semantic_model/positions_silver.yaml`

**Auth:** Session token from `conn._rest._token` (key-pair JWT removed -- caused stage 404s)

**Gate question:** "What is the total market value of account ACC-0042?"
- Gold: George Group Trust / $47,944,909.80 (correct)
- Silver: no data (A4 -- account_ref = 'ACC-0042' but Silver stores source keys)

---

## Epic 5: Variance Capture and Visualization

**Status:** Complete (2026-04-04)

**Components:**
- 11-question bank (`variance/questions.py`), one per ambiguity A1-A11
- Ground truth module (`variance/ground_truth.py`), from seed CSVs (independent of Snowflake)
- Variance runner (`variance/runner.py`), scores CORRECT/WRONG/NO_DATA/ERROR
- Comparator (`variance/comparator.py`), tolerances: scalar +/-0.01%, percentage +/-0.1pp, row_count exact
- Streamlit app (`app/streamlit_app.py`), scorecard + chart + detail tables

**Ground truth values:**
| Q# | Ambiguity | Expected (Gold) |
|---|---|---|
| Q01 | A4 | $47,944,909.80 (ACC-0042 market value) |
| Q02 | A1, A8 | 156,813 shares (Rasmussen LLC plc / RLP / SEC-0018) |
| Q03 | A2 | $6,107,574,036.42 (total AUM) |
| Q04 | A3, A11 | -$1,713,597,904.29 (total loss > $10k) |
| Q05 | A5, A7 | 3,557,137 units (Fixed Income quantity) |
| Q06 | A6 | 4,886 rows (positions as of 2024-12-31) |
| Q07 | A8 | $1,006,108,902.53 (Fixed Income market value) |
| Q08 | A9 | $36,547,881.58 (Equity unrealized gain) |
| Q09 | A10 | 16.4731% (Fixed Income allocation) |
| Q10 | A11 | 2,365 positions (unrealized loss > $10k) |
| Q11 | A1, A4, A9 | $932,065,151.31 (Fixed Income cost basis) |

---

## Epic 6: AI Bias Analysis Tool

**Status:** Deferred as a separate project. This project is designed with plug-in points
(variance JSON output format) so Epic 6 can consume the results independently.
