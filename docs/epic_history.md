# Epic History

Historical record of completed epics (PoC phase and beyond) plus planned future work.
Completed epics preserve task breakdowns and validation results; planned epics document design decisions for when implementation begins.

---

## Epic 1: Multi-Source Position Data Design and Generator

**Status:** Complete
**Generator:** `generator_v2/` (deterministic, seed=42)
**Output:** 11 CSVs in `data/seed_v2/`

### 1a. Normalized DW Schema (canonical target)
- DW_CLIENT: client_id, client_name, client_type
- DW_ACCOUNT: account_id, account_name, account_type, base_currency, custodian_account_num (Topaz), portfolio_code (Emerald), fund_code (Ruby), is_active, client_id, strategy_type
- DW_ACCOUNT_LINKS: account_id, linked_account_id, link_type
- DW_POSITION: position_id, account_id, security_id, lot_id, position_date, quantity, market_price, market_value, cost_basis, unrealized_gain_loss, currency, source_system
- DW_SECURITY: security_id, security_name, cusip, isin, ticker, asset_class, security_type, currency
- DW_TRADE_LOT: lot_id, account_id, security_id, acquisition_date, acquisition_price, original_quantity, remaining_quantity, cost_basis

### 1b. Source System Schemas
- **Topaz** (custodian): CUSIP, ACCT_NUM, position-level grain, custodian EOD price
- **Emerald** (portfolio mgmt): ticker, portfolioId, lot-level grain, PM evaluated price
- **Ruby** (fund accounting): ISIN, fund_code, position-level grain, NAV price

### 1c. Cross-Source Ambiguity Registry
A1-A11 documented in `docs/ambiguity_registry_v2.md`

### 1d. Generator
All 26 integrity checks pass (V1-V13 canonical + VI1-VI5 integrated + VS1-VS3 stub + VC1-VC5 client/household tier).
Key design: every source file is derivable from canonical DW with deterministic transformations.

---

## Epic 2: Naive Pipeline -- Bronze + Silver (No Semantic Layer)

**Status:** Complete (2026-03-27)
**Validation:** `validate_silver.py` passed SC1-SC9

**Row counts:** TOPAZ=4,886 / EMERALD=12,388 / RUBY=4,886 / STUB=170 / INTEGRATED=22,160

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

## Epic 6: AI Results Analysis Tool

**Status:** Deferred as a separate project. This project is designed with plug-in points
(variance JSON output format) so Epic 6 can consume the results independently.

---

## Cross-Project: GSF Identity Network Integration (2026-04-22)

**Status:** Phase 2 in progress (`feat/gsf-identity-network-phase2`)

The GSF_Account_Network project visualizes the multi-source identity ambiguity problem as the
"setup" half of a two-demo portfolio narrative. This project is the resolution half.

**Phase 1 (Account Network — unblocked):** Uses existing position CSVs to render 100 canonical
accounts as hub nodes with 3 source spokes each (Topaz/Emerald/Ruby), MV discrepancy tooltips.

**Phase 2 (this project's contribution):** Generator extended to produce client/household tier
data and OTC collateral account relationships. See `docs/refactoring_changelog.md` for full detail.

**Ambiguities surfaced in the network graph:** A4 (account ID fragmentation, the core spoke
pattern), A2 (MV delta per account), A5/A7 (Emerald lot count vs Topaz position count),
A8 (unmastered security orphans), A11 (Ruby NULL unrealized G/L). New derivative: collateral
account relationship gap (Derivatives accounts → Ruby GL incomplete without collateral movements).

---

## Epic 7 (Planned): Interactive "Try It Yourself" Chat Tab

**Status:** Planned
**Motivation:** Portfolio visitors currently take the variance results on faith. This epic
adds a multi-turn chat interface so they can ask their own natural language questions directly
to Cortex Analyst across all four semantic tiers, see the generated SQL, and explore with
follow-up questions. The "agentic" feel comes from Cortex maintaining conversation context
across turns.

### Goal

Replace the one-way "watch the pre-run results" experience with a live chat where users type
questions, watch the SQL generate, and ask follow-ups — proving the demo is real, not staged.
Must work on Streamlit Community Cloud (via Secrets) and locally (via `.env`).

### Key Deliverables

| File | Change |
|---|---|
| `app/cortex_chat.py` | New module — credential loading, Snowflake connection, multi-turn Cortex API call, SQL execution |
| `app/streamlit_app.py` | Add second tab "Try It Yourself" with tier selector, suggested questions, `st.chat_message` history, `st.chat_input` |
| `requirements-app.txt` | Add `snowflake-connector-python>=3.0`, `cryptography>=41.0`, `python-dotenv>=1.0` |
| `docs/runbook.md` | New "Enabling the Live Chat Tab" section with Streamlit Secrets `.toml` template |

### Design Decisions

**Multi-turn:** Full `messages` history is sent on every Cortex Analyst API call. The assistant's
prior text + SQL blocks must be echoed back so Cortex can resolve follow-up pronouns ("now break
*that* down..."). Session state holds a richer dict `{role, content, sql, rows, error}`.

**Credential loading (two paths):**
- *Deployed (Streamlit Secrets):* `st.secrets["SNOWFLAKE_PRIVATE_KEY_PEM"]` (PEM string) →
  `cryptography` → DER bytes → `snowflake.connector.connect(private_key=...)`
- *Local (.env):* Existing `SNOWFLAKE_PRIVATE_KEY_FILE` (.p8 DER file) path →
  read bytes directly → same `private_key=` param

**Graceful degradation:** When neither `st.secrets` nor `.env` provides Snowflake config,
the tab renders an informational callout (local setup instructions + Community Cloud Secrets
instructions) instead of an error.

**Tier selector:** All four tiers available (Bronze / Silver / Naive Gold / Semantic Gold), each
with a one-line description. Switching tier requires starting a new conversation (session state
reset) to avoid context pollution across semantic models.

**Reuse from existing code:**
- `cortex/query_cortex.py`: `execute_sql()`, `_ensure_staged()`, `MODELS` dict, REST endpoint pattern
- `variance/questions.py`: `QUESTIONS` list powers the suggested-question prompts

### Cortex Analyst Wire Format (multi-turn)

```json
{
  "messages": [
    {"role": "user",     "content": [{"type": "text", "text": "What is the total MV of ACC-0042?"}]},
    {"role": "analyst",  "content": [{"type": "text", "text": "The total is $47.9M."}, {"type": "sql", "statement": "SELECT ..."}]},
    {"role": "user",     "content": [{"type": "text", "text": "Now break that down by asset class"}]}
  ],
  "semantic_model_file": "@GSF_DEMO.GOLD.GSF_GOLD_STAGE/semantic/positions_gold.yaml"
}
```

### Verification Checklist

1. Local with `.env` → chat tab live, multi-turn works, SQL + results render
2. Local without `.env` → tab shows no-credentials callout, no errors
3. Deployed → after adding Streamlit Secrets, chat tab goes live with no code changes
4. Multi-turn → ask Q01, then ask a follow-up scoped to the same account → Cortex uses context
5. Tier comparison → same question on `gold_naive` vs `gold` shows wrong answer vs correct answer in real-time
6. Suggested questions → clicking pre-fills and submits in one action
7. Tab 1 (Variance Analysis) → unaffected by the tab restructure
