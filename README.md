# Semantic Layer Bias Study

A portfolio demonstration proving that the Snowflake semantic layer is not an optional
convenience — it is a **bias control mechanism** for AI-generated results.

Three synthetic legacy source systems (Topaz, Emerald, Ruby) each produce position data
in their own physical schema with different identifiers, column names, price sources, and
grains. Without a semantic layer, Cortex Analyst queries produce confident but wrong
answers. With one, queries against a unified governed view produce correct answers.

**Portfolio angle:** Governed data is the prerequisite for trustworthy AI.
**Snowflake features:** Cortex Analyst, semantic model (YAML), Horizon governance.

---

## Architecture

```
LEGACY SOURCE SYSTEMS
  Topaz (Custodian)     — CUSIP, lot-level, custodian EOD price
  Emerald (Portfolio)   — ticker, position-level, PM evaluated price
  Ruby (Fund Acctg)     — ISIN, position-level, NAV price
        │
        ├──► Pipeline A ──► BRONZE (3 raw tables, obviously broken)
        │                ──► SILVER (POSITIONS_INTEGRATED — looks normalized, semantically broken)
        │                ──► Cortex Analyst Silver (confident but wrong — A7–A11)
        │
        └──► Pipeline B ──► GOLD (4 DW tables — DW_ACCOUNT, DW_SECURITY, DW_POSITION, DW_TRADE_LOT)
                         ──► Semantic Model YAML (resolves A1–A11)
                         ──► Cortex Analyst Gold (correct)
                                      │
                                      ▼
                              Variance Report  ← Epic 5 (in progress)
```

---

## Status

| Epic | Description | Status |
|---|---|---|
| 1 | Multi-source data generator (V2) | ✅ Complete |
| 2 | Pipeline A — Bronze + Silver | ✅ Complete |
| 3 | Pipeline B — Gold + Semantic Model | ✅ Complete |
| 4 | Cortex Analyst (both models, Python + Snowsight) | ✅ Complete |
| 5 | Variance capture and Streamlit visualization | 🔧 In progress |
| 6 | AI Bias Analysis Tool | 📋 R&D / future |

---

## Quick Start

### 1. Prerequisites

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your Snowflake credentials.
Key-pair authentication is required (Duo MFA blocks password auth for scripted runs).
See CLAUDE.md for key-pair setup instructions.

### 2. Generate seed data

```bash
python -m generator_v2.generator --validate
```

Produces 9 deterministic CSVs in `data/seed_v2/` — 4 DW tables + 3 source feeds +
1 security master stub + 1 integrated reference.

### 3. Run a question against both models

```bash
# Governed Gold model — correct answer
python cortex/query_cortex.py --model gold

# Naive Silver model — demonstrates A4 (account ID fragmentation)
python cortex/query_cortex.py --model silver

# Custom question
python cortex/query_cortex.py --model gold --question "What is the total AUM?"

# Print SQL only, don't execute
python cortex/query_cortex.py --model gold --no-execute
```

Gate question: *"What is the total market value of account ACC-0042?"*
- Gold → George Group Trust / **$47,944,909.80** ✓
- Silver → no data (queries `account_ref = 'ACC-0042'`; Silver stores source keys)

---

## Project Structure

```
├── generator_v2/           # Epic 1: Deterministic seed data generator (V2, seed=42)
├── pipeline_a/             # Epic 2: Bronze + Silver load scripts
│   ├── setup.sql           # Database/schema/warehouse/role creation
│   ├── ddl_bronze.sql      # Bronze table DDL
│   ├── ddl_silver.sql      # Silver table DDL
│   ├── load_bronze.py      # PUT + COPY INTO Bronze tables
│   ├── etl_silver.sql      # Naive ETL → POSITIONS_INTEGRATED
│   └── validate_silver.py  # SC1–SC9 validation checks
├── pipeline_b/             # Epic 3: Gold DW load + semantic model staging
│   ├── setup_gold.sql      # Gold table DDL + stage creation
│   ├── load_gold.py        # PUT + COPY INTO Gold tables; stages both YAMLs
│   └── validate_gold.py    # GC1–GC12 validation checks
├── cortex/                 # Epic 4: Cortex Analyst query runner
│   └── query_cortex.py     # REST API caller; --model gold|silver
├── semantic_model/         # Cortex Analyst semantic model YAMLs
│   ├── positions.yaml      # Gold — governed, resolves A1–A11
│   └── positions_silver.yaml  # Silver — naive, no disambiguation
├── variance/               # Epic 5: Comparison script (not yet built)
├── app/                    # Epic 5: Streamlit visualization (not yet built)
├── data/
│   ├── seed_v2/            # Generated CSVs (gitignored — run generator_v2)
│   └── schema/
│       └── schema_definition.md
└── docs/
    └── ambiguity_registry_v2.md   # All 11 intentional ambiguities (A1–A11)
```

---

## The 11 Ambiguities

| # | Name | Layer | Demo Question |
|---|---|---|---|
| A1 | Security ID fragmentation | Raw sources | Total quantity in Apple across all accounts? |
| A2 | Price source divergence | Raw sources | Total market value of ACC-0042 at month-end? |
| A3 | Column name heterogeneity | Raw sources | Positions with unrealized losses > $10K? |
| A4 | Account ID fragmentation | Raw sources | Total market value of account ACC-0042? |
| A5 | Position grain mismatch | Raw sources | Total quantity in Apple across all accounts? |
| A6 | Date field semantics | Raw sources | All positions as of December 31, 2024? |
| A7 | Mixed-grain record IDs | Integrated | Total quantity of fixed income across all portfolios? |
| A8 | Unmastered security IDs | Integrated | Total market value of all fixed income positions? |
| A9 | Cost basis fragmentation | Integrated | Total unrealized gain for the equity sleeve? |
| A10 | Asset class classification gap | Integrated | What % of AUM is allocated to fixed income? |
| A11 | NULL unrealized G/L from Ruby | Integrated | Positions with unrealized losses > $10K? |

See [docs/ambiguity_registry_v2.md](docs/ambiguity_registry_v2.md) for full detail on each.

---

## Snowflake Objects

| Object | Name |
|---|---|
| Database | `GSF_DEMO` |
| Warehouse | `GSF_WH` |
| Role | `GSF_ROLE` |
| Bronze schema | `GSF_DEMO.BRONZE` |
| Silver schema | `GSF_DEMO.SILVER` |
| Gold schema | `GSF_DEMO.GOLD` |
| Semantic model stage | `@GSF_DEMO.GOLD.GSF_GOLD_STAGE/semantic/` |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Seed data | Python (Faker, deterministic seed=42) |
| Data warehouse | Snowflake (Bronze / Silver / Gold schemas) |
| Semantic layer | Snowflake Cortex Analyst YAML + Horizon governance |
| AI querying | Cortex Analyst (natural language → SQL) |
| Visualization | Streamlit (Epic 5 — not yet built) |
| Hosting | AWS (planned) |
