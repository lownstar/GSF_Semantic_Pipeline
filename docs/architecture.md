# Architecture

## Seven Lifecycle Phases

```
Phase 1          Phase 2          Phase 3          Phase 4          Phase 5           Phase 6          Phase 7
GENERATION  -->  DELIVERY    -->  BRONZE      -->  SILVER      -->  GOLD         -->  AI QUERY    -->  ANALYSIS
generator_v2     boto3 -> S3      COPY INTO        dbt run          dbt run           Cortex           Streamlit
9 CSVs           landing zone     raw tables       naive union      DW tables         Analyst          variance
                                                   (dbt Silver)     + semantic YAML   (4 tiers)        scorecard
```

Phases 1-4 are shared by both pipelines. Phase 5 is where they diverge:

- **Naive Pipeline** builds `GOLD_NAIVE` using assumption-based dbt transforms — looks
  like a star schema but carries Silver-layer integrity problems forward (A1-A11 unresolved)
- **Semantic Enriched Pipeline** builds `GOLD` guided by the semantic model — structurally
  and semantically correct, resolves all 11 ambiguities

The core argument: without semantic governance, you cannot trust *any* layer, not even Gold.

**Four-tier comparison:** Bronze → Silver → Naive Gold → Semantic Gold. Cortex Analyst
queries all four. The Naive Gold tier isolates the semantic model as the key variable —
it shows that a well-structured dbt Gold layer is still not enough without governance.

---

## Data Flow

```
LEGACY SOURCE SYSTEMS (3 synthetic feeds, different schemas)
  Topaz (Custodian)     — CUSIP, lot-level, custodian EOD price
  Emerald (Portfolio)   — ticker, position-level, PM evaluated price
  Ruby (Fund Acctg)     — ISIN, position-level, NAV price
        |
        v [Phase 1: Generation]
  generator_v2 -> 9 CSVs in data/seed_v2/
        |
        v [Phase 2: Delivery]
  delivery/deliver.py -> S3 landing zone
    s3://gsf-demo-landing/topaz/
    s3://gsf-demo-landing/emerald/
    s3://gsf-demo-landing/ruby/
    s3://gsf-demo-landing/reference/
        |
        v [Phase 3: Bronze Ingest]
  BRONZE.TOPAZ_POSITIONS    (12,388 rows — CUSIP, lot-level)
  BRONZE.EMERALD_POSITIONS  (4,886 rows  — ticker, position-level)
  BRONZE.RUBY_POSITIONS     (4,886 rows  — ISIN, position-level)
  BRONZE.SECURITY_MASTER_STUB (170 rows — 30 securities absent, produces A8/A10 NULLs)
        |
        v [Phase 4: Silver Transform]
  SILVER.POSITIONS_INTEGRATED (22,160 rows)
    Naive ETL union of all three sources.
    Looks normalized. Semantically broken (A7-A11 embedded).
    Unmastered NULLs emerge from failed LEFT JOINs on the stub.
        |
        +--- Naive Pipeline ----------------> [Phase 5: Gold — assumption-based]
        |    GOLD_NAIVE.DW_POSITION           Picks first available security ID
        |    GOLD_NAIVE.DW_ACCOUNT            Keeps raw source account refs
        |    GOLD_NAIVE.DW_SECURITY           ~15% NULL security_master_id
        |    GOLD_NAIVE.DW_TRADE_LOT          Mixed grain, blended cost basis
        |                                      [Phase 6: Cortex Analyst]
        |                                      queries GOLD_NAIVE via positions_silver.yaml
        |                                      -> confident but wrong answers
        |
        +--- Semantic Enriched Pipeline ----> [Phase 5: Gold — governed]
             GOLD.DW_POSITION                 Complete security master (200 rows)
             GOLD.DW_ACCOUNT                  All 3 source account keys mapped
             GOLD.DW_SECURITY                 Zero NULL unrealized_gain_loss
             GOLD.DW_TRADE_LOT                Explicit lot grain
             semantic_model/positions_gold.yaml    Resolves A1-A11 explicitly
                                              [Phase 6: Cortex Analyst]
                                              queries GOLD via positions_gold.yaml
                                              -> correct answers
                                                      |
                                                      v [Phase 7: Analysis]
                                              variance/runner.py (11 questions, scored)
                                              app/streamlit_app.py (scorecard + detail)
```

---

## Snowflake Objects

| Object | Name | Status | Purpose |
|---|---|---|---|
| Database | `GSF_DEMO` | Active | Single database for entire demo |
| Warehouse | `GSF_WH` | Active | Single compute resource |
| Role | `GSF_ROLE` | Active | Demo role with object privileges |
| Schema | `GSF_DEMO.BRONZE` | Active | Raw source tables (3 feeds + security stub) |
| Schema | `GSF_DEMO.SILVER` | Active | Naive ETL output (POSITIONS_INTEGRATED) |
| Schema | `GSF_DEMO.GOLD` | Active | Governed DW tables + semantic model |
| Schema | `GSF_DEMO.GOLD_NAIVE` | Active | Assumption-based DW tables (dbt gold_naive models) |
| Stage | `@BRONZE.GSF_BRONZE_STAGE` | Active | Internal stage for local Bronze CSV loads |
| Stage | `@BRONZE.GSF_S3_LANDING` | Planned (Step 3 SQL) | External stage for S3 loads |
| Stage | `@GOLD.GSF_GOLD_STAGE` | Active | Internal stage for Gold loads + semantic YAMLs |

---

## Row Counts

| Table | Rows | Notes |
|---|---|---|
| BRONZE.TOPAZ_POSITIONS | 12,388 | Lot-level (CUSIP, ACCT_NUM) |
| BRONZE.EMERALD_POSITIONS | 4,886 | Position-level (ticker, portfolioId) |
| BRONZE.RUBY_POSITIONS | 4,886 | Position-level (ISIN, fund_code) |
| BRONZE.SECURITY_MASTER_STUB | 170 | 30 of 200 securities absent — produces A8/A10 NULLs |
| SILVER.POSITIONS_INTEGRATED | 22,160 | Union of all 3 sources — A7-A11 embedded |
| GOLD.DW_ACCOUNT | 100 | Canonical accounts (all 3 source keys mapped) |
| GOLD.DW_SECURITY | 200 | Complete master — zero gaps, zero NULL asset_class |
| GOLD.DW_POSITION | 4,886 | Position-level grain — zero NULL unrealized_gain_loss |
| GOLD.DW_TRADE_LOT | 12,388 | Lot-level detail |

---

## S3 Landing Zone Layout

```
s3://gsf-demo-landing/
├── topaz/
│   └── positions_topaz.csv        (12,388 rows — Topaz custodian feed)
├── emerald/
│   └── positions_emerald.csv      (4,886 rows  — Emerald PM feed)
├── ruby/
│   └── positions_ruby.csv         (4,886 rows  — Ruby fund accounting feed)
└── reference/
    └── security_master_stub.csv   (170 rows    — partial security master)
```

Populated by `delivery/deliver.py`. Snowflake external stage `@BRONZE.GSF_S3_LANDING`
reads from this bucket via storage integration (see `infrastructure/s3_external_stage.sql`).

---

## The 11 Ambiguities

| # | Name | Where | What breaks without governance |
|---|---|---|---|
| A1 | Security ID fragmentation | Raw | CUSIP/ticker/ISIN — no shared key |
| A2 | Price source divergence | Raw | Custodian/PM/NAV prices blended |
| A3 | Column name heterogeneity | Raw | Different names for same concept |
| A4 | Account ID fragmentation | Raw | No link between source account keys |
| A5 | Position grain mismatch | Raw | Lot-level vs position-level mixed |
| A6 | Date field semantics | Raw | Settlement/trade/NAV dates conflated |
| A7 | Mixed-grain record IDs | Silver | LOT-* and POS-* coexist in one table |
| A8 | Unmastered security IDs | Silver | ~15% NULL security_master_id |
| A9 | Cost basis fragmentation | Silver | Lot/avg/book cost blended |
| A10 | Asset class gap | Silver | ~15% NULL asset_class |
| A11 | NULL unrealized G/L | Silver | Ruby rows NULL (22% of table silently excluded) |

Full detail: `docs/ambiguity_registry_v2.md`
