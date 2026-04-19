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

- **Naive Pipeline** builds `GOLD_NAIVE` from Ruby (back office GL) — canonical accounts,
  all 200 securities, correct position grain. Fails only where fund accounting diverges from
  custodian governance: price authority (A2), G/L completeness (A11), cost basis method (A9).
  Scores 7/11 — the well-built Gold layer that still needs a semantic model.
- **Semantic Enriched Pipeline** builds `GOLD` guided by the semantic model — structurally
  and semantically correct, resolves all 11 ambiguities. Scores 11/11.

The core argument: without semantic governance, you cannot trust *any* layer, not even Gold.

**Four-tier comparison:** Bronze → Silver → Naive Gold → Semantic Gold. Cortex Analyst
queries all four. The Naive Gold tier isolates the semantic model as the key variable —
it shows that a well-structured dbt Gold layer is still not enough without governance.

---

## Data Flow

```
LEGACY SOURCE SYSTEMS (3 synthetic feeds, different schemas)
  Topaz (Custodian)     — CUSIP, position-level, custodian EOD price
  Emerald (Front Office/OMS) — ticker, lot-level, PM evaluated price
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
  BRONZE.TOPAZ_POSITIONS    (4,886 rows  — CUSIP, position-level)
  BRONZE.EMERALD_POSITIONS  (12,388 rows — ticker, lot-level)
  BRONZE.RUBY_POSITIONS     (4,886 rows  — ISIN, position-level)
  BRONZE.SECURITY_MASTER_STUB (170 rows — 30 securities absent, produces A8/A10 NULLs)
        |
        v [Phase 4: Silver Transform]
  SILVER.POSITIONS_INTEGRATED (22,160 rows)
    Naive ETL union of all three sources.
    Looks normalized. Semantically broken (A7-A11 embedded).
    Unmastered NULLs emerge from failed LEFT JOINs on the stub.
        |
        +--- Naive Pipeline ----------------> [Phase 5: Gold — Ruby-authoritative]
        |    GOLD_NAIVE.POSITIONS_NAIVE        Ruby back-office GL; NAV price, NULL G/L, book cost
        |    GOLD_NAIVE.ACCOUNTS_NAIVE         100 canonical accounts (Ruby fund_code mapping)
        |    GOLD_NAIVE.SECURITIES_NAIVE       200 securities (full security master)
        |    semantic_model/positions_gold_naive.yaml  3-table model; no verified_queries
        |                                      [Phase 6: Cortex Analyst]
        |                                      queries GOLD_NAIVE via positions_gold_naive.yaml
        |                                      -> correct on structure, fails valuation/G/L (7/11)
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
| Schema | `GSF_DEMO.GOLD_NAIVE` | Active | Ruby-authoritative Gold tables (POSITIONS_NAIVE, ACCOUNTS_NAIVE, SECURITIES_NAIVE) |
| Stage | `@BRONZE.GSF_BRONZE_STAGE` | Active | Internal stage for local Bronze CSV loads |
| Stage | `@BRONZE.GSF_S3_LANDING` | Active | External stage for S3 loads |
| Stage | `@GOLD.GSF_GOLD_STAGE` | Active | Internal stage for Gold loads + semantic YAMLs |

---

## Row Counts

| Table | Rows | Notes |
|---|---|---|
| BRONZE.TOPAZ_POSITIONS | 4,886 | Position-level (CUSIP, ACCT_NUM) |
| BRONZE.EMERALD_POSITIONS | 12,388 | Lot-level (ticker, portfolioId, LOT_ID) |
| BRONZE.RUBY_POSITIONS | 4,886 | Position-level (ISIN, fund_code) |
| BRONZE.SECURITY_MASTER_STUB | 170 | 30 of 200 securities absent — produces A8/A10 NULLs |
| SILVER.POSITIONS_INTEGRATED | 22,160 | Union of all 3 sources — A7-A11 embedded |
| GOLD_NAIVE.ACCOUNTS_NAIVE | 100 | Canonical accounts (Ruby fund_code mapping, all source keys) |
| GOLD_NAIVE.SECURITIES_NAIVE | 200 | Full security master (CUSIP/ISIN/ticker/asset_class) |
| GOLD_NAIVE.POSITIONS_NAIVE | 4,886 | Ruby-only positions — correct grain, NAV price, NULL G/L |
| GOLD.DW_ACCOUNT | 100 | Canonical accounts (all 3 source keys mapped) |
| GOLD.DW_SECURITY | 200 | Complete master — zero gaps, zero NULL asset_class |
| GOLD.DW_POSITION | 4,886 | Position-level grain — zero NULL unrealized_gain_loss |
| GOLD.DW_TRADE_LOT | 12,388 | Lot-level detail |

---

## S3 Landing Zone Layout

```
s3://gsf-demo-landing/
├── topaz/
│   └── positions_topaz.csv         (4,886 rows — Topaz custodian feed)
├── emerald/
│   └── positions_emerald.csv      (12,388 rows — Emerald PM feed)
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
| A7 | Mixed-grain record IDs | Silver | LOT-* (Emerald) and POS-*/NAV-* coexist in one table |
| A8 | Unmastered security IDs | Silver | ~15% NULL security_master_id |
| A9 | Cost basis fragmentation | Silver | Lot/avg/book cost blended |
| A10 | Asset class gap | Silver | ~15% NULL asset_class |
| A11 | NULL unrealized G/L | Silver | Ruby rows NULL (22% of table silently excluded) |

Full detail: `docs/ambiguity_registry_v2.md`
