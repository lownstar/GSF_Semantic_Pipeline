# Architecture

## Seven Lifecycle Phases

```
Phase 1          Phase 2          Phase 3          Phase 4          Phase 5           Phase 6          Phase 7
GENERATION  -->  DELIVERY    -->  BRONZE      -->  SILVER      -->  GOLD         -->  AI QUERY    -->  ANALYSIS
generator_v2     boto3 -> S3      COPY INTO        dbt models       dbt models        Cortex           Streamlit
9 CSVs           landing zone     raw tables       naive ETL        DW tables         Analyst          variance
                                                                                                       scorecard
```

Both pipelines share Phases 1-4. They diverge at Phase 5:

- **Naive Pipeline:** Gold tables built on assumptions (looks like star schema, semantically broken)
- **Semantic Enriched Pipeline:** Gold tables built with semantic layer guidance (correct)

---

## Data Flow

```
LEGACY SOURCE SYSTEMS (3 synthetic feeds)
  Topaz (Custodian)     -- CUSIP, lot-level, custodian EOD price
  Emerald (Portfolio)   -- ticker, position-level, PM evaluated price
  Ruby (Fund Acctg)     -- ISIN, position-level, NAV price
        |
        v
  [Phase 1: Generation] generator_v2 -> 9 CSVs in data/seed_v2/
        |
        v
  [Phase 2: Delivery] boto3 -> S3 landing zone
        |
        v
  [Phase 3: Bronze Ingest] COPY INTO -> BRONZE.TOPAZ/EMERALD/RUBY_POSITIONS
        |
        v
  [Phase 4: Silver Transform] dbt -> SILVER.POSITIONS_INTEGRATED (22,160 rows, A7-A11)
        |
        +--- Naive Pipeline ---------> [Phase 5: Gold] GOLD_NAIVE.DW_* (assumption-based, broken)
        |                               [Phase 6: Query] Cortex Analyst -> wrong answers
        |
        +--- Semantic Enriched ------> [Phase 5: Gold] GOLD.DW_* (governed, correct)
                                        Semantic model YAML staged
                                        [Phase 6: Query] Cortex Analyst -> correct answers
                                                |
                                                v
                                        [Phase 7: Analysis] Variance scoring + Streamlit app
```

---

## Snowflake Objects

| Object | Name | Purpose |
|---|---|---|
| Database | `GSF_DEMO` | Single database for entire demo |
| Warehouse | `GSF_WH` | Single compute resource |
| Role | `GSF_ROLE` | Demo role with object privileges |
| Schema | `GSF_DEMO.BRONZE` | Raw source tables (3 feeds + security stub) |
| Schema | `GSF_DEMO.SILVER` | Naive ETL output (POSITIONS_INTEGRATED) |
| Schema | `GSF_DEMO.GOLD` | Governed DW tables + semantic model |
| Schema | `GSF_DEMO.GOLD_NAIVE` | Assumption-based DW tables (planned) |
| Stage | `@GSF_DEMO.BRONZE.GSF_BRONZE_STAGE` | Internal stage for Bronze CSV loads |
| Stage | `@GSF_DEMO.GOLD.GSF_GOLD_STAGE` | Internal stage for Gold loads + semantic YAMLs |

All objects use the `GSF` (Gemstone Financial) prefix. Medallion layers are schemas
within a single database.

---

## Row Counts

| Table | Rows | Notes |
|---|---|---|
| BRONZE.TOPAZ_POSITIONS | 12,388 | Lot-level (CUSIP, ACCT_NUM) |
| BRONZE.EMERALD_POSITIONS | 4,886 | Position-level (ticker, portfolioId) |
| BRONZE.RUBY_POSITIONS | 4,886 | Position-level (ISIN, fund_code) |
| BRONZE.SECURITY_MASTER_STUB | 170 | 30 of 200 securities absent (A8/A10) |
| SILVER.POSITIONS_INTEGRATED | 22,160 | Union of all 3 sources (A7-A11 embedded) |
| GOLD.DW_ACCOUNT | 100 | Canonical accounts |
| GOLD.DW_SECURITY | 200 | Complete security master |
| GOLD.DW_POSITION | 4,886 | Position-level grain |
| GOLD.DW_TRADE_LOT | 12,388 | Lot-level detail |

---

## The 11 Ambiguities

| # | Name | Where | What breaks |
|---|---|---|---|
| A1 | Security ID fragmentation | Raw | CUSIP/ticker/ISIN with no shared key |
| A2 | Price source divergence | Raw | Custodian/PM/NAV prices blended |
| A3 | Column name heterogeneity | Raw | Different names for same concept |
| A4 | Account ID fragmentation | Raw | No link between source account keys |
| A5 | Position grain mismatch | Raw | Lot-level vs position-level mixed |
| A6 | Date field semantics | Raw | Settlement/trade/NAV dates conflated |
| A7 | Mixed-grain record IDs | Silver | LOT-* and POS-* coexist |
| A8 | Unmastered security IDs | Silver | ~15% NULL security_master_id |
| A9 | Cost basis fragmentation | Silver | Lot/avg/book cost blended |
| A10 | Asset class gap | Silver | ~15% NULL asset_class |
| A11 | NULL unrealized G/L | Silver | Ruby rows have NULL (22% of table) |

Full detail: `docs/ambiguity_registry_v2.md`
