# Ambiguity Registry V2 — Cross-Source (Gemstone Systems)

This document is the **design contract** for the V2 Semantic Layer Bias demo.

Each entry describes one intentional ambiguity baked into the demo data:
- What the raw source columns look like across the three systems
- Why they are ambiguous when queried without a semantic layer
- Which Cortex Analyst question they will trip up in Env A (raw)
- How the semantic model in Env B resolves them

These ambiguities are the mechanism by which the demo works. They are not bugs —
they are the same ambiguities that exist in real production financial data environments
when no semantic layer governs multiple source systems.

This registry is referenced by:
- `semantic_model/positions_gold.yaml` (Epic 3) — each ambiguity maps to one or more metric/dimension definitions
- The Cortex Analyst standard question set (Epic 4) — at least one question per ambiguity

**A1–A6** exist in the three raw gemstone source files (`positions_topaz.csv`,
`positions_emerald.csv`, `positions_ruby.csv`) — heterogeneous schemas, identifier
types, grains, and price sources that make Cortex fail when querying separate tables.

**A7–A11** exist in the naive integration table (`positions_integrated.csv`) — a single
table produced by an ETL that maps column names to a common schema but makes no semantic
guarantees. This is the more realistic and more insidious failure mode: the data *looks*
normalized, but the semantic landmines are hidden inside it.

---

## Ambiguity A1 — Security ID Fragmentation

**Affected columns:**
| Source | Column | Identifier Type |
|---|---|---|
| Topaz | `SEC_CUSIP` | CUSIP (9-character alphanumeric) |
| Emerald | `securityTicker` | Proprietary internal ticker |
| Ruby | `isin_identifier` | ISIN (12-character alphanumeric) |

**What the raw schema looks like:**
Three tables, each referencing the same security universe using a different identifier type.
No shared key. No cross-reference table in `RAW_ENV`.

**Why it's ambiguous:**
CUSIP, ticker, and ISIN are all valid security identifiers but are not directly comparable.
Apple Inc. is `037833100` (CUSIP), `AAPL` (ticker), and `US0378331005` (ISIN) — three
different strings for the same security. Without a security master join, Cortex cannot
determine that these three rows describe the same holding.

**What Cortex A gets wrong:**
*"What is the total quantity held in Apple across all accounts?"* — Cortex cannot
join Topaz, Emerald, and Ruby rows for Apple because there is no shared key.
It will either query only one source (undercounting) or return three disconnected
result sets with no way to aggregate them correctly.

**What the semantic model defines (Env B):**
- `security_id` — canonical join key defined in `DW_SECURITY`
- `DW_SECURITY` contains `cusip`, `isin`, `ticker` as separate columns with explicit mappings:
  - `TOPAZ.SEC_CUSIP` → `DW_SECURITY.cusip` → `security_id`
  - `EMERALD.securityTicker` → `DW_SECURITY.ticker` → `security_id`
  - `RUBY.isin_identifier` → `DW_SECURITY.isin` → `security_id`
- All cross-source position queries resolve through `security_id` automatically

---

## Ambiguity A2 — Price Source Divergence

**Affected columns:**
| Source | Column | Price Type |
|---|---|---|
| Topaz | `MKT_PRC` | Custodian end-of-day closing price |
| Emerald | `unitPrice` | PM system evaluated price |
| Ruby | `nav_per_share` | Fund NAV-based price |

**What the raw schema looks like:**
Each source has a price column with a different name and a different pricing methodology.
All three represent "the price of the security" for the same date, but they are not the same number.

**Why it's ambiguous:**
- Custodian closing price: official exchange close, used for regulatory reporting
- PM evaluated price: may include fair value adjustments, OTC marks, or model prices
- NAV price: fund-level net asset value calculation, may lag or differ due to fee accruals

For liquid equities the differences are small (~0.1–0.5%). For fixed income or alternatives
they can be material. The raw schema gives Cortex no basis for choosing which price to use
or even knowing the three columns represent the same concept.

**What Cortex A gets wrong:**
*"What is the total market value of account ACC-0042 as of last month-end?"* — Cortex
will pick one price column without knowing the semantic difference. If it joins Topaz and
Emerald data, it may mix custodian and PM prices for different securities in the same
portfolio, producing a market value figure that no source system would actually report.

**What the semantic model defines (Env B):**
- `market_price` — canonical metric sourced from `DW_POSITION.market_price`
  (which uses custodian closing price as the authoritative source)
- `pm_evaluated_price` — available as a labeled alternative, sourced from Emerald
- `nav_price` — available as a labeled alternative, sourced from Ruby
- `market_value` — defined as `quantity × market_price` at the canonical DW grain,
  ensuring a consistent price source across all accounts and securities

---

## Ambiguity A3 — Column Name Heterogeneity

**Affected columns (account identifier):**
| Source | Column | Meaning |
|---|---|---|
| Topaz | `ACCT_NUM` | Custodian account number |
| Emerald | `portfolioId` | Portfolio management code |
| Ruby | `fund_code` | Fund accounting code |

**Affected columns (market value):**
| Source | Column | Meaning |
|---|---|---|
| Topaz | `MKT_VAL` | Market value |
| Emerald | `marketValue` | Market value |
| Ruby | `total_nav_value` | Market value |

**What the raw schema looks like:**
`ACCT_NUM`, `portfolioId`, and `fund_code` are three different column names for the same
concept (the account). `MKT_VAL`, `marketValue`, and `total_nav_value` are three different
column names for the same metric. No aliasing, no mapping, no description.

**Why it's ambiguous:**
Cortex Analyst relies on column names and descriptions to interpret natural language queries.
With three different names for the same concept across three tables, Cortex must guess which
column to use. It may pick the most syntactically similar column to the query term, or query
only one source, or fail to aggregate across sources entirely.

**What Cortex A gets wrong:**
*"Show me all positions with unrealized losses greater than $10,000"* — Cortex must find
the unrealized gain/loss column across three tables: `UNRLZD_GL` (Topaz), `unrealizedPnL`
(Emerald), and Ruby (which has no unrealized G/L column at all — it's a fund accounting
system). Without explicit mappings, Cortex will either miss one source or return an
error on the column lookup.

**What the semantic model defines (Env B):**
- `account_id` — canonical account dimension, mapped from all three source keys via `DW_ACCOUNT`
- `market_value` — canonical metric with a single definition, regardless of source column name
- `unrealized_gain_loss` — canonical metric sourced from `DW_POSITION.unrealized_gain_loss`,
  populated for sources that provide it and null-safe for those that do not (Ruby)
- All logical field names use plain English labels with explicit source column mappings in comments

---

## Ambiguity A4 — Account Identifier Fragmentation

**Affected columns:**
| Source | Column | Key Type |
|---|---|---|
| Topaz | `ACCT_NUM` | Custodian account number (e.g., `C-004291`) |
| Emerald | `portfolioId` | Portfolio code (e.g., `PORT-042`) |
| Ruby | `fund_code` | Fund code (e.g., `FND-42`) |

**What the raw schema looks like:**
All three source systems track accounts but use their own internal identifiers.
There is no foreign key, no lookup table, and no shared format linking `C-004291`,
`PORT-042`, and `FND-42` to the same client account in `RAW_ENV`.

**Why it's ambiguous:**
Aggregating a portfolio's total position across all three source systems requires knowing
that these three identifiers refer to the same client. Without an account master join,
Cortex cannot perform this aggregation. It will either query one source (incomplete) or
return three separate rows for the same account with no indication they should be summed.

**What Cortex A gets wrong:**
*"What is the total cost basis for the fixed income sleeve across all accounts?"* —
Cortex must aggregate across Topaz, Emerald, and Ruby. Without knowing that
`C-004291 = PORT-042 = FND-42`, it cannot sum correctly. The result will either
be wrong (missing one or two sources) or Cortex will attempt a cross-join that
produces a cartesian product.

**What the semantic model defines (Env B):**
- `DW_ACCOUNT` — canonical account master with all three source keys as columns:
  `custodian_account_num` (Topaz), `portfolio_code` (Emerald), `fund_code` (Ruby)
- `account_id` — single canonical key used in all DW tables and semantic model joins
- Cross-source account aggregation is a single `GROUP BY account_id` in the governed environment

---

## Ambiguity A5 — Position Grain Mismatch

**Affected tables:**
| Source | Grain | Notes |
|---|---|---|
| Topaz | Lot-level | One row per account × security × lot |
| Emerald | Position-level | One row per account × security (lots collapsed) |
| Ruby | Position-level | One row per fund × security (lots collapsed) |

**What the raw schema looks like:**
Topaz has multiple rows per account × security combination (one per tax lot).
Emerald and Ruby have one row per account × security with aggregated quantities.
No grain label exists in any source schema.

**Why it's ambiguous:**
If Cortex queries `SUM(quantity)` across all three sources for the same account and
security, it will double- or triple-count Topaz lot rows against the already-aggregated
Emerald and Ruby rows. The grain mismatch is invisible in the raw schema — all three
tables look like position tables, and `quantity` or its equivalent appears in all three.

**What Cortex A gets wrong:**
*"What is the total quantity held in Apple across all accounts?"* — Cortex sums
`UNITS` from Topaz (lot-level, e.g., 3 lots × 100 shares = 300 rows contributing
300 each) against `quantity` from Emerald (already summed to 300) and `shares_held`
from Ruby (already summed to 300). The result is 900 instead of 300.

**What the semantic model defines (Env B):**
- `DW_POSITION` — canonical position-level grain (lots collapsed)
- `DW_TRADE_LOT` — canonical lot-level grain (Topaz source detail preserved)
- Semantic model surfaces these as separate labeled contexts:
  - `position_quantity` — from `DW_POSITION`, safe for cross-source aggregation
  - `lot_quantity` — from `DW_TRADE_LOT`, explicitly scoped to lot-level analysis
- Cortex B routes quantity questions to `position_quantity` by default, preventing double-counting

---

## Ambiguity A6 — Date Field Naming and Semantics

**Affected columns:**
| Source | Column | Date Semantic |
|---|---|---|
| Topaz | `AS_OF_DT` | Settlement/custody date |
| Emerald | `positionDate` | Trade date (portfolio manager view) |
| Ruby | `nav_date` | NAV strike date (fund accounting) |

**What the raw schema looks like:**
Each source has one date column representing "the date of the position snapshot."
All three are DATE type. All three will contain the same calendar date for a given
month-end position file. The column names and their semantics differ.

**Why it's ambiguous:**
Settlement date (Topaz), trade date (Emerald), and NAV strike date (Ruby) can fall
on different calendar days around period boundaries:
- A trade executed December 31 (trade date) may settle January 2 (settlement date)
- A fund's NAV may be struck on a different day than the custodian's closing price date

For most mid-month queries the three dates are identical. At month-end, quarter-end,
and year-end boundaries they diverge — exactly when period-end reporting matters most.
Cortex treats all three as equivalent date filters with no awareness of the semantic difference.

**What Cortex A gets wrong:**
*"Show me all positions as of December 31, 2024"* — Cortex filters each source on
its respective date column against `2024-12-31`. For Topaz, this returns positions
with settlement date 12/31 (which may exclude trades that settled 1/2/2025 but were
economically owned on 12/31). For Emerald, it returns trade-date positions correctly.
For Ruby, the NAV date may be 12/30 due to fund holiday calendars. The result set
spans three different effective dates presented as a single coherent snapshot.

**What the semantic model defines (Env B):**
- `position_date` — canonical date dimension sourced from `DW_POSITION.position_date`
  (normalized to trade date as the standard for portfolio reporting)
- `settlement_date` — available as a labeled dimension for custody/accounting queries
- `nav_date` — available as a labeled dimension for fund-level queries
- All period-end position queries default to `position_date` (trade date basis),
  consistent with GIPS performance reporting standards

---

---

## Ambiguity A7 — Mixed-Grain Record IDs

**Where it lives:** `POSITIONS_INTEGRATED.record_id`

**What the integrated schema looks like:**
The naive ETL assigns a `record_id` to every row:
- Topaz rows: original lot ID (e.g., `LOT-0000001`) — one per tax lot
- Emerald rows: fabricated composite key (e.g., `POS-PORT-0042-AAPL`) — one per position
- Ruby rows: fabricated composite key (e.g., `NAV-FND-0042-US037833100X`) — one per position

All three formats are VARCHAR in the same column. There is no `grain` column. The
`source_system` column is present but a naive query against `POSITIONS_INTEGRATED`
will not filter on it.

**Why it causes silent double-counting:**
For any given account × security, Topaz contributes 2–4 lot rows (lot-level grain) while
Emerald and Ruby each contribute exactly 1 row (position-level grain). A naive
`SUM(quantity)` without a `WHERE source_system = ...` filter produces a result roughly
2.5× higher than the true position for any security held in Topaz, and 3× higher when
all three sources contribute.

**What Cortex A gets wrong:**
*"What is the total quantity of fixed income securities held across all portfolios?"*
Cortex sums `quantity` across all rows. Topaz lots inflate the result relative to
Emerald/Ruby position rows. The overcount factor varies by security (proportional to
its average lot count) and is not detectable from the result set.

**What the semantic model defines (Env B):**
- `DW_POSITION.quantity` — canonical position-level grain, lots already collapsed
- `DW_TRADE_LOT.remaining_quantity` — lot-level grain, explicitly labeled
- Semantic model routes all position quantity metrics to `DW_POSITION` by default;
  lot-level analysis is a separate, explicitly labeled context

---

## Ambiguity A8 — Unmastered Security IDs

**Where it lives:** `POSITIONS_INTEGRATED.security_master_id` (NULL for ~15% of rows)

**What the integrated schema looks like:**
The naive ETL attempted to resolve each source's security identifier (CUSIP, ticker, ISIN)
against a security master and write the canonical `security_master_id` as a foreign key.
For ~30 of 200 securities, the resolution failed — the security exists in the source files
but has no entry in the master. These rows have:
- `security_ref`: a valid raw identifier (CUSIP, ticker, or ISIN from the source)
- `security_ref_type`: the identifier type
- `security_master_id`: NULL (join failed)
- `asset_class`: NULL (cannot be populated without a master record)

These securities are fully mastered in `DW_SECURITY` (Env B). The gap is the Naive Pipeline's
failure, not a fundamental data quality issue.

**Why it causes silent exclusion:**
Any query that joins `POSITIONS_INTEGRATED` on `security_master_id` silently drops all
NULL rows. `SUM(market_value)` for "total portfolio AUM" is systematically understated by
the value of unmastered positions — by an unknown and unannounced amount. The result
looks complete but is not.

**What Cortex A gets wrong:**
*"What is the total market value of all fixed income positions?"*
Cortex joins on `security_master_id` to retrieve `asset_class`. All rows where
`security_master_id IS NULL` are dropped from the join. The result understates fixed
income AUM by the value of unmastered fixed income holdings.

**What the semantic model defines (Env B):**
- `DW_SECURITY` — complete security master, 200 entries, no unmastered gaps
- `DW_POSITION.security_id` — FK always resolves; no NULLs in the join path
- `total_market_value` metric computed entirely within the governed schema

---

## Ambiguity A9 — Cost Basis Semantic Fragmentation

**Where it lives:** `POSITIONS_INTEGRATED.cost_basis`

**What the integrated schema looks like:**
Three source systems report "cost" using three different accounting methods. The ETL
maps all three to a single `cost_basis` column:

| Source | Original column | Accounting method |
|---|---|---|
| Topaz | `COST_BASIS` | Specific identification: acquisition price × remaining lot quantity |
| Emerald | `avgCostBasis × quantity` | Average cost method: total cost ÷ shares, applied to current quantity |
| Ruby | `book_cost` | Book cost: original acquisition cost, not adjusted for partial redemptions |

All three methods produce different numbers for the same holding. The column name
`cost_basis` implies they are comparable. They are not.

**Why it's realistic:**
These are the three cost accounting methods actually used in financial services:
- Custody systems (Topaz): lot-level FIFO or specific identification
- Portfolio management systems (Emerald): average cost
- Fund accounting (Ruby): book cost per GAAP/IFRS

**What Cortex A gets wrong:**
*"What is the total unrealized gain for the equity sleeve?"*
Cortex computes `SUM(market_value - cost_basis)`. The result blends three incompatible
cost accounting methods. The figure would not match any system of record's reported
unrealized gain and cannot be audited or reconciled.

**What the semantic model defines (Env B):**
- `DW_POSITION.cost_basis` — sum of specific lot costs from `DW_TRADE_LOT` (highest
  quality method: specific identification)
- `unrealized_gain_loss` — defined as `market_value - cost_basis` at the DW grain,
  documented in the YAML as using specific identification (lot-level) costing

---

## Ambiguity A10 — Asset Class Classification Gap

**Where it lives:** `POSITIONS_INTEGRATED.asset_class` (NULL for unmastered rows)

**What the integrated schema looks like:**
`asset_class` is populated from the security master join — only for rows where
`security_master_id IS NOT NULL`. All ~15% unmastered rows have `asset_class = NULL`.

The NULL rows are distributed across all three source systems (Topaz, Emerald, Ruby)
and across multiple asset classes. Some fixed income securities are unmastered.

**Why it distorts allocation analytics:**
Sleeve-level percentages (equity %, fixed income %, alternatives %) computed from
`POSITIONS_INTEGRATED` are computed against a partial AUM base. The percentages are
internally consistent — they add up to 100% — but they represent the wrong universe.
No warning, no error, no indication that any positions were excluded.

**What Cortex A gets wrong:**
*"What percentage of AUM is allocated to fixed income?"*
Cortex computes:
```sql
SUM(market_value) FILTER (WHERE asset_class = 'Fixed Income')
/ SUM(market_value) FILTER (WHERE asset_class IS NOT NULL)
```
Both the numerator and denominator exclude unmastered positions. The ratio looks
plausible but is computed on a partial universe, and the denominator already excludes
the unmastered positions — so the allocation percentages are wrong for every sleeve.

**What the semantic model defines (Env B):**
- Asset class allocation percentages defined as ratios over `total_aum`, which is
  computed from `DW_POSITION` — complete, no NULLs in the FK path
- Both numerator and denominator computed from the same complete governed universe

---

## Ambiguity A11 — NULL Unrealized G/L from Ruby

**Where it lives:** `POSITIONS_INTEGRATED.unrealized_gl` (NULL for all Ruby rows)

**What the integrated schema looks like:**
Ruby is a fund accounting system. Fund accounting tracks book cost but does not publish
unrealized gain/loss — that is a portfolio management concept. The ETL has no source
value to write, so `unrealized_gl = NULL` for all 4,886 Ruby rows (~22% of all rows).

Topaz and Emerald both provide unrealized G/L, so 78% of rows are non-NULL. The column
looks populated.

**Why it causes silent exclusion:**
A filter `WHERE unrealized_gl < -10000` silently excludes all Ruby rows. A
`SUM(unrealized_gl)` is implicitly `SUM(unrealized_gl) FILTER (WHERE source_system != 'RUBY')`.
The result covers only two of three source systems with no indication of the gap.

**What Cortex A gets wrong:**
*"Show me all positions with unrealized losses greater than $10,000"*
Cortex queries `unrealized_gl < -10000`. All Ruby-sourced positions (22% of the table)
are excluded. Some of those positions may have unrealized losses when priced at the
custodian or PM price — they simply aren't reported here because Ruby doesn't publish
the figure.

**What the semantic model defines (Env B):**
- `DW_POSITION.unrealized_gain_loss` — computed at the canonical grain from
  `market_value - cost_basis` using the custodian closing price. Complete for all
  positions regardless of source system origin.
- No NULLs in this column for the governed environment

---

## Summary Table

| # | Ambiguity | Layer | Why It Fails | Demo Question |
|---|---|---|---|---|
| A1 | Security ID fragmentation | Raw source files | No shared key without security master | "Total quantity in Apple across all accounts?" |
| A2 | Price source divergence | Raw source files | Same security, same date, three prices | "Total market value of ACC-0042 at month-end?" |
| A3 | Column name heterogeneity | Raw source files | Three names for account; three for market value | "Positions with unrealized losses > $10K?" |
| A4 | Account ID fragmentation | Raw source files | No FK linking the three account identifiers | "Total cost basis for fixed income across all accounts?" |
| A5 | Position grain mismatch | Raw source files | Summing across grains double-counts | "Total quantity in Apple across all accounts?" |
| A6 | Date field semantics | Raw source files | Settlement / trade / NAV dates diverge at period-end | "All positions as of December 31, 2024?" |
| A7 | Mixed-grain record IDs | Integrated table | Lot IDs and position IDs coexist; SUM double-counts Topaz | "Total quantity of fixed income across all portfolios?" |
| A8 | Unmastered security IDs | Integrated table | security_master_id NULL for 15%; JOIN silently drops them | "Total market value of all fixed income positions?" |
| A9 | Cost basis fragmentation | Integrated table | Lot cost / avg cost / book cost blended in one column | "Total unrealized gain for the equity sleeve?" |
| A10 | Asset class classification gap | Integrated table | asset_class NULL for unmastered rows; allocation % wrong | "What % of AUM is allocated to fixed income?" |
| A11 | NULL unrealized G/L from Ruby | Integrated table | Ruby has no G/L concept; 22% of rows silently excluded | "Show positions with unrealized losses > $10K" |

---

## Usage in Downstream Epics

**Epic 2 (the Naive Pipeline):**
The naive integration table (`POSITIONS_INTEGRATED`) is the primary the Naive Pipeline demo
artifact. The three raw source tables (`TOPAZ_POSITIONS`, `EMERALD_POSITIONS`,
`RUBY_POSITIONS`) serve as the pre-integration staging state for the three-act demo
narrative. Ambiguities A1–A6 are visible in the raw tables; A7–A11 are the more
insidious failures hidden in the integrated table.

**Epic 3 (Semantic Enriched Pipeline / Semantic Model):**
Each ambiguity maps to one or more metric/dimension definitions in `semantic_model/positions_gold.yaml`.
The YAML should reference this registry explicitly in its comments (e.g., `# resolves A1`).

**Epic 4 (Cortex Analyst Question Set):**
The standard question set must include at least one question designed to expose each of the
eleven ambiguities. The expected correct answer (from Env B) should be pre-computed from
`data/seed_v2/` before running Cortex, so variance can be measured objectively.
