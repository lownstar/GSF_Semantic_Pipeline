> **DEPRECATED** — This registry documents v1 within-schema ambiguities (single star schema).
> The active demo uses v2 cross-source ambiguities across the Topaz, Emerald, and Ruby source systems.
> See [`ambiguity_registry_v2.md`](ambiguity_registry_v2.md) for the current registry.
> V1 generator code is preserved in `generator/` as reference only and is not loaded into any pipeline.

---

# Ambiguity Registry (V1 — Deprecated)

This document is the **design contract** for the Semantic Layer Bias demo.

Each entry describes one intentional schema ambiguity baked into the seed data:
- What the raw column(s) look like
- Why they are ambiguous
- Which Cortex question they will trip up in Env A (raw)
- How the semantic model in Env B resolves them

These ambiguities are the mechanism by which the demo works. They are not bugs —
they are the same ambiguities that exist in real production financial data schemas
when no semantic layer is applied.

---

## Ambiguity #1 — `price`: same name, two meanings

**Affected tables:** `fact_position.price`, `fact_transaction.price`

**What the raw schema looks like:**
Both tables have a column named `price` (DECIMAL). No further description.

**Why it's ambiguous:**
- `fact_position.price` = end-of-period **market price** (closing price as of `as_of_date`)
- `fact_transaction.price` = **execution price** at time of trade, including slippage

These are semantically different values. A security's end-of-day market price on a
month-end snapshot date is not the same as the price at which a specific buy or sell
was executed weeks earlier. The raw schema gives Cortex no way to distinguish them.

**What Cortex A gets wrong:**
Asked *"What is the current value of account ACC-0042?"*, Cortex may join or confuse
the two `price` columns — using execution price from `fact_transaction` instead of
market price from `fact_position`, producing stale or incorrect valuations.

**What the semantic model defines (Env B):**
- `market_price` — explicitly sourced from `fact_position.price`, labeled "End-of-period closing market price"
- `execution_price` — explicitly sourced from `fact_transaction.price`, labeled "Trade execution price including slippage"
- `portfolio_value` — defined as `SUM(quantity * market_price)` at the `fact_position` grain

Cortex B routes to the correct source automatically based on the question type.

---

## Ambiguity #2 — `return_*`: two tables, five columns, no default

**Affected tables:** `fact_position` (return_1d, return_mtd, return_qtd, return_ytd, return_1yr),
`fact_return` (return_gross, return_net, return_twr, return_mwr)

**What the raw schema looks like:**
Return data exists in two separate tables. `fact_position` has position-level return
columns (gross, price-change-only). `fact_return` has account-level returns in five
variants. No column is labeled as "the one to use."

**Why it's ambiguous:**
When a user asks for "the return" of an account, Cortex must resolve:
1. Which table? (position-level `fact_position` vs account-level `fact_return`)
2. Which column? (`return_gross` vs `return_net` — difference is fee drag)
3. Which calculation method? (`return_twr` vs `return_mwr` — different for accounts with cash flows)

The raw schema provides no guidance. Cortex will typically pick the first or
most syntactically similar column — often `return_gross` from `fact_return`, or
`return_ytd` from `fact_position` (simpler table name).

**What Cortex A gets wrong:**
- *"What is the return for account 1042 year to date?"* → Cortex picks `return_gross`
  instead of `return_net`, overstating performance by fee drag
- *"Which asset class performed best last quarter?"* → Cortex may aggregate
  position-level `return_qtd` (gross, position-weighted) instead of account-level
  `return_net` (net, TWR), producing different rankings

**What the semantic model defines (Env B):**
- `portfolio_return` — canonical metric, defined as `return_net` from `fact_return`
  where `return_period` matches the requested period, using `return_twr`
- `gross_return` — available as a labeled alternative
- `position_return` — explicitly scoped to `fact_position` for position-level analysis

---

## Ambiguity #3 — `trade_date` vs `settlement_date`: T+2 is invisible

**Affected table:** `fact_transaction`

**What the raw schema looks like:**
Two date columns: `trade_date` (DATE) and `settlement_date` (DATE). No description.

**Why it's ambiguous:**
Financial transactions have two dates:
- **Trade date** (T+0): the date the transaction was agreed/executed
- **Settlement date**: the date cash and securities actually change hands
  - Equities: T+2 business days
  - Bonds: T+1 business day
  - Cash equivalents: T+0 (same day)

For questions about "what happened in a given period," the correct date depends on context:
- Portfolio manager / performance view → trade date
- Cash management / accounting view → settlement date
- Regulatory reporting → often settlement date

The raw schema exposes both columns with no guidance. Cortex will arbitrarily pick one —
typically `trade_date` because it comes first in the schema or has the word "trade."

**What Cortex A gets wrong:**
*"Show me all transactions that settled in January 2024"* — Cortex filters on
`trade_date = January 2024` instead of `settlement_date = January 2024`. This
excludes trades placed December 30–31, 2023 that settled in early January 2024,
and may include trades placed late January that settled in February. The result
set is wrong by a systematic, non-obvious margin.

**What the semantic model defines (Env B):**
- `transaction_date` dimension — defaults to `trade_date` for performance attribution
- `settlement_date` dimension — explicitly scoped for cash flow and accounting queries
- `settled_transactions` metric — filtered to `settlement_date` automatically

---

## Ambiguity #4 — `realized_gain_loss`: mixed tax lot methods

**Affected table:** `fact_transaction`

**What the raw schema looks like:**
`realized_gain_loss` (DECIMAL) and `tax_lot_method` (VARCHAR: FIFO/LIFO/SPECIFIC_ID).

**Why it's ambiguous:**
The realized gain or loss on a sale depends entirely on which tax lots are closed:
- **FIFO** (First In, First Out): oldest shares sold first
- **LIFO** (Last In, First Out): newest shares sold first
- **SPECIFIC_ID**: advisor identifies which specific lots to sell

Each method produces a different gain/loss number for the same sale. The seed data
assigns methods randomly per SELL transaction (70% FIFO, 20% SPECIFIC_ID, 10% LIFO),
simulating a realistic portfolio where different lot methods are used.

When Cortex sums `realized_gain_loss` across an account, it adds together values
calculated under different methods — which is mathematically valid row-by-row but
conceptually incoherent as an account total. No CPA would accept this aggregate.

**What Cortex A gets wrong:**
*"What is the total realized gain for account ACC-0101 this year?"* — Cortex will
`SUM(realized_gain_loss)` directly, mixing FIFO, LIFO, and SPECIFIC_ID figures.
The result is a number, but it doesn't represent any standard tax or accounting metric.

**What the semantic model defines (Env B):**
- `fifo_realized_gain` — metric defined as `SUM(realized_gain_loss) WHERE tax_lot_method = 'FIFO'`
  labeled as the standard for performance and tax reporting
- `realized_gain_loss` — accessible as a raw dimension for advanced users, with a
  description warning about the mixed lot method issue

---

## Ambiguity #5 — `return_annualized`: period-dependent, but period isn't filtered

**Affected table:** `fact_return`

**What the raw schema looks like:**
`return_annualized` (DECIMAL) column alongside `return_period` (VARCHAR: MTD/QTD/YTD/1YR/3YR/5YR/INCEPTION).

**Why it's ambiguous:**
"Annualized return" has a different calculation basis depending on the period:
- `return_period = '1YR'` → annualized 1-year return (simple)
- `return_period = '3YR'` → annualized 3-year return (geometric mean over 3 years)
- `return_period = '5YR'` → annualized 5-year return (geometric mean over 5 years)
- `return_period = 'INCEPTION'` → annualized since inception (variable length)

`fact_return` has multiple rows per account per month-end — one per `return_period`.
When Cortex queries `return_annualized` without filtering `return_period`, it will
average or aggregate across all periods — mixing a 1YR annualized return with a
3YR annualized return for the same account. This is statistically meaningless.

**What Cortex A gets wrong:**
*"Show me accounts with annualized returns above 8%"* — Cortex will not add a
`WHERE return_period = '3YR'` filter before querying `return_annualized`. It may:
- Return accounts where any period's annualized return exceeded 8% (too broad)
- Average across all periods for each account (mathematically invalid)
- Use the 1YR figure only if the column ordering happens to favor it (unpredictable)

**What the semantic model defines (Env B):**
- `annualized_return_3yr` — defined as `return_annualized WHERE return_period = '3YR'`,
  labeled as the standard long-term comparison metric
- `annualized_return_1yr`, `annualized_return_5yr` — available as labeled alternatives
- All annualized return metrics enforce the `return_period` filter at definition time,
  so Cortex never receives unfiltered rows

---

## Ambiguity #6 — `benchmark_return`: which benchmark?

**Affected tables:** `fact_return.benchmark_return`, `fact_benchmark_return`

**What the raw schema looks like:**
`fact_return` has a `benchmark_return` column (DECIMAL). There is also a separate
`fact_benchmark_return` table with benchmark returns by `benchmark_id`. Both are
potential sources for "the benchmark return."

**Why it's ambiguous:**
Each account is assigned a specific benchmark (`dim_account.benchmark_id`). The
`benchmark_return` column on `fact_return` stores the return of *that account's
assigned benchmark* for the corresponding period — but the raw schema doesn't say which
benchmark it is. To know, you must JOIN: `fact_return → dim_account → dim_benchmark`.

Meanwhile, `fact_benchmark_return` contains returns for all 10 benchmarks. Cortex may:
1. Use `fact_return.benchmark_return` without knowing it's account-specific
2. Join `fact_benchmark_return` directly with an incorrect or missing JOIN condition,
   comparing every account against the same benchmark
3. Aggregate `benchmark_return` across accounts that have different assigned benchmarks

**What Cortex A gets wrong:**
*"Show me portfolios underperforming their benchmark"* — Cortex will likely compare
`fact_return.return_net` against `fact_benchmark_return.return_value` with an
ambiguous JOIN, potentially comparing every account to the S&P 500 regardless of
whether that account's benchmark is actually the S&P 500. Endowment accounts
benchmarked against the 60/40 blended index will appear to underperform or outperform
based on the wrong comparison.

**What the semantic model defines (Env B):**
- `benchmark_return` metric — explicitly defined as:
  `fact_benchmark_return.return_value JOIN dim_account.benchmark_id JOIN dim_benchmark.benchmark_id`
  ensuring each account is compared to its own assigned benchmark
- `active_return` metric — defined as `portfolio_return - benchmark_return` using
  the correctly-joined benchmark, with `return_net` (not gross) as the portfolio side
- The entire `benchmark_return` derivation path is surfaced in the semantic model,
  making the JOIN logic visible and auditable

---

## Summary Table

| # | Column(s) | Table(s) | Why It Fails | Cortex Question |
|---|---|---|---|---|
| 1 | `price` | position + transaction | Same name, two semantic meanings | "What is the current value of ACC-0042?" |
| 2 | `return_*` | position + return | Two tables, five columns, no default | "What is the return for account 1042 YTD?" |
| 3 | `trade_date` vs `settlement_date` | transaction | T+2 settlement is invisible | "Show me transactions settled in Jan 2024" |
| 4 | `realized_gain_loss` | transaction | Mixed tax lot methods per row | "Total realized gain for ACC-0101 this year?" |
| 5 | `return_annualized` | return | Period not enforced; mixing 1YR/3YR | "Accounts with annualized returns above 8%?" |
| 6 | `benchmark_return` | return + benchmark_return | Wrong or missing JOIN path | "Portfolios underperforming their benchmark?" |

---

## Usage in Downstream Epics

**Epic 3 (Pipeline B / Semantic Model):**
Each ambiguity maps to one or more metric/dimension definitions in the semantic model YAML.
The YAML should reference this registry explicitly in its comments.

**Epic 4 (Cortex Analyst Question Set):**
The standard question set should include at least one question designed to expose each
of the 6 ambiguities. The expected correct answer (from Env B) should be pre-computed
from the seed data before running Cortex, so variance can be measured objectively.
