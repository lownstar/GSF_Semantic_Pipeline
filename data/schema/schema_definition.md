# Schema Definition — Semantic Layer Bias Study

Synthetic financial portfolio management schema.
Date range: 2020-01-01 to 2024-12-31.
All monetary values in USD.

Ambiguous columns are marked ⚠️. See [docs/ambiguity_registry.md](../../docs/ambiguity_registry.md) for full detail.

---

## Dimension Tables

### dim_date
One row per calendar day.

| Column | Type | Description |
|---|---|---|
| date_id | DATE (PK) | Calendar date |
| calendar_year | INT | Year |
| calendar_quarter | INT | Quarter (1–4) |
| calendar_month | INT | Month (1–12) |
| month_name | VARCHAR | Month name |
| week_of_year | INT | ISO week number |
| day_of_week | INT | 0=Monday, 6=Sunday |
| is_weekday | BOOL | True for Mon–Fri |
| is_month_end | BOOL | Last day of month |
| is_quarter_end | BOOL | Last day of quarter |
| is_year_end | BOOL | December 31 |
| fiscal_year | INT | Same as calendar year |
| fiscal_quarter | INT | Same as calendar quarter |

---

### dim_asset_class
8 rows. Static reference data.

| Column | Type | Description |
|---|---|---|
| asset_class_id | VARCHAR (PK) | e.g. "AC-01" |
| asset_class_name | VARCHAR | e.g. "Equity" |
| asset_class_code | VARCHAR | EQ, FI, ALT, CASH, RE, CMDTY, HF, PE |
| sort_order | INT | Display ordering |
| is_liquid | BOOL | Whether the asset class is typically liquid |

---

### dim_benchmark
10 rows. Static reference data.

| Column | Type | Description |
|---|---|---|
| benchmark_id | VARCHAR (PK) | e.g. "BM-01" |
| benchmark_name | VARCHAR | e.g. "S&P 500 Index" |
| benchmark_code | VARCHAR | SPX, AGG, MSCI_WORLD, etc. |
| benchmark_type | VARCHAR | EQUITY_INDEX, BOND_INDEX, BLENDED, CASH, CUSTOM |
| description | VARCHAR | Free-text description |

---

### dim_security
~500 rows. One per synthetic security.

| Column | Type | Description |
|---|---|---|
| security_id | VARCHAR (PK) | e.g. "SEC-00042" |
| ticker | VARCHAR | Fake ticker symbol (3–5 chars) |
| security_name | VARCHAR | Synthetic company name |
| asset_class_id | VARCHAR (FK) | → dim_asset_class |
| security_type | VARCHAR | EQUITY, BOND, ETF, MUTUAL_FUND, CASH_EQUIV |
| sector | VARCHAR | Technology, Healthcare, etc. |
| country | VARCHAR(3) | ISO country code, 80% US |
| currency | VARCHAR(3) | USD |
| cusip | VARCHAR(9) | Fake 9-char CUSIP |
| isin | VARCHAR(12) | Fake ISIN |
| benchmark_id | VARCHAR (FK) | → dim_benchmark |
| is_active | BOOL | Active security flag |

---

### dim_account
~200 rows. One per synthetic investment account.

| Column | Type | Description |
|---|---|---|
| account_id | VARCHAR (PK) | e.g. "ACC-0042" |
| account_name | VARCHAR | Synthetic name (e.g. "Smith Family Trust") |
| account_type | VARCHAR | INDIVIDUAL, INSTITUTIONAL, TRUST, ENDOWMENT |
| inception_date | DATE | Account open date (2010–2020) |
| base_currency | VARCHAR(3) | USD |
| benchmark_id | VARCHAR (FK) | → dim_benchmark (account's assigned benchmark) |
| target_equity_pct | DECIMAL(5,2) | Target equity allocation % |
| target_fixed_pct | DECIMAL(5,2) | Target fixed income allocation % |
| target_alt_pct | DECIMAL(5,2) | Target alternatives allocation % |
| is_active | BOOL | Active account flag |
| custodian | VARCHAR | Custodian institution name |

---

## Fact Tables

### fact_position ⚠️
~36,000 rows. Month-end position snapshots. Contains Ambiguities #1 and #2.

| Column | Type | Description |
|---|---|---|
| position_id | VARCHAR (PK) | Surrogate key |
| account_id | VARCHAR (FK) | → dim_account |
| security_id | VARCHAR (FK) | → dim_security |
| as_of_date | DATE (FK) | Month-end snapshot date → dim_date |
| quantity | DECIMAL(18,4) | Shares/units held |
| **price** ⚠️ | DECIMAL(18,6) | **End-of-period MARKET PRICE** (not execution price) |
| market_value | DECIMAL(18,2) | quantity × price |
| cost_basis | DECIMAL(18,2) | Total cost basis for position |
| cost_basis_per_share | DECIMAL(18,6) | Average cost per share |
| unrealized_gain_loss | DECIMAL(18,2) | market_value − cost_basis |
| weight | DECIMAL(8,6) | Position weight within account (0–1) |
| **return_1d** ⚠️ | DECIMAL(10,6) | **Gross 1-day return** (price change only, no fee drag) |
| **return_mtd** ⚠️ | DECIMAL(10,6) | **Gross MTD return** |
| **return_qtd** ⚠️ | DECIMAL(10,6) | **Gross QTD return** |
| **return_ytd** ⚠️ | DECIMAL(10,6) | **Gross YTD return** |
| **return_1yr** ⚠️ | DECIMAL(10,6) | **Gross 1YR return** (null if < 12 months of data) |
| currency | VARCHAR(3) | USD |
| data_source | VARCHAR | CUSTODIAN, CALCULATED, or ESTIMATED |

---

### fact_transaction ⚠️
~15,000 rows. Transaction history. Contains Ambiguities #1, #3, and #4.

| Column | Type | Description |
|---|---|---|
| transaction_id | VARCHAR (PK) | Surrogate key |
| account_id | VARCHAR (FK) | → dim_account |
| security_id | VARCHAR (FK) | → dim_security |
| **trade_date** ⚠️ | DATE | **Date transaction was agreed** (T+0) |
| **settlement_date** ⚠️ | DATE | **Date cash/securities settled** (T+2 equities, T+1 bonds, T+0 cash) |
| transaction_type | VARCHAR | BUY, SELL, DIVIDEND, FEE, TRANSFER_IN, TRANSFER_OUT |
| quantity | DECIMAL(18,4) | Positive=buy, negative=sell, 0=non-trade |
| **price** ⚠️ | DECIMAL(18,6) | **EXECUTION PRICE** (not market price — different meaning than fact_position.price) |
| gross_amount | DECIMAL(18,2) | abs(quantity) × price |
| commission | DECIMAL(10,2) | Brokerage commission |
| fees | DECIMAL(10,2) | Other fees |
| net_amount | DECIMAL(18,2) | gross_amount − commission − fees |
| currency | VARCHAR(3) | USD |
| **realized_gain_loss** ⚠️ | DECIMAL(18,2) | **Gain/loss on SELL — computed under tax_lot_method for this row** |
| **tax_lot_method** ⚠️ | VARCHAR | **FIFO, LIFO, or SPECIFIC_ID (varies per row)** |
| notes | VARCHAR | Free text, usually null |

---

### fact_return ⚠️
~49,000 rows. Account-level performance returns. Contains Ambiguities #2, #5, and #6.

| Column | Type | Description |
|---|---|---|
| return_id | VARCHAR (PK) | Surrogate key |
| account_id | VARCHAR (FK) | → dim_account |
| as_of_date | DATE | Month-end as-of date |
| **return_period** | VARCHAR | MTD, QTD, YTD, 1YR, 3YR, 5YR, INCEPTION |
| **return_gross** ⚠️ | DECIMAL(10,6) | **Gross return** (before fee drag) |
| **return_net** ⚠️ | DECIMAL(10,6) | **Net return** (after fee drag) — canonical for reporting |
| **return_annualized** ⚠️ | DECIMAL(10,6) | **Annualized net return — period-dependent (see Ambiguity #5)** |
| **return_twr** ⚠️ | DECIMAL(10,6) | **Time-Weighted Return** — standard for portfolio performance |
| **return_mwr** ⚠️ | DECIMAL(10,6) | **Money-Weighted Return** — includes cash flow timing effects |
| **benchmark_return** ⚠️ | DECIMAL(10,6) | **Return of account's assigned benchmark — which one? (see Ambiguity #6)** |
| **active_return** ⚠️ | DECIMAL(10,6) | **return_net − benchmark_return** |
| tracking_error | DECIMAL(10,6) | Standard deviation of active return |
| information_ratio | DECIMAL(10,6) | active_return / tracking_error |
| sharpe_ratio | DECIMAL(10,6) | risk-adjusted return |
| currency | VARCHAR(3) | USD |

---

### fact_benchmark_return
~600 rows. Monthly returns per benchmark per return period.

| Column | Type | Description |
|---|---|---|
| benchmark_return_id | VARCHAR (PK) | Surrogate key |
| benchmark_id | VARCHAR (FK) | → dim_benchmark |
| as_of_date | DATE | Month-end as-of date |
| return_period | VARCHAR | MTD, QTD, YTD, 1YR |
| return_value | DECIMAL(10,6) | Total return (price + income) |
| total_return_value | DECIMAL(10,6) | Same as return_value (explicit alias) |
| price_return_value | DECIMAL(10,6) | Price appreciation only (excludes yield) |

---

## Entity Relationships

```
dim_asset_class ──► dim_security ──► fact_position ◄── dim_account ◄── dim_benchmark
                                  ├─► fact_transaction
dim_benchmark ──────────────────────────────────────────────────────►
                   dim_benchmark ──► fact_benchmark_return
                                         ▲
                   fact_return ──────────┘ (via dim_account.benchmark_id)
dim_date ──────────────────────────────────────────────────────────────►
```
