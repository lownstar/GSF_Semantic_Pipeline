"""
Fact table generators.
Build order: fact_benchmark_return → fact_position → fact_transaction → fact_return

INTENTIONAL AMBIGUITIES are embedded here — see docs/ambiguity_registry.md for full details.
Each ambiguity is flagged with # AMBIGUITY #N comments.
"""

import pandas as pd
import numpy as np
from datetime import date

from generator import config
from generator.utils.date_utils import add_business_days, annualize_return, months_between


# ---------------------------------------------------------------------------
# fact_benchmark_return
# ---------------------------------------------------------------------------

def generate_fact_benchmark_return(
    dim_benchmark: pd.DataFrame,
    month_end_dates: list[date],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Monthly returns for each benchmark, for each return period.
    Builds monthly base returns first, then compounds to longer periods.
    """
    rows = []
    row_id = 1

    for _, bm in dim_benchmark.iterrows():
        bm_id   = bm["benchmark_id"]
        bm_type = bm["benchmark_type"]
        params  = config.BENCHMARK_RETURN_PARAMS[bm_type]

        # Generate a monthly return series for the full date range
        monthly_returns = rng.normal(params["mean"], params["std"], size=len(month_end_dates))

        for i, as_of in enumerate(month_end_dates):
            # MTD = current month's return
            mtd = float(monthly_returns[i])

            # QTD = compound of months in current quarter
            q_start_month = ((as_of.month - 1) // 3) * 3 + 1
            qtd_returns = [
                monthly_returns[j]
                for j, d in enumerate(month_end_dates)
                if d.year == as_of.year and d.month >= q_start_month and d.month <= as_of.month
            ]
            qtd = float(np.prod([1 + r for r in qtd_returns]) - 1)

            # YTD = compound of months in current year
            ytd_returns = [
                monthly_returns[j]
                for j, d in enumerate(month_end_dates)
                if d.year == as_of.year and d.month <= as_of.month
            ]
            ytd = float(np.prod([1 + r for r in ytd_returns]) - 1)

            # 1YR = compound of last 12 months
            one_yr_returns = [
                monthly_returns[j]
                for j, d in enumerate(month_end_dates)
                if 0 < (as_of.year * 12 + as_of.month) - (d.year * 12 + d.month) <= 12
            ]
            one_yr = float(np.prod([1 + r for r in one_yr_returns]) - 1) if len(one_yr_returns) == 12 else None

            # price return ≈ total return minus approximate yield
            approx_yield = {"BOND_INDEX": 0.0025, "CASH": 0.0015}.get(bm_type, 0.0005)

            for period, val in [("MTD", mtd), ("QTD", qtd), ("YTD", ytd), ("1YR", one_yr)]:
                if val is None:
                    continue
                rows.append({
                    "benchmark_return_id": f"BR-{row_id:07d}",
                    "benchmark_id":        bm_id,
                    "as_of_date":          as_of,
                    "return_period":       period,
                    "return_value":        round(val, 8),
                    "total_return_value":  round(val, 8),
                    "price_return_value":  round(val - approx_yield, 8),
                })
                row_id += 1

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# fact_position
# ---------------------------------------------------------------------------

def generate_fact_position(
    dim_account: pd.DataFrame,
    dim_security: pd.DataFrame,
    month_end_dates: list[date],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Month-end position snapshots per account.

    AMBIGUITY #1: 'price' column — this is end-of-period MARKET PRICE (closing price
    as of as_of_date). It is NOT execution price. Both fact_position and fact_transaction
    have a column named 'price' with different meanings. The raw schema provides no hint.

    AMBIGUITY #2: return_* columns — these are POSITION-LEVEL GROSS returns, calculated
    purely from price movements (no fee drag). fact_return has account-level net returns.
    Both tables have return columns; the raw schema doesn't indicate which to use.
    """
    account_ids  = dim_account["account_id"].tolist()
    security_ids = dim_security["security_id"].tolist()

    # Pre-generate per-security base prices (50–500)
    base_prices = {
        sid: float(rng.uniform(50, 500))
        for sid in security_ids
    }

    # Pre-assign holdings per account (fixed set of securities per account)
    min_h, max_h = config.HOLDINGS_PER_ACCOUNT
    account_holdings = {
        acc_id: list(rng.choice(security_ids, size=int(rng.integers(min_h, max_h + 1)), replace=False))
        for acc_id in account_ids
    }

    # Per-security monthly price evolution (random walk, shared across all accounts)
    # Shape: (n_securities, n_months)
    n_months = len(month_end_dates)
    sec_index = {sid: i for i, sid in enumerate(security_ids)}

    monthly_price_returns = rng.normal(0.006, 0.04, size=(len(security_ids), n_months))
    # Build cumulative price series from base
    price_matrix = np.zeros((len(security_ids), n_months))
    for i, sid in enumerate(security_ids):
        price_matrix[i, 0] = base_prices[sid] * (1 + monthly_price_returns[i, 0])
        for t in range(1, n_months):
            price_matrix[i, t] = price_matrix[i, t - 1] * (1 + monthly_price_returns[i, t])

    rows = []
    row_id = 1
    data_sources = ["CUSTODIAN", "CUSTODIAN", "CUSTODIAN", "CALCULATED", "ESTIMATED"]

    for acc_id in account_ids:
        holdings = account_holdings[acc_id]

        # Initial quantities per holding (100–10,000 shares)
        init_quantities = {
            sid: float(rng.uniform(100, 10_000))
            for sid in holdings
        }
        # Cost basis per share — set at inception, does not change (AMBIGUITY #4 mechanism)
        cost_basis_per_share = {
            sid: base_prices[sid] * float(rng.uniform(0.85, 1.15))
            for sid in holdings
        }

        for t, as_of in enumerate(month_end_dates):
            # Quantity evolves with small drift (simulate trades without full transaction detail)
            acct_positions = []
            for sid in holdings:
                si = sec_index[sid]
                qty_drift = float(rng.normal(1.0, 0.03))
                if t == 0:
                    qty = init_quantities[sid]
                else:
                    qty = init_quantities[sid] * qty_drift  # simplified drift
                qty = max(0.0, round(qty, 4))
                if qty == 0:
                    continue

                # AMBIGUITY #1: column named 'price' — this is MARKET PRICE, not execution price
                price = round(float(price_matrix[si, t]), 6)
                market_value = round(qty * price, 2)
                cb_per_share = cost_basis_per_share[sid]
                cost_basis   = round(qty * cb_per_share, 2)

                # AMBIGUITY #2: return columns — GROSS (no fee drag), position-level
                ret_1d   = round(float(monthly_price_returns[si, t]) / 21, 8)  # approx daily
                ret_mtd  = round(float(monthly_price_returns[si, t]), 8)
                # QTD: compound of months in same quarter
                q_start_month = ((as_of.month - 1) // 3) * 3 + 1
                qtd_months = [
                    j for j, d in enumerate(month_end_dates)
                    if d.year == as_of.year and q_start_month <= d.month <= as_of.month
                ]
                ret_qtd = round(float(np.prod([1 + monthly_price_returns[si, j] for j in qtd_months]) - 1), 8)
                # YTD: compound of months in same year
                ytd_months = [
                    j for j, d in enumerate(month_end_dates)
                    if d.year == as_of.year and d.month <= as_of.month
                ]
                ret_ytd = round(float(np.prod([1 + monthly_price_returns[si, j] for j in ytd_months]) - 1), 8)
                # 1YR: last 12 months
                one_yr_months = [
                    j for j, d in enumerate(month_end_dates)
                    if 0 < (as_of.year * 12 + as_of.month) - (d.year * 12 + d.month) <= 12
                ]
                ret_1yr = round(float(np.prod([1 + monthly_price_returns[si, j] for j in one_yr_months]) - 1), 8) \
                          if len(one_yr_months) == 12 else None

                acct_positions.append({
                    "_sid": sid, "_qty": qty, "_mv": market_value,
                    "position_id":          f"POS-{row_id:08d}",
                    "account_id":           acc_id,
                    "security_id":          sid,
                    "as_of_date":           as_of,
                    "quantity":             qty,
                    "price":                price,          # AMBIGUITY #1
                    "market_value":         market_value,
                    "cost_basis":           cost_basis,
                    "cost_basis_per_share": round(cb_per_share, 6),
                    "unrealized_gain_loss": round(market_value - cost_basis, 2),
                    "weight":               None,           # filled in second pass
                    "return_1d":            ret_1d,         # AMBIGUITY #2
                    "return_mtd":           ret_mtd,        # AMBIGUITY #2
                    "return_qtd":           ret_qtd,        # AMBIGUITY #2
                    "return_ytd":           ret_ytd,        # AMBIGUITY #2
                    "return_1yr":           ret_1yr,        # AMBIGUITY #2
                    "currency":             "USD",
                    "data_source":          rng.choice(data_sources),
                })
                row_id += 1

            # Second pass: compute weights within account/date
            total_mv = sum(p["_mv"] for p in acct_positions) or 1.0
            for p in acct_positions:
                p["weight"] = round(p["_mv"] / total_mv, 8)
                del p["_sid"], p["_qty"], p["_mv"]
                rows.append(p)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# fact_transaction
# ---------------------------------------------------------------------------

def generate_fact_transaction(
    dim_account: pd.DataFrame,
    dim_security: pd.DataFrame,
    business_days: list[date],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Transaction history for each account over the full date range.

    AMBIGUITY #1 (variant): 'price' column — this is EXECUTION PRICE at time of trade,
    including slippage noise. Same column name as fact_position.price but completely
    different semantic meaning.

    AMBIGUITY #3: 'trade_date' vs 'settlement_date' — settlement = trade_date + T+2 for
    equities, T+1 bonds, T+0 cash. Raw schema has both columns; Cortex must guess which
    to filter on. The semantic layer defines explicit contexts for each.

    AMBIGUITY #4: 'realized_gain_loss' — computed per-row using whichever tax lot method
    applies to that transaction (FIFO/LIFO/SPECIFIC_ID varies per row). Account-level SUM
    is mathematically possible but semantically incoherent.
    """
    account_ids  = dim_account["account_id"].tolist()
    security_ids = dim_security["security_id"].tolist()

    sec_type_lookup = dict(zip(dim_security["security_id"], dim_security["security_type"]))

    # Pre-generate per-security approximate daily prices via linear interpolation from
    # a simple random walk — used to price transactions on non-month-end dates.
    bday_index = {d: i for i, d in enumerate(business_days)}
    n_bdays = len(business_days)
    sec_base_prices = {sid: float(rng.uniform(50, 500)) for sid in security_ids}
    # Simple daily random walk for each security
    sec_daily_price_returns = rng.normal(0.0003, 0.012, size=(len(security_ids), n_bdays))
    sec_index = {sid: i for i, sid in enumerate(security_ids)}

    # Precompute daily price matrix (store only base × cumulative product per security)
    # We'll look up on demand to avoid huge memory allocation
    def get_execution_price(sid: str, trade_date: date) -> float:
        si = sec_index[sid]
        bi = bday_index.get(trade_date, 0)
        base = sec_base_prices[sid]
        cum_return = float(np.prod(1 + sec_daily_price_returns[si, :bi + 1]))
        return max(0.01, base * cum_return)

    rows = []
    row_id = 1

    min_tx, max_tx = config.TRANSACTIONS_PER_ACCOUNT

    for acc_id in account_ids:
        n_tx = int(rng.integers(min_tx, max_tx + 1))

        for _ in range(n_tx):
            sec_id     = rng.choice(security_ids)
            tx_type    = rng.choice(config.TRANSACTION_TYPES, p=config.TRANSACTION_TYPE_WEIGHTS)
            trade_date = rng.choice(business_days)
            sec_type   = sec_type_lookup.get(sec_id, "EQUITY")

            # AMBIGUITY #3: settlement_date is trade_date + T+N, N varies by security type
            lag = config.SETTLEMENT_LAG.get(sec_type, 2)
            settlement_date = add_business_days(trade_date, lag)

            # AMBIGUITY #1 (variant): execution price — not the same as month-end market price
            exec_price = get_execution_price(sec_id, trade_date)
            slippage   = float(rng.normal(0, 0.002))
            price      = round(max(0.01, exec_price * (1 + slippage)), 6)

            # Quantity
            if tx_type in ("BUY", "TRANSFER_IN"):
                quantity = round(float(rng.uniform(10, 500)), 4)
            elif tx_type in ("SELL", "TRANSFER_OUT"):
                quantity = round(-float(rng.uniform(10, 500)), 4)
            elif tx_type == "DIVIDEND":
                quantity = 0.0
            else:  # FEE
                quantity = 0.0

            gross_amount = round(abs(quantity) * price, 2) if quantity != 0 else round(float(rng.uniform(10, 200)), 2)
            commission   = round(float(rng.uniform(0, 25)) if tx_type in ("BUY", "SELL") else 0.0, 2)
            fees         = round(float(rng.uniform(0, 10)), 2)
            net_amount   = round(gross_amount - commission - fees, 2)

            # AMBIGUITY #4: realized gain/loss — assigned a random tax lot method per SELL
            realized_gl  = None
            tax_lot_method = None
            if tx_type == "SELL":
                tax_lot_method = rng.choice(config.TAX_LOT_METHODS, p=config.TAX_LOT_METHOD_WEIGHTS)
                # Simulated cost basis — random fraction of current price
                cost_per_share = price * float(rng.uniform(0.70, 1.30))
                realized_gl    = round((price - cost_per_share) * abs(quantity), 2)

            rows.append({
                "transaction_id":    f"TXN-{row_id:08d}",
                "account_id":        acc_id,
                "security_id":       sec_id,
                "trade_date":        trade_date,          # AMBIGUITY #3
                "settlement_date":   settlement_date,     # AMBIGUITY #3
                "transaction_type":  tx_type,
                "quantity":          quantity,
                "price":             price,               # AMBIGUITY #1 variant
                "gross_amount":      gross_amount,
                "commission":        commission,
                "fees":              fees,
                "net_amount":        net_amount,
                "currency":          "USD",
                "realized_gain_loss": realized_gl,        # AMBIGUITY #4
                "tax_lot_method":    tax_lot_method,      # AMBIGUITY #4
                "notes":             None,
            })
            row_id += 1

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# fact_return
# ---------------------------------------------------------------------------

def generate_fact_return(
    dim_account: pd.DataFrame,
    month_end_dates: list[date],
    fact_benchmark_return: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Account-level performance returns for each month-end and return period.

    AMBIGUITY #2 (account-level): Multiple return columns — gross vs net, TWR vs MWR.
    The raw schema exposes all of them with no default; Cortex Analyst must pick one.
    The semantic layer designates return_net / return_twr as the canonical metric.

    AMBIGUITY #5: 'return_annualized' — this column appears on rows with different
    return_period values (1YR, 3YR, 5YR, INCEPTION). Raw Cortex may aggregate across
    periods, mixing 1YR and 3YR annualized figures — which is meaningless.

    AMBIGUITY #6: 'benchmark_return' — stored inline on each row but the JOIN path
    to know WHICH benchmark it came from requires dim_account → dim_benchmark.
    Raw Cortex may compare against fact_benchmark_return directly with an incorrect JOIN.
    """
    # Build a benchmark return lookup: (benchmark_id, as_of_date, return_period) → return_value
    bm_lookup = {}
    for _, row in fact_benchmark_return.iterrows():
        key = (row["benchmark_id"], row["as_of_date"], row["return_period"])
        bm_lookup[key] = row["return_value"]

    # Account → benchmark mapping
    acct_bm = dict(zip(dim_account["account_id"], dim_account["benchmark_id"]))
    acct_type_map = dict(zip(dim_account["account_id"], dim_account["account_type"]))
    acct_inception = dict(zip(dim_account["account_id"], dim_account["inception_date"]))

    rows = []
    row_id = 1

    for _, acct in dim_account.iterrows():
        acc_id    = acct["account_id"]
        bm_id     = acct_bm[acc_id]
        acct_type = acct_type_map[acc_id]
        inception = acct_inception[acc_id]
        fee_drag  = config.FEE_DRAG_MONTHLY[acct_type]

        # Generate monthly gross returns for this account: benchmark return + alpha noise
        monthly_gross = {}
        for i, as_of in enumerate(month_end_dates):
            bm_mtd = bm_lookup.get((bm_id, as_of, "MTD"), 0.0)
            alpha  = float(rng.normal(0.001, 0.015))
            monthly_gross[as_of] = bm_mtd + alpha

        for i, as_of in enumerate(month_end_dates):
            # Only generate returns for dates after account inception
            if as_of < inception:
                continue

            for period in config.RETURN_PERIODS:
                # Determine which months fall in this period
                if period == "MTD":
                    period_months = [as_of]
                elif period == "QTD":
                    q_start_month = ((as_of.month - 1) // 3) * 3 + 1
                    period_months = [
                        d for d in month_end_dates
                        if d.year == as_of.year and q_start_month <= d.month <= as_of.month
                    ]
                elif period == "YTD":
                    period_months = [
                        d for d in month_end_dates
                        if d.year == as_of.year and d.month <= as_of.month
                    ]
                elif period == "1YR":
                    period_months = [
                        d for d in month_end_dates
                        if 0 < (as_of.year * 12 + as_of.month) - (d.year * 12 + d.month) <= 12
                    ]
                    if len(period_months) < 12:
                        continue
                elif period == "3YR":
                    period_months = [
                        d for d in month_end_dates
                        if 0 < (as_of.year * 12 + as_of.month) - (d.year * 12 + d.month) <= 36
                    ]
                    if len(period_months) < 36:
                        continue
                elif period == "5YR":
                    period_months = [
                        d for d in month_end_dates
                        if 0 < (as_of.year * 12 + as_of.month) - (d.year * 12 + d.month) <= 60
                    ]
                    if len(period_months) < 60:
                        continue
                elif period == "INCEPTION":
                    period_months = [
                        d for d in month_end_dates
                        if inception <= d <= as_of
                    ]
                    if len(period_months) < 2:
                        continue
                else:
                    continue

                # Compound gross return over period months
                gross_compound = float(np.prod([1 + monthly_gross.get(d, 0.0) for d in period_months]) - 1)

                # Net: apply fee drag per month
                net_compound = float(
                    np.prod([1 + monthly_gross.get(d, 0.0) - fee_drag for d in period_months]) - 1
                )

                # TWR ≈ net for single-period; MWR = TWR + small cash flow noise
                return_twr = net_compound
                return_mwr = net_compound + float(rng.normal(0, 0.002))

                # AMBIGUITY #5: return_annualized — correct per period, but period varies
                n_months_period = len(period_months)
                return_annualized = annualize_return(net_compound, n_months_period)

                # AMBIGUITY #6: benchmark_return — which benchmark is this?
                # Requires dim_account.benchmark_id to know; raw schema doesn't surface that
                bm_return = bm_lookup.get((bm_id, as_of, period))
                if bm_return is None and period in ("MTD", "QTD", "YTD", "1YR"):
                    bm_return = 0.0  # fallback

                active_return = round(net_compound - bm_return, 8) if bm_return is not None else None

                tracking_error = round(abs(float(rng.normal(0.01, 0.005))), 6)
                info_ratio = round(active_return / tracking_error, 4) if (active_return is not None and tracking_error > 0) else None
                sharpe = round(net_compound / max(tracking_error, 0.001), 4)

                rows.append({
                    "return_id":          f"RET-{row_id:08d}",
                    "account_id":         acc_id,
                    "as_of_date":         as_of,
                    "return_period":      period,
                    "return_gross":       round(gross_compound, 8),   # AMBIGUITY #2
                    "return_net":         round(net_compound, 8),      # AMBIGUITY #2
                    "return_annualized":  round(return_annualized, 8), # AMBIGUITY #5
                    "return_twr":         round(return_twr, 8),        # AMBIGUITY #2
                    "return_mwr":         round(return_mwr, 8),        # AMBIGUITY #2
                    "benchmark_return":   round(bm_return, 8) if bm_return is not None else None,  # AMBIGUITY #6
                    "active_return":      active_return,               # AMBIGUITY #6
                    "tracking_error":     tracking_error,
                    "information_ratio":  info_ratio,
                    "sharpe_ratio":       sharpe,
                    "currency":           "USD",
                })
                row_id += 1

    return pd.DataFrame(rows)
