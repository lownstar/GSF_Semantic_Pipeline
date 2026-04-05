"""
Dimension table generators.
Build order: dim_date → dim_asset_class → dim_benchmark → dim_security → dim_account
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from faker import Faker

from generator import config
from generator.utils.date_utils import (
    get_all_dates, get_month_end_dates, get_quarter_end_dates,
    get_year_end_dates, is_month_end, is_quarter_end, is_year_end,
)


def generate_dim_date(start: str = config.DATE_START, end: str = config.DATE_END) -> pd.DataFrame:
    all_dates = get_all_dates(start, end)
    month_ends = set(get_month_end_dates(start, end))
    quarter_ends = set(get_quarter_end_dates(start, end))
    year_ends = set(get_year_end_dates(start, end))

    rows = []
    for dt in all_dates:
        d = dt.date()
        rows.append({
            "date_id":          d,
            "calendar_year":    d.year,
            "calendar_quarter": (d.month - 1) // 3 + 1,
            "calendar_month":   d.month,
            "month_name":       dt.strftime("%B"),
            "week_of_year":     int(dt.strftime("%W")),
            "day_of_week":      d.weekday(),   # 0=Monday, 6=Sunday
            "is_weekday":       d.weekday() < 5,
            "is_month_end":     d in month_ends,
            "is_quarter_end":   d in quarter_ends,
            "is_year_end":      d in year_ends,
            "fiscal_year":      d.year,
            "fiscal_quarter":   (d.month - 1) // 3 + 1,
        })
    return pd.DataFrame(rows)


def generate_dim_asset_class() -> pd.DataFrame:
    rows = []
    for i, (code, name, is_liquid) in enumerate(config.ASSET_CLASSES, start=1):
        rows.append({
            "asset_class_id":   f"AC-{i:02d}",
            "asset_class_name": name,
            "asset_class_code": code,
            "sort_order":       i,
            "is_liquid":        is_liquid,
        })
    return pd.DataFrame(rows)


def generate_dim_benchmark() -> pd.DataFrame:
    rows = []
    for i, (code, name, btype) in enumerate(config.BENCHMARKS, start=1):
        rows.append({
            "benchmark_id":   f"BM-{i:02d}",
            "benchmark_name": name,
            "benchmark_code": code,
            "benchmark_type": btype,
            "description":    f"Standard {btype.replace('_', ' ').title()} benchmark: {name}",
        })
    return pd.DataFrame(rows)


def generate_dim_security(
    rng: np.random.Generator,
    faker: Faker,
    dim_asset_class: pd.DataFrame,
    dim_benchmark: pd.DataFrame,
    n: int = config.ROW_TARGETS["dim_security"],
) -> pd.DataFrame:
    asset_class_codes = list(config.ASSET_CLASS_WEIGHTS.keys())
    asset_class_probs = list(config.ASSET_CLASS_WEIGHTS.values())

    # Build lookup: code → id
    ac_code_to_id = dict(zip(dim_asset_class["asset_class_code"], dim_asset_class["asset_class_id"]))

    # Assign an equity-type benchmark to each asset class
    equity_bm = dim_benchmark[dim_benchmark["benchmark_type"] == "EQUITY_INDEX"]["benchmark_id"].tolist()
    bond_bm   = dim_benchmark[dim_benchmark["benchmark_type"] == "BOND_INDEX"]["benchmark_id"].tolist()
    cash_bm   = dim_benchmark[dim_benchmark["benchmark_type"] == "CASH"]["benchmark_id"].tolist()
    blend_bm  = dim_benchmark[dim_benchmark["benchmark_type"] == "BLENDED"]["benchmark_id"].tolist()
    all_bm    = dim_benchmark["benchmark_id"].tolist()

    def pick_benchmark_for_ac(code: str) -> str:
        if code in ("EQ", "RE", "CMDTY", "HF", "PE", "ALT"):
            pool = equity_bm or all_bm
        elif code == "FI":
            pool = bond_bm or all_bm
        elif code == "CASH":
            pool = cash_bm or blend_bm or all_bm
        else:
            pool = all_bm
        return rng.choice(pool)

    rows = []
    used_tickers = set()
    for i in range(1, n + 1):
        ac_code = rng.choice(asset_class_codes, p=asset_class_probs)
        ac_id   = ac_code_to_id[ac_code]
        sec_types = config.ASSET_CLASS_SECURITY_TYPES[ac_code]
        sec_type  = rng.choice(sec_types)
        sector    = rng.choice(config.SECTORS) if ac_code in ("EQ", "FI") else ac_code

        # Generate a unique fake ticker (3-5 uppercase letters)
        for _ in range(20):
            length = int(rng.integers(3, 6))
            ticker = "".join(rng.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), size=length))
            if ticker not in used_tickers:
                used_tickers.add(ticker)
                break

        company = faker.company().split(" ")[0].upper()
        suffix  = {"EQUITY": "Inc", "BOND": "Notes 2028", "ETF": "ETF", "MUTUAL_FUND": "Fund", "CASH_EQUIV": "MM"}
        sec_name = f"{company} {suffix.get(sec_type, 'Securities')}"

        # Fake CUSIP (9 alphanumeric chars) and ISIN
        cusip_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        cusip = "".join(rng.choice(list(cusip_chars), size=9))
        isin  = f"US{cusip}0"   # simplified fake ISIN

        rows.append({
            "security_id":    f"SEC-{i:05d}",
            "ticker":         ticker,
            "security_name":  sec_name,
            "asset_class_id": ac_id,
            "security_type":  sec_type,
            "sector":         sector,
            "country":        "US" if rng.random() < 0.80 else rng.choice(["GB", "DE", "JP", "CA", "FR"]),
            "currency":       "USD",
            "cusip":          cusip,
            "isin":           isin,
            "benchmark_id":   pick_benchmark_for_ac(ac_code),
            "is_active":      bool(rng.random() < 0.92),
        })
    return pd.DataFrame(rows)


def generate_dim_account(
    rng: np.random.Generator,
    faker: Faker,
    dim_benchmark: pd.DataFrame,
    n: int = config.ROW_TARGETS["dim_account"],
) -> pd.DataFrame:
    benchmark_ids = dim_benchmark["benchmark_id"].tolist()
    inception_start = date(2010, 1, 1)
    inception_end   = date(2020, 1, 1)
    inception_range = (inception_end - inception_start).days

    rows = []
    for i in range(1, n + 1):
        acct_type = rng.choice(config.ACCOUNT_TYPES, p=config.ACCOUNT_TYPE_WEIGHTS)

        # Target allocation — three buckets summing to 100
        eq_pct  = round(float(rng.uniform(20, 80)), 2)
        fi_pct  = round(float(rng.uniform(0, min(70, 100 - eq_pct))), 2)
        alt_pct = round(100.0 - eq_pct - fi_pct, 2)

        inception = inception_start + timedelta(days=int(rng.integers(0, inception_range)))

        suffix_map = {
            "INDIVIDUAL":    ["Family Office", "Personal Portfolio", "Investment Account"],
            "INSTITUTIONAL": ["Pension Fund", "Endowment Fund", "Foundation"],
            "TRUST":         ["Family Trust", "Charitable Trust", "Revocable Trust"],
            "ENDOWMENT":     ["University Endowment", "Community Foundation", "Endowment Fund"],
        }
        name_suffix = rng.choice(suffix_map[acct_type])
        acct_name   = f"{faker.last_name()} {name_suffix}"

        rows.append({
            "account_id":         f"ACC-{i:04d}",
            "account_name":       acct_name,
            "account_type":       acct_type,
            "inception_date":     inception,
            "base_currency":      "USD",
            "benchmark_id":       rng.choice(benchmark_ids),
            "target_equity_pct":  eq_pct,
            "target_fixed_pct":   fi_pct,
            "target_alt_pct":     alt_pct,
            "is_active":          bool(rng.random() < 0.90),
            "custodian":          faker.company(),
        })
    return pd.DataFrame(rows)
