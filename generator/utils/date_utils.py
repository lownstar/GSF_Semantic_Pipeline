"""
Date sequence helpers for the seed data generator.
Used throughout facts.py for position snapshots, settlement dates, and period returns.
"""

import pandas as pd
from datetime import date, timedelta
from typing import List


def get_all_dates(start: str, end: str) -> pd.DatetimeIndex:
    return pd.date_range(start=start, end=end, freq="D")


def get_business_days(start: str, end: str) -> List[date]:
    idx = pd.date_range(start=start, end=end, freq="B")
    return [d.date() for d in idx]


def get_month_end_dates(start: str, end: str) -> List[date]:
    idx = pd.date_range(start=start, end=end, freq="ME")
    return [d.date() for d in idx]


def get_quarter_end_dates(start: str, end: str) -> List[date]:
    idx = pd.date_range(start=start, end=end, freq="QE")
    return [d.date() for d in idx]


def get_year_end_dates(start: str, end: str) -> List[date]:
    idx = pd.date_range(start=start, end=end, freq="YE")
    return [d.date() for d in idx]


def add_business_days(d: date, n: int) -> date:
    """Add n business days to a date (skips weekends; no holiday calendar)."""
    if n == 0:
        return d
    current = d
    added = 0
    while added < n:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Monday–Friday
            added += 1
    return current


def is_month_end(d: date) -> bool:
    return (d + timedelta(days=1)).month != d.month


def is_quarter_end(d: date) -> bool:
    return is_month_end(d) and d.month in (3, 6, 9, 12)


def is_year_end(d: date) -> bool:
    return d.month == 12 and d.day == 31


def period_start(as_of: date, period: str, inception_date: date | None = None) -> date | None:
    """Return the start date for a given return period relative to as_of date."""
    if period == "MTD":
        return as_of.replace(day=1)
    elif period == "QTD":
        q_start_month = ((as_of.month - 1) // 3) * 3 + 1
        return as_of.replace(month=q_start_month, day=1)
    elif period == "YTD":
        return as_of.replace(month=1, day=1)
    elif period == "1YR":
        return date(as_of.year - 1, as_of.month, as_of.day)
    elif period == "3YR":
        return date(as_of.year - 3, as_of.month, as_of.day)
    elif period == "5YR":
        return date(as_of.year - 5, as_of.month, as_of.day)
    elif period == "INCEPTION":
        return inception_date
    return None


def months_between(start: date, end: date) -> float:
    """Approximate number of months between two dates."""
    return (end.year - start.year) * 12 + (end.month - start.month) + (end.day - start.day) / 30.0


def annualize_return(cumulative_return: float, months: float) -> float:
    """Convert a cumulative return to annualized, given holding period in months."""
    if months <= 0:
        return 0.0
    years = months / 12.0
    if years < 0.0833:  # less than ~1 month — just return simple
        return cumulative_return
    return (1 + cumulative_return) ** (1 / years) - 1
