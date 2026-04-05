"""
Scoring logic for Silver vs Gold Cortex Analyst results.

Each (question, model) pair receives a status code:
  CORRECT   — result is within tolerance of ground truth
  WRONG     — result returned but value diverges from ground truth
  NO_DATA   — Cortex generated SQL that returned zero rows
  ERROR     — API exception, invalid SQL, or could not extract a numeric value

Tolerances:
  scalar     — relative tolerance of 0.01% (catches Silver errors, not float noise)
  row_count  — exact match required (integer count)
  percentage — absolute tolerance of 0.1 percentage points
"""

import decimal
from typing import Literal

from variance.questions import Question

StatusCode = Literal["CORRECT", "WRONG", "NO_DATA", "ERROR"]

# Relative tolerance for scalar/currency/quantity answers (0.01%)
_SCALAR_TOLERANCE = 0.0001
# Absolute tolerance for percentage answers (pp)
_PCT_TOLERANCE = 0.1


def extract_value(rows: list[dict], result_type: str) -> float | None:
    """Extract the relevant numeric value from Cortex result rows.

    For "row_count": returns the number of rows.
    For "scalar"/"percentage": returns the first numeric column of the first row.
    Returns None if extraction fails.
    """
    if result_type == "row_count":
        return float(len(rows))

    if not rows:
        return None

    first_row = rows[0]
    for val in first_row.values():
        if val is None:
            continue
        if isinstance(val, (int, float, decimal.Decimal)):
            return float(val)
        if isinstance(val, str):
            cleaned = val.replace(",", "").replace("$", "").replace("%", "").strip()
            try:
                return float(cleaned)
            except ValueError:
                pass
    return None


def score(
    rows: list[dict],
    ground_truth: float,
    result_type: str,
) -> tuple[StatusCode, float | None, float | None]:
    """Score a single (question, model) result.

    Returns (status_code, variance, value) where variance is:
      - (result - ground_truth) / |ground_truth| × 100  for scalars
      - (result - ground_truth) in percentage points     for percentages
      - (result - ground_truth) as integer delta         for row_counts
      - None when status is NO_DATA or ERROR
    """
    if not rows:
        return "NO_DATA", None, None

    value = extract_value(rows, result_type)
    if value is None:
        return "ERROR", None, None

    if result_type == "percentage":
        # Cortex may return a fraction (0.1647) when ground truth is stored as % (16.47)
        if ground_truth > 1.0 and value < 1.0:
            value = value * 100.0
        diff_pp = value - ground_truth
        status = "CORRECT" if abs(diff_pp) <= _PCT_TOLERANCE else "WRONG"
        return status, round(diff_pp, 4), value

    if result_type == "row_count":
        diff = value - ground_truth
        status = "CORRECT" if diff == 0 else "WRONG"
        return status, round(diff, 0), value

    # scalar
    if ground_truth == 0:
        status = "CORRECT" if value == 0 else "WRONG"
        return status, 0.0 if value == 0 else None, value

    rel = (value - ground_truth) / abs(ground_truth)
    variance_pct = round(rel * 100.0, 4)
    status = "CORRECT" if abs(rel) <= _SCALAR_TOLERANCE else "WRONG"
    return status, variance_pct, value


def score_run(questions, ground_truths: dict[str, float], model_results: dict) -> dict:
    """Score all questions for one model's results.

    model_results: {question_id: {"rows": [...], "sql": "...", "error": None|str}}
    Returns: {question_id: {"status": ..., "variance": ..., "value": ...}}
    """
    scored = {}
    for q in questions:
        qr = model_results.get(q.id, {})
        error = qr.get("error")
        rows = qr.get("rows", [])
        gt = ground_truths[q.id]

        if error:
            scored[q.id] = {"status": "ERROR", "variance": None, "value": None}
            continue

        status, variance, value = score(rows, gt, q.result_type)
        scored[q.id] = {"status": status, "variance": variance, "value": value}

    return scored
