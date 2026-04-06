# Smoke Test Run Report
**Date:** 2026-04-06  
**Environment:** Windows 11, Python 3.14.0 (system), Python 3.13.x (also installed)  
**Branch:** main  
**Run command:** `python run_pipeline.py` (phases 1 3 4 5 6, then phases 3-6 iteratively as issues were resolved)

---

## Executive Summary

This was the first end-to-end run of the pipeline following all six refactoring steps. The
pipeline was not runnable out of the box — seven distinct issues blocked execution. All were
diagnosed and fixed during the session. The pipeline now runs end-to-end and produces variance
output for all four tiers (Bronze, Silver, Gold Naive, Gold Semantic). One open question
remains in the variance scoring logic (see Finding 8).

**Final outcome:** All phases completed successfully. dbt 68/68 tests pass.

---

## Issues Found and Fixed

### Finding 1 — Dependencies not installed (Phase 3 abort)

**Symptom:** `ModuleNotFoundError: No module named 'snowflake'` on first run.

**Root cause:** `requirements.txt` exists but was never installed in the active Python
environment. The project has no virtual environment setup and no first-run instructions that
include the install step.

**Fix applied:** `pip install -r requirements.txt`

**Doc suggestion:** Add an explicit "Prerequisites" or "First-time setup" section to
`docs/runbook.md` with:
```bash
pip install -r requirements.txt
```
Note that `dbt-snowflake` and `snowflake-connector-python` are the largest dependencies
and take ~2 minutes to install fresh.

---

### Finding 2 — Bronze tables double on re-run (Phase 3 fail)

**Symptom:** After a prior run existed, the row count verification step reported exactly
2× expected rows:

```
FAIL  BRONZE.TOPAZ_POSITIONS: 24,776 rows (expected 12,388)
FAIL  BRONZE.EMERALD_POSITIONS: 9,772 rows (expected 4,886)
```

**Root cause:** Snowflake's `COPY INTO` command tracks load history and will re-load a
file if it has been re-PUT with `OVERWRITE=TRUE`. With no `TRUNCATE` before the COPY, each
re-run appends data to existing tables.

**Fix applied:** Added `cur.execute(f"TRUNCATE TABLE {table}")` before each load in
`pipeline_naive/load_bronze.py`.

**Doc suggestion:** Add a note to `docs/runbook.md` that Phase 3 is idempotent (truncate
+ reload) and safe to re-run. Also consider adding `FORCE = TRUE` to the COPY statement as
belt-and-suspenders, or noting that `PURGE = TRUE` only removes staged files, not table data.

---

### Finding 3 — Unicode crash on Windows terminal (Phase 4 abort)

**Symptom:** `UnicodeEncodeError: 'charmap' codec can't encode character '\u2192'` when
the Phase 4 banner printed.

**Root cause:** `PHASE_LABELS[4]` contained `→` (U+2192). Windows terminals default to
CP1252, which does not include that character.

**Fix applied:** Replaced `→` with `->` in both `PHASE_LABELS` and the docstring in
`run_pipeline.py`.

**Doc suggestion:** Add a note to `docs/runbook.md` that on Windows, the terminal encoding
must be UTF-8 if any unicode characters are used in output — or avoid non-ASCII characters
in print statements. Could also set `PYTHONUTF8=1` in `.env` as a systemic fix.

---

### Finding 4 — dbt incompatible with Python 3.14 (Phase 4 abort)

**Symptom:** `mashumaro.exceptions.UnserializableField: Field "schema" of type Optional[str]
in JSONObjectSchema is not serializable` — dbt crashes at import before running any SQL.

**Root cause:** `dbt-core 1.11` depends on `mashumaro`, which has a known incompatibility
with Python 3.14. Python 3.14 was released after dbt-snowflake 1.11's dependency tree was
locked.

**Fix applied:** Updated `phase_4()` in `run_pipeline.py` to invoke dbt via
`py -3.13 -m dbt.cli.main` (Python 3.13 was installed alongside 3.14 on this machine).

**Doc suggestion:**
- Add a **Python version requirement** to `docs/runbook.md`: dbt-snowflake requires
  Python ≤ 3.13 as of April 2026.
- Add to `requirements.txt` a comment noting the Python version constraint.
- Consider adding a pre-flight check in `run_pipeline.py` that warns if `sys.version_info >= (3, 14)`.

---

### Finding 5 — `.env` variables not inherited by dbt subprocess (Phase 4 abort)

**Symptom:** After fixing Finding 4, dbt started but immediately aborted with:
`Env var required but not provided: 'SNOWFLAKE_ACCOUNT'`

**Root cause:** `run_pipeline.py` never called `load_dotenv()`. Each individual script
(`load_bronze.py`, `load_gold.py`, etc.) calls `load_dotenv()` for itself, but that only
sets vars in its own process. The `dbt` subprocess spawned by `run_pipeline.py` inherited
a clean environment with no Snowflake credentials.

**Fix applied:** Added `from dotenv import load_dotenv` and
`load_dotenv(PROJECT_ROOT / ".env")` near the top of `run_pipeline.py`, so all env vars
are set before any subprocess is spawned.

**Doc suggestion:** Document in `docs/runbook.md` that the orchestrator loads `.env`
automatically. No manual `export` steps required.

---

### Finding 6 — RSA private key: wrong path and wrong format for dbt (Phase 4 abort)

Two sub-issues surfaced in sequence:

**6a — Relative key path resolved against dbt/ instead of project root**

`SNOWFLAKE_PRIVATE_KEY_FILE` was set to `snowflake_rsa_key.p8` (relative). Because dbt
runs with `cwd=dbt/`, it looked for the key in `dbt/snowflake_rsa_key.p8` — which doesn't
exist.

**6b — DER format key rejected by dbt**

The key file (`snowflake_rsa_key.p8`) is in DER (binary) format. `load_bronze.py` uses
`load_der_private_key()` which handles this correctly. But dbt's `private_key_path` profile
field expects a PEM file (the `-----BEGIN PRIVATE KEY-----` ASCII format). Passing DER
produces: `Unable to load PEM file. MalformedFraming`.

**Fix applied:**
1. Converted the DER key to PEM once: `snowflake_rsa_key.pem` (generated via
   `cryptography` library, not committed — add to `.gitignore`).
2. Updated `dbt/profiles.yml` to use `SNOWFLAKE_PRIVATE_KEY_PEM` env var instead of
   `SNOWFLAKE_PRIVATE_KEY_FILE`.
3. In `phase_4()`, set `SNOWFLAKE_PRIVATE_KEY_PEM` to the absolute path of the PEM file
   before spawning dbt.

**Doc suggestion:**
- `docs/runbook.md` should document the key format requirements clearly:
  - `load_bronze.py` and other Python scripts: use DER key (`snowflake_rsa_key.p8`)
  - dbt: requires PEM key (`snowflake_rsa_key.pem`) — run the one-time conversion if missing:
    ```python
    from cryptography.hazmat.primitives.serialization import (
        load_der_private_key, Encoding, PrivateFormat, NoEncryption)
    with open('snowflake_rsa_key.p8', 'rb') as f:
        key = load_der_private_key(f.read(), password=None)
    with open('snowflake_rsa_key.pem', 'wb') as f:
        f.write(key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()))
    ```
- Add `snowflake_rsa_key.pem` to `.gitignore` (alongside the existing `.p8` entry, if any).
- Consider unifying on PEM everywhere (convert load_bronze.py to use PEM) to eliminate
  the two-format problem.

---

### Finding 7 — dbt schema test missing two asset classes (dbt test fail)

**Symptom:** `dbt test` failed on `accepted_values_dw_security_asset_class` — 2 rows with
unexpected values (`Alternatives`, `Commodities`).

**Root cause:** `dbt/models/gold_semantic/schema.yml` listed only four accepted asset
classes (`Equity`, `Fixed Income`, `Real Estate`, `Cash`), but `generator_v2/config.py`
defines six:

```python
ASSET_CLASSES = ["Equity", "Fixed Income", "Real Estate", "Cash", "Alternatives", "Commodities"]
```

The schema test had never been run against real data — the mismatch was silently present
from the original authoring.

**Fix applied:** Added `Alternatives` and `Commodities` to the `accepted_values` list in
`dbt/models/gold_semantic/schema.yml`. Updated the column description to remove the
hard-coded list.

**Doc suggestion:** When updating `generator_v2/config.py` (e.g., adding or removing asset
classes), the corresponding `accepted_values` test in the dbt schema must be updated too.
Consider generating the accepted values list from config rather than hard-coding it in two
places — or add a note in `generator_v2/config.py` pointing to the schema file as a
dependency.

---

## Open Question — Variance Scoring: Gold 0/11 Correct

**Observation:** The variance runner reports Gold at 0/11 correct (0%). Visual inspection
of the results JSON shows that Gold is successfully generating SQL and returning data for
all 11 questions, but the values are consistently off by approximately 3×:

| Question | Ground Truth | Gold Returned | Ratio |
|---|---|---|---|
| Q01 — Total MV ACC-0042 | $47,944,909 | $143,811,774 | ~3× |
| Q02 — Total qty RLP | 156,813 | 470,439 | 3× |
| Q03 — Total MV all positions | $6,107,574,036 | $18,322,597,789 | ~3× |

**Likely cause:** `DW_POSITION` in Snowflake holds 14,658 rows — one row per
(account × security × source_system). Ground truth is computed from `data/seed_v2/dw_position.csv`,
which has 4,886 rows at canonical DW grain (de-duplicated). Summing all 14,658 rows
produces exactly 3× the canonical total.

**This may be intentional** — the demo narrative includes the point that even a Gold layer
can return wrong answers without the right semantic guardrails. If intentional, the variance
runner should document why Gold scores 0% and what additional semantic constraint would fix
it (e.g., filtering to a canonical source_system flag or using a de-duplicated view).

**If not intentional**, the ground truth calculation or the `DW_POSITION` model grain needs
reconciliation.

**Action needed:** Clarify design intent and either update the ground truth calculation or
add documentation explaining that Gold's 0% score is a feature of the demo, not a bug.

---

## Summary of Changes Made

| File | Change |
|---|---|
| `pipeline_naive/load_bronze.py` | Added `TRUNCATE TABLE` before each COPY INTO |
| `run_pipeline.py` | Added `load_dotenv()`; replaced `→` with `->`; dbt now runs via `py -3.13 -m dbt.cli.main`; `SNOWFLAKE_PRIVATE_KEY_PEM` passed as absolute path to dbt subprocess |
| `dbt/profiles.yml` | Changed `private_key_path` to use `SNOWFLAKE_PRIVATE_KEY_PEM` env var |
| `dbt/models/gold_semantic/schema.yml` | Added `Alternatives` and `Commodities` to `asset_class` accepted values |
| `snowflake_rsa_key.pem` | New file (generated, not committed) — PEM version of existing DER key |

---

## Recommended Documentation Updates

Priority order for updating `docs/runbook.md` and `docs/decisions.md`:

1. **First-time setup**: `pip install -r requirements.txt` before any pipeline step.
2. **Python version constraint**: dbt requires Python ≤ 3.13 (mashumaro/Python 3.14 incompatibility as of April 2026).
3. **RSA key format**: Document both `.p8` (DER, used by Python scripts) and `.pem` (PEM, needed for dbt) — and the one-time conversion command.
4. **Variance scoring caveat**: Explain the 0% Gold accuracy result and whether it is intentional.
5. **Re-run safety**: Confirm Phase 3 is idempotent (truncate + reload).
6. **Asset class registry**: Note that `generator_v2/config.py:ASSET_CLASSES` must stay in sync with `dbt/models/gold_semantic/schema.yml` accepted values.
