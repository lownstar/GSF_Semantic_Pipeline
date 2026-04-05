# Design Decisions

Architectural and technical decisions made during the project, with rationale.

---

## Snowflake Semantic Model: YAML via Stage (not Semantic Views)

**Decision:** Use Cortex Analyst YAML staged to an internal Snowflake stage, not
Native Semantic Views (CREATE SEMANTIC VIEW DDL).

**Why:** The YAML file is a visible, reviewable, portable artifact. It lives in the
repo, can be diffed, and can be shown to a hiring manager. Stage-based approach keeps
the pipeline transparent.

**Future:** The same YAML could drive a CREATE SEMANTIC VIEW DDL generator for deeper
Horizon lineage integration.

---

## YAML Authoring: Use Snowflake Generator, Not Hand-Author

**Decision:** Generate the base YAML via Snowflake's Semantic Model Generator
(Snowsight -> AI & ML -> Semantic Model -> Create), then enrich manually.

**Why:** Hand-authoring against Snowflake's undocumented protobuf spec is fragile.
Lessons learned (2026-03-28 through 2026-03-30):
- `primary_entity` is NOT valid -> must be `primary_key`
- `primary_key` requires `columns:` nested field, not a bare list
- `measures:` is NOT valid -> must be `facts:` for numeric columns
- `verified_queries` SQL must use fully qualified table names (`GSF_DEMO.GOLD.*`)

---

## Authentication: Key-Pair (not Password)

**Decision:** All Python scripts use RSA key-pair auth, not password.

**Why:** The Snowflake account uses Duo MFA. Password auth triggers an MFA push on
every script run and may time out. Key-pair auth is the only reliable programmatic approach.

**Implementation:** DER-encoded `.p8` file, loaded via `cryptography` library.
Session token extracted from `conn._rest._token` for Cortex Analyst REST API calls.

---

## Single Database, Three Schemas (not Separate Accounts)

**Decision:** Single `GSF_DEMO` database with `BRONZE`, `SILVER`, `GOLD` schemas.

**Why:** One `USE DATABASE` statement, clean IaC, single-account billing. The medallion
layers map to schemas, not databases or accounts.

---

## Cortex Analyst: Both Environments Need a YAML

**Decision:** Silver queries also use a YAML (`positions_silver.yaml`), not raw SQL.

**Why:** Cortex Analyst requires a semantic model file for every query -- there is no
"no model" mode. The Silver YAML is intentionally thin: it exposes POSITIONS_INTEGRATED
as-is with minimal metadata, no disambiguation. This makes Cortex generate confident
but wrong SQL.

---

## Pipeline Naming: Naive / Semantic Enriched (not A / B)

**Decision:** Renamed from "Pipeline A" / "Pipeline B" to "Naive Pipeline" /
"Semantic Enriched Pipeline" during the portfolio-grade refactoring (2026-04-04).

**Why:** "A" and "B" communicate nothing about what makes them different. The pipeline
names should tell the demo story: one lacks semantic governance, one has it.

---

## Deterministic Generator (seed=42)

**Decision:** All generated data uses a fixed random seed. Every CSV is reproducible.

**Why:** Ground truth must be knowable and unchanging. The variance comparison in Epic 5
compares Cortex results against pandas computations over the same seed CSVs. If the data
changes, the ground truth changes, and the variance scores become meaningless.

---

## Ground Truth from Seed CSVs (not Snowflake)

**Decision:** Ground truth is computed from local `data/seed_v2/` CSVs via pandas,
never from Snowflake queries.

**Why:** Ensures objectivity. If Snowflake has a bug or the ETL changes, ground truth
remains stable. The comparison is always: "what did Cortex return vs what the data
actually says."

---

## dbt as the Transformation Layer

**Decision:** Use dbt for all Bronze → Silver → Gold transforms, replacing the hand-rolled
`etl_silver.sql` and `setup_gold.sql` scripts.

**Why (portfolio rationale):** Most enterprises that have a Gold layer already built it with
dbt. This demo can then argue: "you have dbt Gold, yet Cortex still gets it wrong — because
the semantic model is what you're missing." dbt also provides built-in schema tests (replacing
`validate_silver.py` and `validate_gold.py`), a generated DAG for documentation, and a
standard pattern that any data engineer will recognize.

**What dbt owns:** Bronze → Silver transform, Silver → GOLD_NAIVE, Silver → GOLD transforms,
schema tests. **What Python still owns:** PUT/COPY file operations to Bronze (dbt cannot do
this), YAML staging to Snowflake stage, Cortex REST API calls, variance scoring.

---

## Four-Tier Comparison (Bronze / Silver / Naive Gold / Semantic Gold)

**Decision:** Extend the comparison from two tiers (Silver vs Gold) to four, adding Bronze
and Naive Gold.

**Why:** The original two-tier comparison conflated two variables: data quality (Silver had
known gaps) and semantic model richness (Gold had the governed YAML). A hiring manager could
reasonably conclude "better data = better AI" without crediting the semantic model. The four-tier
design isolates the variable:

- Bronze shows Cortex struggling with raw fragmented schemas
- Silver shows improvement from integration, but ambiguities embedded
- Naive Gold uses a proper dbt star schema — and still gets wrong answers
- Semantic Gold resolves all 11 ambiguities — correct answers

Naive Gold is the key tier: it proves that a structurally clean Gold layer is insufficient
without semantic governance.

---

## Semantic Model YAML Naming: `positions_{layer}.yaml`

**Decision:** Use `positions_{layer}.yaml` naming across all four tiers:
`positions_bronze.yaml`, `positions_silver.yaml`, `positions_gold_naive.yaml`, `positions_gold.yaml`.

**Why:** The `positions.yaml` / `positions_naive.yaml` original names broke the convention and
required reading file content to know which tier a YAML targeted. The layer-suffixed names are
self-documenting in a directory listing — important when hiring managers browse the repo.

---

## Surrogate Key: Native MD5 (not dbt_utils)

**Decision:** Use `MD5(CONCAT(...))` in `dw_position.sql` instead of `dbt_utils.generate_surrogate_key`.

**Why:** Avoids adding the `dbt-utils` package as a dependency for a single function call.
`MD5(CONCAT(account_id, '|', security_id, '|', position_date::VARCHAR, '|', source_system))`
is equivalent for this use case and keeps the project dependency-light.
