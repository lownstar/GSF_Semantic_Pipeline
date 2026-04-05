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
