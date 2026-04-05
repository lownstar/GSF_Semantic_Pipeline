-- =============================================================================
-- GSF_DEMO — Cortex AI + Horizon Governance Setup
-- =============================================================================
-- One-time account-level setup for Cortex Analyst and Horizon Catalog features.
-- Requires ACCOUNTADMIN. Safe to re-run (idempotent).
--
-- Run AFTER setup.sql (GSF_ROLE and GSF_DEMO database must already exist).
--
-- Sections:
--   1. Cortex Analyst — enable on account, grant to GSF_ROLE
--   2. Horizon tags    — FINANCIAL_SENSITIVITY and PII_CLASSIFICATION
--   3. Semantic View   — grant CREATE SEMANTIC VIEW to GSF_ROLE (Epic 3)
--   4. Verification    — confirm everything is in place before running pipelines
--
-- Account profile this script was written for:
--   Edition: Business Critical (full Horizon support)
--   Region:  AWS US East 1 (Virginia) — native Cortex Analyst
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- SECTION 1: Cortex Analyst
-- =============================================================================

-- Check current state first — will show if already enabled
SHOW PARAMETERS LIKE 'ENABLE_CORTEX_ANALYST' IN ACCOUNT;

-- Enable Cortex Analyst (safe to run if already enabled — no-op)
-- Required for: Cortex Analyst REST API, semantic model queries
ALTER ACCOUNT SET ENABLE_CORTEX_ANALYST = TRUE;

-- Grant the CORTEX_USER database role to GSF_ROLE.
-- CORTEX_USER covers:
--   - All Cortex AI SQL functions (COMPLETE, SUMMARIZE, SENTIMENT, etc.)
--   - Cortex Analyst (natural language → SQL)
--   - Cortex Search
-- NOTE: Cortex functions are granted to PUBLIC by default on most accounts.
-- This explicit grant ensures access if your org has restricted PUBLIC.
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE GSF_ROLE;

-- Also grant CORTEX_ANALYST_USER (scoped to Analyst only — belt-and-suspenders)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER TO ROLE GSF_ROLE;


-- =============================================================================
-- SECTION 2: Horizon Governance Tags
-- =============================================================================
-- Tags are created in GOLD schema — they only govern GOLD layer objects.
-- BRONZE and SILVER are intentionally ungoverned (that's the demo point).
--
-- Note: GOLD schema was created by setup.sql. Tags require it to exist first.

USE DATABASE GSF_DEMO;

-- ── Tag 1: Financial Sensitivity ──────────────────────────────────────────────
-- Applied to columns in GOLD that contain sensitive financial data:
--   CONFIDENTIAL → market_value, unrealized_gain_loss, market_price
--   RESTRICTED   → cost_basis (acquisition cost is commercially sensitive)
--   INTERNAL     → quantity, position_date
--   PUBLIC       → security identifiers (CUSIP, ISIN, ticker), currency

CREATE TAG IF NOT EXISTS GSF_DEMO.GOLD.FINANCIAL_SENSITIVITY
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Financial data sensitivity — used in Horizon governance demo to tag GOLD layer columns';

-- ── Tag 2: PII Classification ─────────────────────────────────────────────────
-- Simulates PII tagging on account identifiers in the governed layer.
-- Demonstrates that Horizon can enforce different data handling rules per column.
--   DIRECT_IDENTIFIER → account_id, account_name (directly identifies a client)
--   QUASI_IDENTIFIER  → custodian_account_num, portfolio_code, fund_code
--   NONE              → financial metrics, security identifiers

CREATE TAG IF NOT EXISTS GSF_DEMO.GOLD.PII_CLASSIFICATION
    ALLOWED_VALUES 'NONE', 'QUASI_IDENTIFIER', 'DIRECT_IDENTIFIER'
    COMMENT = 'PII classification simulation — tags account identifiers in GOLD layer for Horizon governance demo';

-- ── Grant tag application privileges to GSF_ROLE ─────────────────────────────
-- Required so that pipeline_b/apply_horizon_tags.sql can SET TAG on columns
-- using GSF_ROLE (rather than requiring ACCOUNTADMIN for every tag operation).

GRANT APPLY ON TAG GSF_DEMO.GOLD.FINANCIAL_SENSITIVITY TO ROLE GSF_ROLE;
GRANT APPLY ON TAG GSF_DEMO.GOLD.PII_CLASSIFICATION     TO ROLE GSF_ROLE;


-- =============================================================================
-- SECTION 3: Semantic View Privileges (Epic 3 — Pipeline B)
-- =============================================================================
-- Native Semantic Views (CREATE SEMANTIC VIEW) are Snowflake's recommended
-- approach for Cortex Analyst semantic models as of 2025. The view is a
-- schema-level object in GOLD with full RBAC.

GRANT CREATE SEMANTIC VIEW ON SCHEMA GSF_DEMO.GOLD TO ROLE GSF_ROLE;

-- Also grant SELECT on semantic views to GSF_ROLE (needed to query via Cortex Analyst)
-- This is a future grant so it covers the view once created by pipeline_b.
GRANT SELECT ON FUTURE SEMANTIC VIEWS IN SCHEMA GSF_DEMO.GOLD TO ROLE GSF_ROLE;


-- =============================================================================
-- SECTION 4: Verification
-- =============================================================================
-- Run these after the grants above to confirm everything is in place.
-- Expected results annotated in comments.

-- 4a. Cortex Analyst must be TRUE
SHOW PARAMETERS LIKE 'ENABLE_CORTEX_ANALYST' IN ACCOUNT;
-- Expected: value = true

-- 4b. CORTEX_USER and CORTEX_ANALYST_USER should appear in GSF_ROLE's grants
SHOW GRANTS TO ROLE GSF_ROLE;
-- Expected: rows for SNOWFLAKE.CORTEX_USER and SNOWFLAKE.CORTEX_ANALYST_USER

-- 4c. Tags should exist in GOLD schema
SHOW TAGS IN SCHEMA GSF_DEMO.GOLD;
-- Expected: FINANCIAL_SENSITIVITY and PII_CLASSIFICATION

-- 4d. Quick smoke test — run after granting to confirm Cortex functions work
-- Switch to GSF_ROLE for this test:
USE ROLE GSF_ROLE;
USE WAREHOUSE GSF_WH;

SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', 'Reply with the single word: ready') AS cortex_test;
-- Expected: 'ready' (or similar single-word response)
-- If this fails, Cortex AI functions are not reachable — check network policy or allowlist.

-- Return to ACCOUNTADMIN for any follow-up grants
USE ROLE ACCOUNTADMIN;
