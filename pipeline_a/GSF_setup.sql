-- =============================================================================
-- GSF_DEMO Infrastructure Setup
-- =============================================================================
-- Creates the database, medallion schemas, compute warehouse, and demo role
-- for the Gemstone Financial semantic layer bias demonstration.
--
-- Run once per Snowflake account (idempotent — uses IF NOT EXISTS).
-- Requires ACCOUNTADMIN or SYSADMIN with CREATE DATABASE privilege.
--
-- Naming convention: all objects use the GSF (Gemstone Financial) prefix.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- ── Warehouse ─────────────────────────────────────────────────────────────────
-- X-Small is sufficient for seed-data scale. Auto-suspend after 60s of idle.

CREATE WAREHOUSE IF NOT EXISTS GSF_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'GSF demo warehouse — X-Small, auto-suspend 60s idle';

-- ── Database ──────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS GSF_DEMO
    COMMENT = 'Gemstone Financial Demo — semantic layer bias study (Bronze/Silver/Gold)';

USE DATABASE GSF_DEMO;

-- ── Medallion Schemas ─────────────────────────────────────────────────────────
-- Bronze / Silver / Gold map directly to the three-act demo narrative.

CREATE SCHEMA IF NOT EXISTS GSF_DEMO.BRONZE
    COMMENT = 'Act 1 — raw source tables loaded as-is from seed CSVs (no governance)';

CREATE SCHEMA IF NOT EXISTS GSF_DEMO.SILVER
    COMMENT = 'Act 2 — naive ETL integration table: looks normalized, semantically broken (A7-A11)';

CREATE SCHEMA IF NOT EXISTS GSF_DEMO.GOLD
    COMMENT = 'Act 3 — governed DW tables + Cortex Analyst semantic model (Pipeline B)';

-- ── Role ──────────────────────────────────────────────────────────────────────

CREATE ROLE IF NOT EXISTS GSF_ROLE
    COMMENT = 'Demo role for GSF_DEMO — used for all pipeline and Cortex Analyst operations';

GRANT USAGE ON WAREHOUSE GSF_WH TO ROLE GSF_ROLE;

GRANT ALL PRIVILEGES ON DATABASE GSF_DEMO TO ROLE GSF_ROLE;

GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE GSF_DEMO TO ROLE GSF_ROLE;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE GSF_DEMO TO ROLE GSF_ROLE;

GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE GSF_DEMO TO ROLE GSF_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE GSF_DEMO TO ROLE GSF_ROLE;

GRANT ALL PRIVILEGES ON ALL STAGES IN DATABASE GSF_DEMO TO ROLE GSF_ROLE;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN DATABASE GSF_DEMO TO ROLE GSF_ROLE;

-- Grant role to current user so subsequent scripts can USE ROLE GSF_ROLE
GRANT ROLE GSF_ROLE TO USER CURRENT_USER();

-- ── Verify ────────────────────────────────────────────────────────────────────
SHOW SCHEMAS IN DATABASE GSF_DEMO;
