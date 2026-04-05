-- =============================================================================
-- S3 External Stage + Storage Integration
-- =============================================================================
-- Creates a Snowflake storage integration and external stage pointing to the
-- S3 landing zone where source system files are delivered by delivery/deliver.py.
--
-- Prerequisites:
--   1. Run infrastructure/snowflake_setup.sql first (creates GSF_DEMO, GSF_ROLE)
--   2. S3 bucket must exist: gsf-demo-landing (or your chosen bucket name)
--   3. IAM role must exist with s3:GetObject, s3:ListBucket on the bucket
--
-- Run as: ACCOUNTADMIN (storage integrations require ACCOUNTADMIN)
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- ── Section 1: Storage Integration ──────────────────────────────────────────
-- A storage integration is a Snowflake object that stores IAM credentials
-- for accessing an external cloud storage location. It is created once and
-- referenced by external stages.
--
-- IMPORTANT: After creating, run DESC INTEGRATION to get the Snowflake IAM
-- role ARN and external ID. These must be added to your AWS IAM role's
-- trust policy. See Section 3 below.

CREATE OR REPLACE STORAGE INTEGRATION gsf_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<your-iam-role-arn>'       -- e.g. arn:aws:iam::123456789012:role/gsf-snowflake-access
    STORAGE_ALLOWED_LOCATIONS = ('s3://gsf-demo-landing/');

-- Grant usage to GSF_ROLE so pipeline scripts can access the stage
GRANT USAGE ON INTEGRATION gsf_s3_integration TO ROLE GSF_ROLE;

-- ── Section 2: External Stage ───────────────────────────────────────────────

USE ROLE GSF_ROLE;
USE DATABASE GSF_DEMO;
USE WAREHOUSE GSF_WH;

CREATE OR REPLACE STAGE BRONZE.GSF_S3_LANDING
    URL = 's3://gsf-demo-landing/'
    STORAGE_INTEGRATION = gsf_s3_integration
    FILE_FORMAT = (
        TYPE = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        NULL_IF = ('', 'None', 'nan')
        EMPTY_FIELD_AS_NULL = TRUE
        DATE_FORMAT = 'YYYY-MM-DD'
        TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    )
    COMMENT = 'S3 landing zone for source system file delivery';

-- Verify the stage can list files (run after delivery/deliver.py)
-- LIST @BRONZE.GSF_S3_LANDING;

-- ── Section 3: AWS IAM Trust Policy (reference) ─────────────────────────────
-- After creating the storage integration, get the Snowflake values:
--
--   DESC INTEGRATION gsf_s3_integration;
--
-- Look for:
--   STORAGE_AWS_IAM_USER_ARN     (Snowflake's IAM user)
--   STORAGE_AWS_EXTERNAL_ID      (external ID for trust)
--
-- Add these to your IAM role's trust policy:
--
--   {
--     "Version": "2012-10-17",
--     "Statement": [
--       {
--         "Effect": "Allow",
--         "Principal": {
--           "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
--         },
--         "Action": "sts:AssumeRole",
--         "Condition": {
--           "StringEquals": {
--             "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
--           }
--         }
--       }
--     ]
--   }
--
-- And ensure the IAM role has this permission policy:
--
--   {
--     "Version": "2012-10-17",
--     "Statement": [
--       {
--         "Effect": "Allow",
--         "Action": [
--           "s3:GetObject",
--           "s3:GetObjectVersion",
--           "s3:ListBucket",
--           "s3:GetBucketLocation"
--         ],
--         "Resource": [
--           "arn:aws:s3:::gsf-demo-landing",
--           "arn:aws:s3:::gsf-demo-landing/*"
--         ]
--       }
--     ]
--   }

-- ── Section 4: Verification ─────────────────────────────────────────────────
-- After completing IAM setup and running deliver.py:
--
-- LIST @BRONZE.GSF_S3_LANDING/topaz/;
-- LIST @BRONZE.GSF_S3_LANDING/emerald/;
-- LIST @BRONZE.GSF_S3_LANDING/ruby/;
-- LIST @BRONZE.GSF_S3_LANDING/reference/;
--
-- Test COPY (dry run):
-- COPY INTO BRONZE.TOPAZ_POSITIONS
--   FROM @BRONZE.GSF_S3_LANDING/topaz/positions_topaz.csv
--   VALIDATION_MODE = 'RETURN_ALL_ERRORS';
