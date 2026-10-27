-- Phase 3: Master Warehouse Initialization
-- Runs all schema creation scripts in order
-- Idempotent: safe to run multiple times

-- Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;

-- Run individual schema files
-- Order: bronze -> silver -> gold -> metadata
-- (metadata last as it may reference other schemas for documentation)

-- Include bronze schema
\i 04_bronze_schema.sql

-- Include silver schema
\i 05_silver_schema.sql

-- Include gold schema
\i 06_gold_schema.sql

-- Include metadata schema
\i 07_metadata_schema.sql

-- Verify all tables created
SELECT 'bronze.articles_snapshot' as table_name, count(*) as row_count FROM bronze.articles_snapshot
UNION ALL
SELECT 'bronze.article_events', count(*) FROM bronze.article_events
UNION ALL
SELECT 'silver.articles_current', count(*) FROM silver.articles_current
UNION ALL
SELECT 'gold.article_daily_metrics', count(*) FROM gold.article_daily_metrics
UNION ALL
SELECT 'metadata.pipeline_state', count(*) FROM metadata.pipeline_state
UNION ALL
SELECT 'metadata.processed_events', count(*) FROM metadata.processed_events;