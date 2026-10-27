-- Phase 3: Gold Layer Schema
-- Analytics-ready daily metrics

-- Create gold schema
CREATE SCHEMA IF NOT EXISTS gold;

-- gold.article_daily_metrics
-- Daily activity metrics for articles
-- Date grain: day (UTC), based on event timestamps converted to UTC date
-- 
-- Semantics:
-- - metric_date: The UTC date of the metric (YYYY-MM-DD)
-- - articles_created: Count of articles with created_at::date = metric_date
-- - articles_published: Count of articles with published_at::date = metric_date (non-NULL)
-- - articles_deleted: Count of delete events with event_at::date = metric_date
-- - refreshed_at: Timestamp when this metric row was last computed
--
-- Limitations of initial snapshot:
-- - Initial source data only provides created_at and updated_at
-- - published_at is available for published articles
-- - Hard deletes are NOT captured in source (deleted_at is always NULL initially)
-- - Historical publication/deletion events before snapshot are unknown
-- - Gold metrics for pre-snapshot dates can only be partially reconstructed
--   from created_at and published_at; true deletion events require event log
CREATE TABLE IF NOT EXISTS gold.article_daily_metrics (
    metric_date DATE NOT NULL,
    articles_created BIGINT NOT NULL DEFAULT 0,
    articles_published BIGINT NOT NULL DEFAULT 0,
    articles_deleted BIGINT NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (metric_date)
);

-- Indexes for gold.article_daily_metrics
CREATE INDEX IF NOT EXISTS idx_gold_article_daily_metrics_refreshed_at
    ON gold.article_daily_metrics (refreshed_at);

-- Note: This table will be populated by SQL transformations in Phase 5+
-- The transformation should:
-- 1. Use bronze.articles_snapshot for initial load (created_at, published_at)
-- 2. Use bronze.article_events for incremental updates (all event types)
-- 3. For articles_deleted: use event_at from DELETE events, not deleted_at from source
-- 4. Handle late-arriving events by recomputing affected dates
-- 5. Use UTC consistently for date grain conversion