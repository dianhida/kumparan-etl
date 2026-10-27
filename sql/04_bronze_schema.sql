-- Phase 3: Bronze Layer Schema
-- Raw source snapshots and captured change history

-- Create bronze schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- bronze.articles_snapshot
-- Stores the initial source snapshot
-- All source columns preserved, plus ingestion metadata
CREATE TABLE IF NOT EXISTS bronze.articles_snapshot (
    -- Source columns (preserved as-is)
    id BIGINT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    author_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ,

    -- Ingestion metadata
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    snapshot_batch_id UUID NOT NULL,

    -- Distinguish initial snapshot from subsequent change events
    is_initial_snapshot BOOLEAN NOT NULL DEFAULT TRUE,

    PRIMARY KEY (id, snapshot_batch_id)
);

-- Indexes for bronze.articles_snapshot
CREATE INDEX IF NOT EXISTS idx_bronze_articles_snapshot_created_at
    ON bronze.articles_snapshot (created_at);
CREATE INDEX IF NOT EXISTS idx_bronze_articles_snapshot_snapshot_batch_id
    ON bronze.articles_snapshot (snapshot_batch_id);
CREATE INDEX IF NOT EXISTS idx_bronze_articles_snapshot_ingested_at
    ON bronze.articles_snapshot (ingested_at);

-- bronze.article_events
-- Stores source change events captured from PostgreSQL triggers
-- Compatible with planned PostgreSQL event log structure
CREATE TABLE IF NOT EXISTS bronze.article_events (
    event_id UUID NOT NULL,
    article_id BIGINT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('INSERT', 'UPDATE', 'DELETE')),
    payload JSON NOT NULL,
    event_at TIMESTAMPTZ NOT NULL,

    -- Ingestion metadata
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,

    PRIMARY KEY (event_id)
);

-- Indexes for bronze.article_events
CREATE INDEX IF NOT EXISTS idx_bronze_article_events_article_id
    ON bronze.article_events (article_id);
CREATE INDEX IF NOT EXISTS idx_bronze_article_events_event_at
    ON bronze.article_events (event_at);
CREATE INDEX IF NOT EXISTS idx_bronze_article_events_event_type
    ON bronze.article_events (event_type);
CREATE INDEX IF NOT EXISTS idx_bronze_article_events_ingested_at
    ON bronze.article_events (ingested_at);
CREATE INDEX IF NOT EXISTS idx_bronze_article_events_processed_at
    ON bronze.article_events (processed_at);