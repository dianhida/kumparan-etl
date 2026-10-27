-- Phase 3: Silver Layer Schema
-- Current state of active articles

-- Create silver schema
CREATE SCHEMA IF NOT EXISTS silver;

-- silver.articles_current
-- Represents the current state of active (non-deleted) articles
-- Physically removes rows when hard-delete events are processed
-- No SCD Type 2 - historical versions retained in Bronze
CREATE TABLE IF NOT EXISTS silver.articles_current (
    id BIGINT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    author_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ,

    -- Warehouse metadata
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

-- Indexes for silver.articles_current
CREATE INDEX IF NOT EXISTS idx_silver_articles_current_published_at
    ON silver.articles_current (published_at);
CREATE INDEX IF NOT EXISTS idx_silver_articles_current_author_id
    ON silver.articles_current (author_id);
CREATE INDEX IF NOT EXISTS idx_silver_articles_current_created_at
    ON silver.articles_current (created_at);
CREATE INDEX IF NOT EXISTS idx_silver_articles_current_updated_at
    ON silver.articles_current (updated_at);
CREATE INDEX IF NOT EXISTS idx_silver_articles_current_last_synced_at
    ON silver.articles_current (last_synced_at);

-- Note: deleted_at is kept for reference but rows with non-NULL deleted_at
-- should be physically deleted from this table when hard-delete events are processed
-- The constraint below ensures we only track active articles
-- (This is a documentation constraint - actual enforcement happens during ETL)
-- ALTER TABLE silver.articles_current ADD CONSTRAINT chk_active_articles
--     CHECK (deleted_at IS NULL);