-- Phase 3: Metadata Layer Schema
-- Pipeline initialization state and operational metadata

-- Create metadata schema
CREATE SCHEMA IF NOT EXISTS metadata;

-- metadata.pipeline_state
-- Persists warehouse initialization and pipeline lifecycle state
-- Supports initial-load and incremental-processing safeguards
CREATE TABLE IF NOT EXISTS metadata.pipeline_state (
    pipeline_id TEXT NOT NULL DEFAULT 'articles_pipeline',
    current_phase TEXT NOT NULL CHECK (current_phase IN (
        'INITIALIZING',
        'FULL_LOAD_RUNNING',
        'RECONCILING',
        'READY',
        'FAILED'
    )),
    status_message TEXT,
    snapshot_batch_id UUID,
    loaded_count BIGINT DEFAULT 0,
    last_article_id BIGINT DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,

    PRIMARY KEY (pipeline_id)
);

-- Initialize with default state
INSERT INTO metadata.pipeline_state (pipeline_id, current_phase, status_message)
VALUES ('articles_pipeline', 'INITIALIZING', 'Warehouse schemas created')
ON CONFLICT (pipeline_id) DO NOTHING;

-- metadata.processed_events
-- Idempotency checkpoint for incremental processing
-- event_id is the unique key (from bronze.article_events.event_id)
-- event application, Silver mutation, and checkpoint insertion
-- must share the same DuckDB transaction
CREATE TABLE IF NOT EXISTS metadata.processed_events (
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (event_id)
);

-- Index for metadata.processed_events
CREATE INDEX IF NOT EXISTS idx_metadata_processed_events_processed_at
    ON metadata.processed_events (processed_at);