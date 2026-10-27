-- Phase 2: Articles table schema
-- This file creates the source articles table in PostgreSQL

-- Create articles table if not exists
CREATE TABLE IF NOT EXISTS articles (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    author_id INTEGER NOT NULL CHECK (author_id > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

-- Add constraint to ensure updated_at >= created_at
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'articles_updated_at_after_created_at'
    ) THEN
        ALTER TABLE articles
        ADD CONSTRAINT articles_updated_at_after_created_at
        CHECK (updated_at >= created_at);
    END IF;
END $$;

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_articles_created_at ON articles (created_at);
CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles (published_at);
CREATE INDEX IF NOT EXISTS idx_articles_author_id ON articles (author_id);
CREATE INDEX IF NOT EXISTS idx_articles_deleted_at ON articles (deleted_at);