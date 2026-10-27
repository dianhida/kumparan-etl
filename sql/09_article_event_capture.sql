-- Phase 4: PostgreSQL Event Capture
-- Creates article_event table and trigger for INSERT, UPDATE, DELETE on articles

-- Enable uuid-ossp extension for UUID generation (built-in gen_random_uuid in PG 13+)
-- Using gen_random_uuid() which is available in PostgreSQL 13+

-- Create article_event table
CREATE TABLE IF NOT EXISTS public.article_event (
    event_id UUID NOT NULL DEFAULT gen_random_uuid(),
    article_id BIGINT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('INSERT', 'UPDATE', 'DELETE')),
    payload JSONB NOT NULL,
    event_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Processing status for incremental ETL
    status TEXT NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCEEDED', 'FAILED')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    python_msg TEXT,
    exception_msg TEXT,
    claimed_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,

    PRIMARY KEY (event_id)
);

-- Indexes for article_event
CREATE INDEX IF NOT EXISTS idx_article_event_status
    ON public.article_event (status);
CREATE INDEX IF NOT EXISTS idx_article_event_article_id
    ON public.article_event (article_id);
CREATE INDEX IF NOT EXISTS idx_article_event_event_at
    ON public.article_event (event_at);
CREATE INDEX IF NOT EXISTS idx_article_event_status_event_at
    ON public.article_event (status, event_at);

-- Trigger function to capture article changes
CREATE OR REPLACE FUNCTION public.capture_article_event()
RETURNS TRIGGER AS $$
DECLARE
    v_payload JSONB;
    v_event_type TEXT;
BEGIN
    -- Determine event type and payload
    IF TG_OP = 'INSERT' THEN
        v_event_type := 'INSERT';
        v_payload := to_jsonb(NEW) - 'event_id' - 'status' - 'retry_count' - 'python_msg' - 'exception_msg' - 'claimed_at' - 'processed_at';
        -- Remove any auto-generated or system columns that shouldn't be in payload
        v_payload := v_payload || jsonb_build_object('id', NEW.id);

    ELSIF TG_OP = 'UPDATE' THEN
        -- Only generate event if relevant columns changed
        IF NEW.title IS DISTINCT FROM OLD.title
            OR NEW.content IS DISTINCT FROM OLD.content
            OR NEW.published_at IS DISTINCT FROM OLD.published_at
            OR NEW.author_id IS DISTINCT FROM OLD.author_id
            OR NEW.updated_at IS DISTINCT FROM OLD.updated_at
            OR NEW.deleted_at IS DISTINCT FROM OLD.deleted_at THEN
            
            v_event_type := 'UPDATE';
            v_payload := to_jsonb(NEW) - 'event_id' - 'status' - 'retry_count' - 'python_msg' - 'exception_msg' - 'claimed_at' - 'processed_at';
            v_payload := v_payload || jsonb_build_object('id', NEW.id);
        ELSE
            -- No relevant changes, skip event generation
            RETURN NEW;
        END IF;

    ELSIF TG_OP = 'DELETE' THEN
        v_event_type := 'DELETE';
        -- For DELETE, capture OLD row values (the deleted article)
        v_payload := to_jsonb(OLD) - 'event_id' - 'status' - 'retry_count' - 'python_msg' - 'exception_msg' - 'claimed_at' - 'processed_at';
        v_payload := v_payload || jsonb_build_object('id', OLD.id);
    ELSE
        RETURN NULL;
    END IF;

    -- Insert event record
    INSERT INTO public.article_event (article_id, event_type, payload, event_at)
    VALUES (
        COALESCE(NEW.id, OLD.id),
        v_event_type,
        v_payload,
        NOW()
    );

    RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on articles table
-- Drop existing trigger if exists to make this idempotent
DROP TRIGGER IF EXISTS trg_articles_capture_event ON public.articles;

CREATE TRIGGER trg_articles_capture_event
    AFTER INSERT OR UPDATE OR DELETE ON public.articles
    FOR EACH ROW EXECUTE FUNCTION public.capture_article_event();

-- Grant necessary permissions (if using non-superuser)
-- GRANT SELECT, INSERT ON public.article_event TO kumparan;
-- GRANT USAGE, SELECT ON SEQUENCE public.articles_id_seq TO kumparan;

-- Verification query
SELECT 'article_event table created' as status, COUNT(*) as row_count FROM public.article_event;