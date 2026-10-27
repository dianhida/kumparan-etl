-- Phase 5: Gold Layer Transformation
-- Populates gold.article_daily_metrics from bronze snapshot and events
-- Idempotent: uses INSERT ... ON CONFLICT to upsert by metric_date

-- This transformation combines:
-- 1. Creation metrics from bronze.articles_snapshot (initial load) + INSERT events
-- 2. Publication metrics from bronze.articles_snapshot (initial load) + INSERT/UPDATE events with published_at
-- 3. Deletion metrics from bronze.article_events (DELETE events only)

-- Use UTC date for metric_date
-- Refreshed timestamp for tracking when metrics were last computed

-- First, compute creation metrics
-- From initial snapshot (is_initial_snapshot = true)
-- Plus INSERT events from bronze.article_events

WITH creation_metrics AS (
    -- Initial snapshot creations
    SELECT
        created_at::DATE AS metric_date,
        COUNT(*) AS articles_created
    FROM bronze.articles_snapshot
    WHERE is_initial_snapshot = TRUE
    GROUP BY created_at::DATE

    UNION ALL

    -- INSERT events (new articles created after snapshot)
    SELECT
        event_at::DATE AS metric_date,
        COUNT(*) AS articles_created
    FROM bronze.article_events
    WHERE event_type = 'INSERT'
    GROUP BY event_at::DATE
),
creation_agg AS (
    SELECT metric_date, SUM(articles_created) AS articles_created
    FROM creation_metrics
    GROUP BY metric_date
),

-- Publication metrics
publication_metrics AS (
    -- Initial snapshot publications (only published articles)
    SELECT
        published_at::DATE AS metric_date,
        COUNT(*) AS articles_published
    FROM bronze.articles_snapshot
    WHERE is_initial_snapshot = TRUE
      AND published_at IS NOT NULL
    GROUP BY published_at::DATE

    UNION ALL

    -- INSERT events with published_at
    SELECT
        event_at::DATE AS metric_date,
        COUNT(*) AS articles_published
    FROM bronze.article_events
    WHERE event_type = 'INSERT'
      AND json_extract(payload, '$.published_at') IS NOT NULL
    GROUP BY event_at::DATE

    UNION ALL

    -- UPDATE events that change published_at from NULL to non-NULL
    -- or change published_at to a different date
    SELECT
        event_at::DATE AS metric_date,
        COUNT(*) AS articles_published
    FROM bronze.article_events
    WHERE event_type = 'UPDATE'
      AND json_extract(payload, '$.published_at') IS NOT NULL
    GROUP BY event_at::DATE
),
publication_agg AS (
    SELECT metric_date, SUM(articles_published) AS articles_published
    FROM publication_metrics
    GROUP BY metric_date
),

-- Deletion metrics (only from DELETE events, not from deleted_at column)
deletion_metrics AS (
    SELECT
        event_at::DATE AS metric_date,
        COUNT(*) AS articles_deleted
    FROM bronze.article_events
    WHERE event_type = 'DELETE'
    GROUP BY event_at::DATE
),
deletion_agg AS (
    SELECT metric_date, SUM(articles_deleted) AS articles_deleted
    FROM deletion_metrics
    GROUP BY metric_date
),

-- Combine all metrics - use FULL OUTER JOIN to include dates with any activity
all_dates AS (
    SELECT metric_date FROM creation_agg
    UNION
    SELECT metric_date FROM publication_agg
    UNION
    SELECT metric_date FROM deletion_agg
),
combined AS (
    SELECT
        d.metric_date,
        COALESCE(c.articles_created, 0) AS articles_created,
        COALESCE(p.articles_published, 0) AS articles_published,
        COALESCE(del.articles_deleted, 0) AS articles_deleted,
        CURRENT_TIMESTAMP AS refreshed_at
    FROM all_dates d
    LEFT JOIN creation_agg c ON d.metric_date = c.metric_date
    LEFT JOIN publication_agg p ON d.metric_date = p.metric_date
    LEFT JOIN deletion_agg del ON d.metric_date = del.metric_date
)
-- Upsert into gold.article_daily_metrics
INSERT INTO gold.article_daily_metrics (metric_date, articles_created, articles_published, articles_deleted, refreshed_at)
SELECT metric_date, articles_created, articles_published, articles_deleted, refreshed_at
FROM combined
ON CONFLICT (metric_date) DO UPDATE SET
    articles_created = EXCLUDED.articles_created,
    articles_published = EXCLUDED.articles_published,
    articles_deleted = EXCLUDED.articles_deleted,
    refreshed_at = EXCLUDED.refreshed_at;

-- Verification query
SELECT
    metric_date,
    articles_created,
    articles_published,
    articles_deleted,
    refreshed_at
FROM gold.article_daily_metrics
ORDER BY metric_date
LIMIT 20;