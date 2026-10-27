-- Phase 2: Data Validation Queries
-- Run these queries to validate the generated dataset

-- 1. Total article count
SELECT 'Total Articles' as check_name, COUNT(*) as value FROM articles;

-- 2. Min and Max created_at
SELECT 'Min created_at' as check_name, MIN(created_at) as value FROM articles;
SELECT 'Max created_at' as check_name, MAX(created_at) as value FROM articles;

-- 3. Distribution by creation year
SELECT
    EXTRACT(YEAR FROM created_at) as year,
    COUNT(*) as article_count
FROM articles
GROUP BY EXTRACT(YEAR FROM created_at)
ORDER BY year;

-- 4. Published vs Unpublished counts
SELECT
    CASE WHEN published_at IS NULL THEN 'Unpublished' ELSE 'Published' END as status,
    COUNT(*) as count
FROM articles
GROUP BY CASE WHEN published_at IS NULL THEN 'Unpublished' ELSE 'Published' END;

-- 5. NULL counts for relevant fields
SELECT
    'title' as field, COUNT(*) FILTER (WHERE title IS NULL) as null_count FROM articles
UNION ALL
SELECT 'content', COUNT(*) FILTER (WHERE content IS NULL) FROM articles
UNION ALL
SELECT 'published_at', COUNT(*) FILTER (WHERE published_at IS NULL) FROM articles
UNION ALL
SELECT 'author_id', COUNT(*) FILTER (WHERE author_id IS NULL) FROM articles
UNION ALL
SELECT 'created_at', COUNT(*) FILTER (WHERE created_at IS NULL) FROM articles
UNION ALL
SELECT 'updated_at', COUNT(*) FILTER (WHERE updated_at IS NULL) FROM articles
UNION ALL
SELECT 'deleted_at', COUNT(*) FILTER (WHERE deleted_at IS NULL) FROM articles;

-- 6. Invalid timestamp relationships (updated_at < created_at)
SELECT 'Invalid: updated_at < created_at' as check_name, COUNT(*) as count
FROM articles
WHERE updated_at < created_at;

-- Invalid: published_at < created_at
SELECT 'Invalid: published_at < created_at' as check_name, COUNT(*) as count
FROM articles
WHERE published_at IS NOT NULL AND published_at < created_at;

-- Invalid: published_at > updated_at
SELECT 'Invalid: published_at > updated_at' as check_name, COUNT(*) as count
FROM articles
WHERE published_at IS NOT NULL AND published_at > updated_at;

-- 7. Duplicate primary keys (should be 0)
SELECT 'Duplicate IDs' as check_name, COUNT(*) as count
FROM (
    SELECT id, COUNT(*) as cnt
    FROM articles
    GROUP BY id
    HAVING COUNT(*) > 1
) duplicates;

-- 8. Author distribution
SELECT author_id, COUNT(*) as article_count
FROM articles
GROUP BY author_id
ORDER BY article_count DESC
LIMIT 20;

-- 9. Articles with deleted_at not NULL (should be 0 for initial dataset)
SELECT 'Non-NULL deleted_at' as check_name, COUNT(*) as count
FROM articles
WHERE deleted_at IS NOT NULL;

-- 10. Summary statistics
SELECT
    COUNT(*) as total_articles,
    COUNT(DISTINCT author_id) as unique_authors,
    MIN(created_at) as earliest_article,
    MAX(created_at) as latest_article,
    COUNT(*) FILTER (WHERE published_at IS NOT NULL) as published_count,
    COUNT(*) FILTER (WHERE published_at IS NULL) as unpublished_count
FROM articles;