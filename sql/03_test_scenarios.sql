-- Phase 2: Controlled Test Scenarios
-- These are documented SQL examples for later testing.
-- DO NOT execute automatically during initial dataset generation.
-- Use clearly identifiable test records (prefix: TEST_) for easy cleanup.

-- =============================================================================
-- CLEANUP: Remove any existing test records first
-- =============================================================================
-- DELETE FROM articles WHERE title LIKE 'TEST_%';

-- =============================================================================
-- SCENARIO 1: Insert a new article
-- =============================================================================
INSERT INTO articles (title, content, published_at, author_id, created_at, updated_at, deleted_at)
VALUES (
    'TEST_New_Article_Insert',
    'This is a test article for verifying INSERT operations.',
    NOW() AT TIME ZONE 'UTC',
    999,  -- Test author_id
    NOW() AT TIME ZONE 'UTC',
    NOW() AT TIME ZONE 'UTC',
    NULL
)
RETURNING id, title, created_at;

-- =============================================================================
-- SCENARIO 2: Update an existing article
-- =============================================================================
-- First, find a test article or create one
-- UPDATE articles
-- SET
--     title = 'TEST_Updated_Title',
--     content = 'Updated content for testing UPDATE operations.',
--     updated_at = NOW() AT TIME ZONE 'UTC'
-- WHERE title = 'TEST_New_Article_Insert'
-- RETURNING id, title, updated_at;

-- =============================================================================
-- SCENARIO 3: Hard-delete an existing article
-- =============================================================================
-- DELETE FROM articles WHERE title = 'TEST_New_Article_Insert';

-- =============================================================================
-- SCENARIO 4: Insert an unpublished article (published_at = NULL)
-- =============================================================================
INSERT INTO articles (title, content, published_at, author_id, created_at, updated_at, deleted_at)
VALUES (
    'TEST_Unpublished_Article',
    'This article is not published yet.',
    NULL,  -- Unpublished
    999,
    NOW() AT TIME ZONE 'UTC',
    NOW() AT TIME ZONE 'UTC',
    NULL
)
RETURNING id, title, published_at, created_at;

-- =============================================================================
-- SCENARIO 5: Update an article's publication timestamp
-- =============================================================================
-- UPDATE articles
-- SET
--     published_at = NOW() AT TIME ZONE 'UTC',
--     updated_at = NOW() AT TIME ZONE 'UTC'
-- WHERE title = 'TEST_Unpublished_Article'
-- RETURNING id, title, published_at, updated_at;

-- =============================================================================
-- VERIFICATION QUERIES FOR TEST SCENARIOS
-- =============================================================================
-- Find all test articles
-- SELECT id, title, published_at, author_id, created_at, updated_at, deleted_at
-- FROM articles
-- WHERE title LIKE 'TEST_%'
-- ORDER BY created_at;

-- Count test articles
-- SELECT COUNT(*) FROM articles WHERE title LIKE 'TEST_%';

-- =============================================================================
-- CLEANUP: Remove all test records after testing
-- =============================================================================
-- DELETE FROM articles WHERE title LIKE 'TEST_%';