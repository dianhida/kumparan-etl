#!/usr/bin/env python3
"""Phase 5 Integration Tests."""

import sys
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
import duckdb


# Load environment
load_dotenv()
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def get_pg_conn():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "kumparan"),
        password=os.getenv("POSTGRES_PASSWORD", "kumparan"),
        dbname=os.getenv("POSTGRES_DB", "kumparan"),
        connect_timeout=10,
        row_factory=dict_row,
    )


def get_duck_conn():
    duckdb_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    if not os.path.isabs(duckdb_path):
        duckdb_path = project_root / duckdb_path
    return duckdb.connect(str(duckdb_path))


from scripts.process_events import process_batch
from scripts.run_gold import run_gold_transformation


def reset_test_data(pg_conn, duck_conn):
    """Clean up test data."""
    test_ids = [999000 + i for i in range(100)]
    
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM public.article_event WHERE article_id = ANY(%s)", (test_ids,))
        cur.execute("DELETE FROM public.articles WHERE id = ANY(%s)", (test_ids,))
        pg_conn.commit()
    
    for tid in test_ids:
        duck_conn.execute("DELETE FROM bronze.articles_snapshot WHERE id = ?", (tid,))
        duck_conn.execute("DELETE FROM silver.articles_current WHERE id = ?", (tid,))
        duck_conn.execute("DELETE FROM bronze.article_events WHERE article_id = ?", (tid,))
        duck_conn.execute("DELETE FROM metadata.processed_events WHERE event_id IN (SELECT event_id FROM bronze.article_events WHERE article_id = ?)", (tid,))


def test_bronze_silver_all_active(pg_conn, duck_conn):
    """Test 1: All articles in initial snapshot are active (deleted_at IS NULL in Silver)."""
    print("Test 1: Bronze snapshot → Silver only active articles...")
    
    # Count in Bronze snapshot (initial load)
    bronze_count = duck_conn.execute(
        "SELECT COUNT(*) FROM bronze.articles_snapshot WHERE is_initial_snapshot = TRUE"
    ).fetchone()[0]
    
    # Count in Silver
    silver_count = duck_conn.execute(
        "SELECT COUNT(*) FROM silver.articles_current"
    ).fetchone()[0]
    
    # Count deleted in Bronze snapshot
    bronze_deleted = duck_conn.execute(
        "SELECT COUNT(*) FROM bronze.articles_snapshot WHERE is_initial_snapshot = TRUE AND deleted_at IS NOT NULL"
    ).fetchone()[0]
    
    # Silver should have NO deleted_at
    silver_deleted = duck_conn.execute(
        "SELECT COUNT(*) FROM silver.articles_current WHERE deleted_at IS NOT NULL"
    ).fetchone()[0]
    
    print(f"  Bronze snapshot (initial): {bronze_count}")
    print(f"  Bronze deleted_at NOT NULL: {bronze_deleted}")
    print(f"  Silver current: {silver_count}")
    print(f"  Silver deleted_at NOT NULL: {silver_deleted}")
    
    # In initial dataset, deleted_at is always NULL
    assert bronze_deleted == 0, f"Expected no deleted in initial snapshot, got {bronze_deleted}"
    assert silver_deleted == 0, f"Expected no deleted in Silver, got {silver_deleted}"
    
    # Silver >= Bronze snapshot (new articles added via events)
    assert silver_count >= bronze_count, f"Silver ({silver_count}) < Bronze snapshot ({bronze_count})"
    
    print("  ✓ All Bronze snapshot articles are active in Silver")


def test_gold_consistency(pg_conn, duck_conn):
    """Test 2: Gold metrics consistent with source."""
    print("Test 2: Gold metrics consistency with source...")
    
    # Get counts from PostgreSQL
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM public.articles WHERE deleted_at IS NULL")
        pg_active = cur.fetchone()['cnt']
        
        cur.execute("SELECT COUNT(*) AS cnt FROM public.articles WHERE published_at IS NOT NULL")
        pg_published = cur.fetchone()['cnt']
    
    # Get Gold sums
    gold_created = duck_conn.execute(
        "SELECT SUM(articles_created) FROM gold.article_daily_metrics"
    ).fetchone()[0]
    
    gold_published = duck_conn.execute(
        "SELECT SUM(articles_published) FROM gold.article_daily_metrics"
    ).fetchone()[0]
    
    gold_deleted = duck_conn.execute(
        "SELECT SUM(articles_deleted) FROM gold.article_daily_metrics"
    ).fetchone()[0]
    
    # Silver should match PostgreSQL active
    silver_count = duck_conn.execute(
        "SELECT COUNT(*) FROM silver.articles_current"
    ).fetchone()[0]
    
    print(f"  PostgreSQL active: {pg_active}")
    print(f"  PostgreSQL published: {pg_published}")
    print(f"  Gold articles_created: {gold_created}")
    print(f"  Gold articles_published: {gold_published}")
    print(f"  Gold articles_deleted: {gold_deleted}")
    print(f"  Silver count: {silver_count}")
    
    # Silver matches current PG active
    assert silver_count == pg_active, f"Silver ({silver_count}) != PG active ({pg_active})"
    
    # Gold created >= PG active (includes historical creates from events)
    assert gold_created >= pg_active, f"Gold created ({gold_created}) < PG active ({pg_active})"
    
    # Gold published >= PG published (includes historical publications from deleted articles)
    assert gold_published >= pg_published, f"Gold published ({gold_published}) < PG published ({pg_published})"
    
    # Gold deleted >= 0 (hard deletes captured)
    assert gold_deleted >= 0, f"Gold deleted should be >= 0, got {gold_deleted}"
    
    print("  ✓ Gold metrics consistent with source")


def test_hard_delete_scenario(pg_conn, duck_conn):
    """Test 3: Hard delete scenario - DELETE removes from Silver, preserved in Bronze."""
    print("Test 3: Hard delete handling...")
    
    test_id = 999900
    
    # Insert test article
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.articles (id, title, content, published_at, author_id, created_at, updated_at, deleted_at)
            VALUES (%s, 'Hard Delete Test', 'Content', NOW(), 999, NOW(), NOW(), NULL)
        """, (test_id,))
        pg_conn.commit()
    
    # Process INSERT event
    process_batch(pg_conn, duck_conn, batch_size=10)
    
    # Verify in Silver
    silver_before = duck_conn.execute("SELECT COUNT(*) FROM silver.articles_current WHERE id = ?", (test_id,)).fetchone()[0]
    assert silver_before == 1, "Article should be in Silver after INSERT"
    
    # Hard delete from PostgreSQL
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM public.articles WHERE id = %s", (test_id,))
        pg_conn.commit()
    
    # Process DELETE event
    process_batch(pg_conn, duck_conn, batch_size=10)
    
    # Verify Silver - should be removed
    silver_after = duck_conn.execute("SELECT COUNT(*) FROM silver.articles_current WHERE id = ?", (test_id,)).fetchone()[0]
    assert silver_after == 0, "Article should be removed from Silver after hard DELETE"
    
    # Verify Bronze - event preserved
    bronze_events = duck_conn.execute(
        "SELECT event_type FROM bronze.article_events WHERE article_id = ? ORDER BY event_at", (test_id,)
    ).fetchall()
    event_types = [e[0] for e in bronze_events]
    assert 'INSERT' in event_types and 'DELETE' in event_types, f"Expected INSERT and DELETE events, got {event_types}"
    
    # Re-run gold transformation to capture DELETE event
    run_gold_transformation()
    
    # Verify Gold - deleted count increased
    gold_deleted = duck_conn.execute(
        "SELECT SUM(articles_deleted) FROM gold.article_daily_metrics"
    ).fetchone()[0]
    assert gold_deleted >= 1, f"Gold deleted should be >= 1, got {gold_deleted}"
    
    print("  ✓ Hard delete removes from Silver, preserves in Bronze, updates Gold")


def test_update_scenario(pg_conn, duck_conn):
    """Test 4: UPDATE scenario - Silver updated, event captured."""
    print("Test 4: UPDATE handling...")
    
    test_id = 999901
    
    # Insert test article
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.articles (id, title, content, published_at, author_id, created_at, updated_at, deleted_at)
            VALUES (%s, 'Original Title', 'Content', NOW(), 999, NOW(), NOW(), NULL)
        """, (test_id,))
        pg_conn.commit()
    
    process_batch(pg_conn, duck_conn, batch_size=10)
    
    # Verify original
    silver_title = duck_conn.execute("SELECT title FROM silver.articles_current WHERE id = ?", (test_id,)).fetchone()[0]
    assert silver_title == 'Original Title', f"Expected 'Original Title', got {silver_title}"
    
    # Update
    with pg_conn.cursor() as cur:
        cur.execute("UPDATE public.articles SET title = 'Updated Title', updated_at = NOW() WHERE id = %s", (test_id,))
        pg_conn.commit()
    
    process_batch(pg_conn, duck_conn, batch_size=10)
    
    # Verify updated
    silver_title = duck_conn.execute("SELECT title FROM silver.articles_current WHERE id = ?", (test_id,)).fetchone()[0]
    assert silver_title == 'Updated Title', f"Expected 'Updated Title', got {silver_title}"
    
    # Verify Bronze has UPDATE event
    bronze_events = duck_conn.execute(
        "SELECT event_type FROM bronze.article_events WHERE article_id = ? AND event_type = 'UPDATE'", (test_id,)
    ).fetchall()
    assert len(bronze_events) == 1, f"Expected 1 UPDATE event, got {len(bronze_events)}"
    
    print("  ✓ UPDATE updates Silver and captures event")


def test_noop_update_no_event(pg_conn, duck_conn):
    """Test 5: No-op UPDATE creates no event."""
    print("Test 5: No-op UPDATE creates no event...")
    
    test_id = 999902
    
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.articles (id, title, content, published_at, author_id, created_at, updated_at, deleted_at)
            VALUES (%s, 'No-op Test', 'Content', NOW(), 999, NOW(), NOW(), NULL)
        """, (test_id,))
        pg_conn.commit()
    
    process_batch(pg_conn, duck_conn, batch_size=10)
    
    # Count events before no-op update
    events_before = duck_conn.execute(
        "SELECT COUNT(*) FROM bronze.article_events WHERE article_id = ?", (test_id,)
    ).fetchone()[0]
    
    # Update with same value
    with pg_conn.cursor() as cur:
        cur.execute("UPDATE public.articles SET title = 'No-op Test' WHERE id = %s", (test_id,))
        pg_conn.commit()
    
    process_batch(pg_conn, duck_conn, batch_size=10)
    
    events_after = duck_conn.execute(
        "SELECT COUNT(*) FROM bronze.article_events WHERE article_id = ?", (test_id,)
    ).fetchone()[0]
    
    assert events_after == events_before, f"Events increased from {events_before} to {events_after} on no-op UPDATE"
    
    print("  ✓ No-op UPDATE creates no event")


def test_trigger_ready_scenarios(pg_conn, duck_conn):
    """Test 6: Trigger scenarios ready for testing."""
    print("Test 6: Trigger scenarios...")
    
    # Scenario 1: INSERT
    test_id = 999910
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.articles (id, title, content, published_at, author_id, created_at, updated_at, deleted_at)
            VALUES (%s, 'Trigger INSERT', 'Content', NOW(), 999, NOW(), NOW(), NULL)
        """, (test_id,))
        pg_conn.commit()
    
    with pg_conn.cursor() as cur:
        cur.execute("SELECT event_type FROM public.article_event WHERE article_id = %s ORDER BY event_at DESC LIMIT 1", (test_id,))
        event = cur.fetchone()
        assert event and event['event_type'] == 'INSERT', f"Expected INSERT event, got {event}"
    
    # Scenario 2: UPDATE
    test_id = 999911
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.articles (id, title, content, published_at, author_id, created_at, updated_at, deleted_at)
            VALUES (%s, 'Trigger UPDATE', 'Content', NOW(), 999, NOW(), NOW(), NULL)
        """, (test_id,))
        pg_conn.commit()
    
    with pg_conn.cursor() as cur:
        cur.execute("UPDATE public.articles SET title = 'Updated' WHERE id = %s", (test_id,))
        pg_conn.commit()
    
    with pg_conn.cursor() as cur:
        cur.execute("SELECT event_type FROM public.article_event WHERE article_id = %s AND event_type = 'UPDATE'", (test_id,))
        event = cur.fetchone()
        assert event, f"Expected UPDATE event for article {test_id}"
    
    # Scenario 3: DELETE
    test_id = 999912
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.articles (id, title, content, published_at, author_id, created_at, updated_at, deleted_at)
            VALUES (%s, 'Trigger DELETE', 'Content', NOW(), 999, NOW(), NOW(), NULL)
        """, (test_id,))
        pg_conn.commit()
    
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM public.articles WHERE id = %s", (test_id,))
        pg_conn.commit()
    
    with pg_conn.cursor() as cur:
        cur.execute("SELECT event_type, payload FROM public.article_event WHERE article_id = %s AND event_type = 'DELETE'", (test_id,))
        event = cur.fetchone()
        assert event, f"Expected DELETE event for article {test_id}"
        payload = event['payload']
        assert payload['title'] == 'Trigger DELETE', f"DELETE payload should have deleted article data, got {payload}"
    
    # Scenario 4: Transaction rollback leaves no event
    test_id = 999913
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.articles (id, title, content, published_at, author_id, created_at, updated_at, deleted_at)
                VALUES (%s, 'Rollback Test', 'Content', NOW(), 999, NOW(), NOW(), NULL)
            """, (test_id,))
            raise Exception("Intentional rollback")
    except:
        pg_conn.rollback()
    
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM public.article_event WHERE article_id = %s", (test_id,))
        count = cur.fetchone()['cnt']
        assert count == 0, f"Rollback left {count} events"
    
    print("  ✓ All trigger scenarios work")


def run_all_tests():
    """Run all Phase 5 tests."""
    print("=" * 60)
    print("Phase 5 Integration Tests")
    print("=" * 60)
    
    pg_conn = get_pg_conn()
    duck_conn = get_duck_conn()
    
    try:
        # Clean up any existing test data
        reset_test_data(pg_conn, duck_conn)
        
        test_bronze_silver_all_active(pg_conn, duck_conn)
        test_gold_consistency(pg_conn, duck_conn)
        test_hard_delete_scenario(pg_conn, duck_conn)
        test_update_scenario(pg_conn, duck_conn)
        test_noop_update_no_event(pg_conn, duck_conn)
        test_trigger_ready_scenarios(pg_conn, duck_conn)
        
        print("\n" + "=" * 60)
        print("✓ ALL PHASE 5 TESTS PASSED")
        print("=" * 60)
        return True
        
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        return False
    except Exception as e:
        print(f"\n✗ TEST ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        pg_conn.close()
        duck_conn.close()


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)