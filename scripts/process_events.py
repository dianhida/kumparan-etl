#!/usr/bin/env python3
"""Event processor: reads events from PostgreSQL and applies to DuckDB."""

import sys
import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
import duckdb


# Load environment variables
load_dotenv()


def get_postgres_connection():
    """Get PostgreSQL connection from environment."""
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "kumparan"),
        password=os.getenv("POSTGRES_PASSWORD", "kumparan"),
        dbname=os.getenv("POSTGRES_DB", "kumparan"),
        connect_timeout=10,
        row_factory=dict_row,
    )


def get_duckdb_connection():
    """Get DuckDB connection from environment."""
    duckdb_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    if not os.path.isabs(duckdb_path):
        project_root = os.path.dirname(os.path.dirname(__file__))
        duckdb_path = os.path.join(project_root, duckdb_path)
    return duckdb.connect(duckdb_path)


def claim_events(pg_conn, batch_size=100):
    """
    Claim QUEUED events for processing.
    Uses FOR UPDATE SKIP LOCKED to safely claim events for a single worker.
    Updates status to PROCESSING and sets claimed_at.
    Returns list of claimed events.
    """
    with pg_conn.cursor() as cur:
        # Claim events atomically
        cur.execute("""
            UPDATE public.article_event
            SET status = 'PROCESSING', claimed_at = NOW()
            WHERE event_id IN (
                SELECT event_id
                FROM public.article_event
                WHERE status = 'QUEUED'
                ORDER BY event_at
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING event_id, article_id, event_type, payload, event_at
        """, (batch_size,))
        claimed = cur.fetchall()
        pg_conn.commit()
        return claimed


def release_stale_events(pg_conn, stale_minutes=5, max_retries=3):
    """
    Release events stuck in PROCESSING (e.g., from crashed worker).
    Resets them to QUEUED for retry.
    Also retries FAILED events that haven't exceeded max_retries.
    """
    with pg_conn.cursor() as cur:
        # Release stale PROCESSING events
        cur.execute("""
            UPDATE public.article_event
            SET status = 'QUEUED', claimed_at = NULL, retry_count = retry_count + 1
            WHERE status = 'PROCESSING'
            AND claimed_at < NOW() - INTERVAL '%s minutes'
        """, (stale_minutes,))
        released = cur.rowcount
        
        # Retry FAILED events that haven't exceeded max retries
        cur.execute("""
            UPDATE public.article_event
            SET status = 'QUEUED', claimed_at = NULL, python_msg = NULL
            WHERE status = 'FAILED'
            AND retry_count < %s
            AND python_msg IS NOT NULL
        """, (max_retries,))
        retried = cur.rowcount
        
        total = released + retried
        pg_conn.commit()
        if total > 0:
            print(f"  Released {released} stale events, retried {retried} failed events back to QUEUED")
        return total


def is_event_processed(duck_conn, event_id):
    """Check if event is already processed (idempotency checkpoint)."""
    result = duck_conn.execute(
        "SELECT 1 FROM metadata.processed_events WHERE event_id = ?",
        (str(event_id),)
    ).fetchone()
    return result is not None


def apply_event_to_duckdb(duck_conn, event):
    """
    Apply a single event to DuckDB within a transaction.
    - Bronze: insert event record
    - Silver: INSERT/UPDATE upsert, DELETE removes row
    - Metadata: checkpoint event_id
    All in one transaction for atomicity.
    """
    event_id = str(event['event_id'])
    article_id = event['article_id']
    event_type = event['event_type']
    payload = event['payload']
    event_at = event['event_at']

    # Parse payload (JSONB from PostgreSQL comes as dict via psycopg)
    if isinstance(payload, str):
        payload = json.loads(payload)

    # Extract fields from payload
    # Payload contains all article columns from NEW/OLD row
    title = payload.get('title')
    content = payload.get('content')
    published_at = payload.get('published_at')
    author_id = payload.get('author_id')
    created_at = payload.get('created_at')
    updated_at = payload.get('updated_at')
    deleted_at = payload.get('deleted_at')

    # Begin transaction
    duck_conn.execute("BEGIN TRANSACTION")

    try:
        now = datetime.now(timezone.utc)

        # 1. Insert into bronze.article_events (preserve history)
        duck_conn.execute("""
            INSERT INTO bronze.article_events (event_id, article_id, event_type, payload, event_at, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (event_id, article_id, event_type, json.dumps(payload), event_at, now))

        # 2. Apply to silver.articles_current
        if event_type == 'INSERT':
            duck_conn.execute("""
                INSERT INTO silver.articles_current (id, title, content, published_at, author_id, created_at, updated_at, deleted_at, last_synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    published_at = EXCLUDED.published_at,
                    author_id = EXCLUDED.author_id,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    deleted_at = EXCLUDED.deleted_at,
                    last_synced_at = EXCLUDED.last_synced_at
            """, (article_id, title, content, published_at, author_id, created_at, updated_at, deleted_at, now))

        elif event_type == 'UPDATE':
            duck_conn.execute("""
                INSERT INTO silver.articles_current (id, title, content, published_at, author_id, created_at, updated_at, deleted_at, last_synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    published_at = EXCLUDED.published_at,
                    author_id = EXCLUDED.author_id,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    deleted_at = EXCLUDED.deleted_at,
                    last_synced_at = EXCLUDED.last_synced_at
            """, (article_id, title, content, published_at, author_id, created_at, updated_at, deleted_at, now))

        elif event_type == 'DELETE':
            # Hard delete: physically remove from silver
            duck_conn.execute("""
                DELETE FROM silver.articles_current WHERE id = ?
            """, (article_id,))

        # 3. Checkpoint in metadata.processed_events
        duck_conn.execute("""
            INSERT INTO metadata.processed_events (event_id, processed_at)
            VALUES (?, ?)
            ON CONFLICT (event_id) DO NOTHING
        """, (event_id, now))

        # Commit transaction
        duck_conn.execute("COMMIT")
        return True, None

    except Exception as e:
        duck_conn.execute("ROLLBACK")
        return False, str(e)


def mark_event_status(pg_conn, event_id, status, error_msg=None):
    """Update event processing status in PostgreSQL."""
    with pg_conn.cursor() as cur:
        if status == 'SUCCEEDED':
            cur.execute("""
                UPDATE public.article_event
                SET status = 'SUCCEEDED', processed_at = NOW()
                WHERE event_id = %s
            """, (str(event_id),))
        elif status == 'FAILED':
            cur.execute("""
                UPDATE public.article_event
                SET status = 'FAILED', python_msg = %s, retry_count = retry_count + 1
                WHERE event_id = %s
            """, (error_msg, str(event_id)))
        pg_conn.commit()


def process_batch(pg_conn, duck_conn, batch_size=100):
    """Process a batch of events."""
    # Release any stale events first
    release_stale_events(pg_conn)

    # Claim events
    events = claim_events(pg_conn, batch_size)
    if not events:
        return 0, 0

    print(f"  Claimed {len(events)} events for processing")

    succeeded = 0
    failed = 0

    for event in events:
        event_id = event['event_id']

        # Idempotency check
        if is_event_processed(duck_conn, event_id):
            print(f"    Event {event_id} already processed, skipping")
            mark_event_status(pg_conn, event_id, 'SUCCEEDED')
            succeeded += 1
            continue

        # Apply to DuckDB
        success, error = apply_event_to_duckdb(duck_conn, event)

        if success:
            mark_event_status(pg_conn, event_id, 'SUCCEEDED')
            succeeded += 1
        else:
            mark_event_status(pg_conn, event_id, 'FAILED', error)
            failed += 1
            print(f"    ✗ Failed to process event {event_id}: {error}")

    return succeeded, failed


def main():
    print("=" * 60)
    print("Phase 4: Event Processor")
    print("=" * 60)

    batch_size = int(os.getenv("EVENT_BATCH_SIZE", "100"))

    # Connect to PostgreSQL
    try:
        pg_conn = get_postgres_connection()
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

    # Connect to DuckDB
    try:
        duck_conn = get_duckdb_connection()
        print("✓ Connected to DuckDB")
    except Exception as e:
        print(f"✗ Failed to connect to DuckDB: {e}")
        pg_conn.close()
        sys.exit(1)

    try:
        total_succeeded = 0
        total_failed = 0

        while True:
            succeeded, failed = process_batch(pg_conn, duck_conn, batch_size)
            total_succeeded += succeeded
            total_failed += failed

            if succeeded == 0 and failed == 0:
                print("  No more events to process")
                break

            # Small delay between batches (as per spec: 10 seconds)
            # Using shorter delay for testing
            import time
            time.sleep(0.5)

        print(f"\n✓ Processing complete: {total_succeeded} succeeded, {total_failed} failed")

        # Summary
        with pg_conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM public.article_event GROUP BY status")
            for row in cur.fetchall():
                print(f"  {row['status']}: {row['count']}")

    except KeyboardInterrupt:
        print("\n⚠ Interrupted by user")
    except Exception as e:
        print(f"\n✗ Processing failed: {e}")
        sys.exit(1)
    finally:
        pg_conn.close()
        duck_conn.close()


if __name__ == "__main__":
    main()