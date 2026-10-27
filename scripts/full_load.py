#!/usr/bin/env python3
"""Initial full load: reads all articles from PostgreSQL and populates warehouse.

Supports resumable execution with progress tracking.
Uses file locking to prevent concurrent access to DuckDB.
"""

import sys
import os
import json
import uuid
from pathlib import Path
from datetime import datetime, timezone

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
import duckdb

# Import our locking mechanism
from scripts.duckdb_lock import duckdb_lock


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
    """Get DuckDB connection from environment (without lock - use lock context manager)."""
    duckdb_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    if not os.path.isabs(duckdb_path):
        project_root = os.path.dirname(os.path.dirname(__file__))
        duckdb_path = os.path.join(project_root, duckdb_path)
    return duckdb.connect(duckdb_path)


def get_pipeline_state(duck_conn):
    """Get current pipeline state."""
    result = duck_conn.execute(
        "SELECT current_phase, status_message, snapshot_batch_id, loaded_count, last_article_id FROM metadata.pipeline_state WHERE pipeline_id = 'articles_pipeline'"
    ).fetchone()
    if result:
        return {
            "phase": result[0],
            "message": result[1],
            "snapshot_batch_id": result[2],
            "loaded_count": result[3] if result[3] is not None else 0,
            "last_article_id": result[4] if result[4] is not None else 0,
        }
    return {"phase": "UNKNOWN", "message": "Not initialized", "snapshot_batch_id": None, "loaded_count": 0, "last_article_id": 0}


def update_pipeline_state(duck_conn, phase, message=None, snapshot_batch_id=None, loaded_count=None, last_article_id=None):
    """Update pipeline state with progress tracking."""
    now = datetime.now(timezone.utc)
    
    # Build dynamic update query based on provided parameters
    updates = ["current_phase = ?", "updated_at = ?"]
    params = [phase, now]
    
    if message is not None:
        updates.append("status_message = ?")
        params.append(message)
    if snapshot_batch_id is not None:
        updates.append("snapshot_batch_id = ?")
        params.append(snapshot_batch_id)
    if loaded_count is not None:
        updates.append("loaded_count = ?")
        params.append(loaded_count)
    if last_article_id is not None:
        updates.append("last_article_id = ?")
        params.append(last_article_id)
    
    query = f"""
        UPDATE metadata.pipeline_state
        SET {', '.join(updates)}
        WHERE pipeline_id = 'articles_pipeline'
    """
    duck_conn.execute(query, params)


def check_snapshot_exists(duck_conn):
    """Check if initial snapshot already exists."""
    result = duck_conn.execute(
        "SELECT COUNT(*) FROM bronze.articles_snapshot WHERE is_initial_snapshot = TRUE"
    ).fetchone()
    return result[0] > 0


def get_total_article_count(pg_conn):
    """Get total article count from source."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM public.articles")
        return cur.fetchone()['cnt']


def load_snapshot_chunk(pg_conn, duck_conn, last_id, chunk_size, snapshot_batch_id, now):
    """Load a chunk of articles using keyset pagination."""
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT id, title, content, published_at, author_id, created_at, updated_at, deleted_at
            FROM public.articles
            WHERE id > %s
            ORDER BY id
            LIMIT %s
        """, (last_id, chunk_size))
        rows = cur.fetchall()

    if not rows:
        return 0, last_id

    snapshot_rows = []
    silver_rows = []

    for row in rows:
        snapshot_rows.append((
            row['id'],
            row['title'],
            row['content'],
            row['published_at'],
            row['author_id'],
            row['created_at'],
            row['updated_at'],
            row['deleted_at'],
            now,
            snapshot_batch_id,
            True  # is_initial_snapshot
        ))
        if row['deleted_at'] is None:
            silver_rows.append((
                row['id'],
                row['title'],
                row['content'],
                row['published_at'],
                row['author_id'],
                row['created_at'],
                row['updated_at'],
                row['deleted_at'],
                now
            ))

    # Insert into bronze.articles_snapshot
    duck_conn.executemany("""
        INSERT INTO bronze.articles_snapshot
        (id, title, content, published_at, author_id, created_at, updated_at, deleted_at, ingested_at, snapshot_batch_id, is_initial_snapshot)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, snapshot_rows)

    # Insert into silver.articles_current (only active articles)
    if silver_rows:
        duck_conn.executemany("""
            INSERT INTO silver.articles_current
            (id, title, content, published_at, author_id, created_at, updated_at, deleted_at, last_synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, silver_rows)

    new_last_id = rows[-1]['id']
    loaded = len(rows)
    return loaded, new_last_id


def run_full_load(chunk_size=1000):
    """Run the initial full load with resumable progress tracking."""
    print("=" * 60)
    print("Phase 5: Initial Full Load")
    print("=" * 60)

    # Get DuckDB path for locking
    duckdb_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    if not os.path.isabs(duckdb_path):
        project_root = os.path.dirname(os.path.dirname(__file__))
        duckdb_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), duckdb_path)

    # Connect to PostgreSQL (outside lock - PG handles concurrency)
    try:
        pg_conn = get_postgres_connection()
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL: {e}")
        return False

    # Use file lock for DuckDB access
    try:
        duckdb_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
        with duckdb_lock(duckdb_path, timeout=300) as duck_conn:
            print("✓ Connected to DuckDB (with file lock)")

            try:
                # Check pipeline state
                state = get_pipeline_state(duck_conn)
                print(f"Current pipeline state: {state['phase']} - {state['message']}")

                # Resume logic: check if we can resume from a previous run
                resume = False
                snapshot_batch_id = state['snapshot_batch_id']
                loaded_count = state['loaded_count'] or 0
                last_article_id = state['last_article_id'] or 0

                if state['phase'] in ['FULL_LOAD_RUNNING', 'RECONCILING']:
                    if state['snapshot_batch_id'] and state['loaded_count'] > 0:
                        print(f"Resuming full load from previous run (batch: {state['snapshot_batch_id']}, loaded: {state['loaded_count']}, last_id: {state['last_article_id']})")
                        resume = True
                        snapshot_batch_id = state['snapshot_batch_id']
                        loaded_count = state['loaded_count'] or 0
                        last_article_id = state['last_article_id'] or 0
                        
                        # Clean up any partial data from the interrupted run
                        # Delete any snapshot records with id >= last_article_id for this batch
                        duck_conn.execute("""
                            DELETE FROM bronze.articles_snapshot 
                            WHERE snapshot_batch_id = ? AND id > ?
                        """, (snapshot_batch_id, last_article_id))
                        # Also clean up silver table for IDs that might have been partially inserted
                        duck_conn.execute("""
                            DELETE FROM silver.articles_current 
                            WHERE id > ?
                        """, (last_article_id,))
                        duck_conn.commit()
                        print(f"  Cleaned up partial data for IDs > {last_article_id}")
                    else:
                        print("Previous run was in progress but no progress data found. Starting fresh.")
                        resume = False

                if state['phase'] == 'READY':
                    if check_snapshot_exists(duck_conn):
                        print("⚠ Initial snapshot already exists and pipeline is READY.")
                        print("  To re-run full load, manually reset pipeline state and truncate snapshot tables.")
                        print("  This prevents accidental duplicate loads.")
                        return False

                if not resume:
                    # Fresh start - check if snapshot already exists
                    if check_snapshot_exists(duck_conn):
                        print("⚠ Initial snapshot already exists.")
                        print("  To re-run full load, manually reset pipeline state and truncate snapshot tables.")
                        print("  This prevents accidental duplicate loads.")
                        return False

                    # Generate new snapshot batch ID
                    snapshot_batch_id = str(uuid.uuid4())
                    loaded_count = 0
                    last_article_id = 0

                    # Get total count for progress tracking
                    total_count = get_total_article_count(pg_conn)
                    print(f"Snapshot batch ID: {snapshot_batch_id}")
                    print(f"Total articles to load: {total_count}")

                    # Update pipeline state to FULL_LOAD_RUNNING
                    update_pipeline_state(duck_conn, 'FULL_LOAD_RUNNING', 
                                        f'Loading snapshot batch {snapshot_batch_id}',
                                        snapshot_batch_id, 0, 0)
                    duck_conn.commit()
                else:
                    # Resuming - get total count
                    total_count = get_total_article_count(pg_conn)
                    print(f"Resuming: {loaded_count}/{total_count} articles loaded, last_id={last_article_id}")

                # Update pipeline state to FULL_LOAD_RUNNING
                update_pipeline_state(duck_conn, 'FULL_LOAD_RUNNING', 
                                    f'Loading snapshot batch {snapshot_batch_id}',
                                    snapshot_batch_id, loaded_count, last_article_id)
                duck_conn.commit()

                # Load snapshot in chunks
                last_id = last_article_id if resume else 0
                loaded_count = loaded_count if resume else 0

                print(f"\nLoading snapshot in chunks of {chunk_size}...")
                while True:
                    loaded, last_id = load_snapshot_chunk(
                        pg_conn, duck_conn, last_id, chunk_size, snapshot_batch_id, datetime.now(timezone.utc)
                    )
                    
                    if loaded == 0:
                        break

                    loaded_count += loaded
                    last_article_id = last_id

                    # Update progress every chunk
                    update_pipeline_state(duck_conn, 'FULL_LOAD_RUNNING',
                                        f'Loaded {loaded_count}/{get_total_article_count(pg_conn)} articles',
                                        snapshot_batch_id, loaded_count, last_id)
                    duck_conn.commit()

                    # For progress display, get current total
                    total_count = get_total_article_count(pg_conn)
                    print(f"  Loaded {loaded_count}/{total_count} articles (last_id={last_id})")

                print(f"\n✓ Snapshot load complete: {loaded_count} articles loaded")

                # Verify counts
                total_count = get_total_article_count(pg_conn)
                pg_count = total_count

                snap_count = duck_conn.execute(
                    "SELECT COUNT(*) AS cnt FROM bronze.articles_snapshot WHERE is_initial_snapshot = TRUE"
                ).fetchone()[0]

                silver_count = duck_conn.execute(
                    "SELECT COUNT(*) AS cnt FROM silver.articles_current"
                ).fetchone()[0]

                print(f"\nVerification:")
                print(f"  PostgreSQL articles: {pg_count}")
                print(f"  Bronze snapshot: {snap_count}")
                print(f"  Silver current: {silver_count}")

                if pg_count != snap_count:
                    print(f"  ⚠ Count mismatch! This may be due to concurrent writes during snapshot.")
                    update_pipeline_state(duck_conn, 'FAILED', f'Count mismatch: PG={pg_count}, Snapshot={snap_count}')
                    duck_conn.commit()
                    return False

                # Update pipeline state to RECONCILING
                update_pipeline_state(duck_conn, 'RECONCILING', 'Replaying captured events after snapshot')
                duck_conn.commit()

                # Replay captured events that occurred during/after snapshot
                print("\nReplaying captured events...")
                from scripts.process_events import process_batch
                total_processed = 0
                while True:
                    succeeded, failed = process_batch(pg_conn, duck_conn, batch_size=100)
                    total_processed += succeeded + failed
                    if succeeded == 0 and failed == 0:
                        break

                print(f"  Replayed {total_processed} events")

                # Verify reconciliation
                final_silver = duck_conn.execute(
                    "SELECT COUNT(*) AS cnt FROM silver.articles_current"
                ).fetchone()[0]
                print(f"  Silver after reconciliation: {final_silver}")

                # Update pipeline state to READY
                update_pipeline_state(duck_conn, 'READY', 
                                    f'Full load complete: {loaded_count} articles, {total_processed} events replayed')
                duck_conn.commit()

                print("\n✓ Full load completed successfully!")
                print(f"  Pipeline state: READY")
                return True

            except Exception as e:
                print(f"\n✗ Full load failed: {e}")
                import traceback
                traceback.print_exc()
                update_pipeline_state(duck_conn, 'FAILED', str(e))
                duck_conn.commit()
                return False

    except Exception as e:
        print(f"\n✗ Failed to acquire DuckDB lock or connect: {e}")
        return False
    finally:
        pg_conn.close()


def get_postgres_connection():
    """Get PostgreSQL connection from environment."""
    import psycopg
    from psycopg.rows import dict_row
    import os
    from dotenv import load_dotenv
    load_dotenv()
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "kumparan"),
        password=os.getenv("POSTGRES_PASSWORD", "kumparan"),
        dbname=os.getenv("POSTGRES_DB", "kumparan"),
        connect_timeout=10,
        row_factory=dict_row,
    )


def main():
    chunk_size = int(os.getenv("FULL_LOAD_CHUNK_SIZE", "1000"))
    success = run_full_load(chunk_size)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()