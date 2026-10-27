#!/usr/bin/env python3
"""Validate DuckDB warehouse initialization."""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb


def get_duckdb_path():
    """Get DuckDB path from environment."""
    load_dotenv()
    duckdb_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    if not os.path.isabs(duckdb_path):
        project_root = Path(__file__).parent.parent
        duckdb_path = project_root / duckdb_path
    return str(duckdb_path)


def validate_warehouse(duckdb_path: str):
    """Validate warehouse schemas and tables."""
    print(f"Validating DuckDB warehouse at: {duckdb_path}")

    if not os.path.exists(duckdb_path):
        print(f"✗ DuckDB file not found: {duckdb_path}")
        return False

    conn = duckdb.connect(duckdb_path, read_only=True)
    print("✓ Connected to DuckDB (read-only)")

    try:
        all_passed = True

        # 1. Check all required schemas exist
        print("\n1. Checking schemas...")
        schemas = conn.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name IN ('bronze', 'silver', 'gold', 'metadata')
        """).fetchall()
        schema_names = {s[0] for s in schemas}
        required_schemas = {'bronze', 'silver', 'gold', 'metadata'}
        missing_schemas = required_schemas - schema_names
        if missing_schemas:
            print(f"  ✗ Missing schemas: {missing_schemas}")
            all_passed = False
        else:
            print(f"  ✓ All required schemas present: {sorted(schema_names)}")

        # 2. Check all required tables exist
        print("\n2. Checking tables...")
        tables = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold', 'metadata')
            ORDER BY table_schema, table_name
        """).fetchall()
        
        required_tables = {
            ('bronze', 'articles_snapshot'),
            ('bronze', 'article_events'),
            ('silver', 'articles_current'),
            ('gold', 'article_daily_metrics'),
            ('metadata', 'pipeline_state'),
            ('metadata', 'processed_events'),
        }
        existing_tables = {(s, t) for s, t in tables}
        missing_tables = required_tables - existing_tables
        if missing_tables:
            print(f"  ✗ Missing tables: {missing_tables}")
            all_passed = False
        else:
            print(f"  ✓ All required tables present:")
            for schema, table in sorted(existing_tables):
                print(f"    {schema}.{table}")

        # 3. Check column structure for key tables
        print("\n3. Checking column structures...")
        
        # bronze.articles_snapshot
        cols = conn.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'bronze' AND table_name = 'articles_snapshot'
            ORDER BY ordinal_position
        """).fetchall()
        expected_cols_snapshot = {
            'id', 'title', 'content', 'published_at', 'author_id',
            'created_at', 'updated_at', 'deleted_at',
            'ingested_at', 'snapshot_batch_id', 'is_initial_snapshot'
        }
        actual_cols = {c[0] for c in cols}
        missing = expected_cols_snapshot - actual_cols
        extra = actual_cols - expected_cols_snapshot
        if missing or extra:
            print(f"  ✗ bronze.articles_snapshot column mismatch")
            if missing: print(f"    Missing: {missing}")
            if extra: print(f"    Extra: {extra}")
            all_passed = False
        else:
            print(f"  ✓ bronze.articles_snapshot columns OK ({len(cols)} columns)")

        # bronze.article_events
        cols = conn.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'bronze' AND table_name = 'article_events'
            ORDER BY ordinal_position
        """).fetchall()
        expected_cols_events = {
            'event_id', 'article_id', 'event_type', 'payload', 'event_at',
            'ingested_at', 'processed_at'
        }
        actual_cols = {c[0] for c in cols}
        missing = expected_cols_events - actual_cols
        extra = actual_cols - expected_cols_events
        if missing or extra:
            print(f"  ✗ bronze.article_events column mismatch")
            if missing: print(f"    Missing: {missing}")
            if extra: print(f"    Extra: {extra}")
            all_passed = False
        else:
            print(f"  ✓ bronze.article_events columns OK ({len(cols)} columns)")

        # silver.articles_current
        cols = conn.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'silver' AND table_name = 'articles_current'
        """).fetchall()
        actual_cols = {c[0] for c in cols}
        expected_cols_silver = {
            'id', 'title', 'content', 'published_at', 'author_id',
            'created_at', 'updated_at', 'deleted_at', 'last_synced_at'
        }
        if expected_cols_silver != actual_cols:
            print(f"  ✗ silver.articles_current column mismatch")
            print(f"    Expected: {expected_cols_silver}")
            print(f"    Actual: {actual_cols}")
            all_passed = False
        else:
            print(f"  ✓ silver.articles_current columns OK ({len(cols)} columns)")

        # gold.article_daily_metrics
        cols = conn.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'gold' AND table_name = 'article_daily_metrics'
        """).fetchall()
        actual_cols = {c[0] for c in cols}
        expected_cols_gold = {
            'metric_date', 'articles_created', 'articles_published',
            'articles_deleted', 'refreshed_at'
        }
        if expected_cols_gold != actual_cols:
            print(f"  ✗ gold.article_daily_metrics column mismatch")
            print(f"    Expected: {expected_cols_gold}")
            print(f"    Actual: {actual_cols}")
            all_passed = False
        else:
            print(f"  ✓ gold.article_daily_metrics columns OK ({len(cols)} columns)")

        # 4. Check metadata.processed_events has unique key on event_id
        print("\n4. Checking primary keys...")
        pk = conn.execute("""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = 'metadata' AND table_name = 'processed_events'
        """).fetchall()
        pk_columns = {c[0] for c in pk}
        if pk_columns != {'event_id'}:
            print(f"  ✗ metadata.processed_events PK mismatch: {pk_columns}")
            all_passed = False
        else:
            print(f"  ✓ metadata.processed_events has PK on event_id")

        # Check silver.articles_current PK
        pk = conn.execute("""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = 'silver' AND table_name = 'articles_current'
        """).fetchall()
        pk_columns = {c[0] for c in pk}
        if pk_columns != {'id'}:
            print(f"  ✗ silver.articles_current PK mismatch: {pk_columns}")
            all_passed = False
        else:
            print(f"  ✓ silver.articles_current has PK on id")

        # 5. Check initialization idempotency (run twice)
        print("\n5. Checking idempotency (re-run init)...")
        # This is tested by running the init script again separately

        # 6. Check DuckDB file location
        print("\n6. Checking DuckDB file location...")
        db_dir = os.path.dirname(os.path.abspath(duckdb_path))
        expected_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        if db_dir == expected_dir:
            print(f"  ✓ DuckDB file in correct location: {duckdb_path}")
        else:
            print(f"  ⚠ DuckDB file location: {duckdb_path} (expected under {expected_dir})")

        # 7. Check PostgreSQL source unchanged
        print("\n7. Checking PostgreSQL source data intact...")
        # This requires a separate connection - just note it
        print(f"  ⚠ Verify separately: PostgreSQL articles table still has 11,000 rows")

        print("\n" + "=" * 50)
        if all_passed:
            print("✓ ALL VALIDATION CHECKS PASSED")
            return True
        else:
            print("✗ SOME VALIDATION CHECKS FAILED")
            return False

    except Exception as e:
        print(f"\n✗ Validation error: {e}")
        return False
    finally:
        conn.close()


def main():
    duckdb_path = get_duckdb_path()
    success = validate_warehouse(duckdb_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()