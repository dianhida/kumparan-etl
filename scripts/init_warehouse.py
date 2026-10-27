#!/usr/bin/env python3
"""Initialize DuckDB warehouse schemas.

This script creates the Bronze, Silver, Gold, and Metadata schemas
and their tables in the configured DuckDB database file.
"""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb


def get_duckdb_path():
    """Get DuckDB path from environment."""
    load_dotenv()
    duckdb_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    # Ensure absolute path
    if not os.path.isabs(duckdb_path):
        project_root = Path(__file__).parent.parent
        duckdb_path = project_root / duckdb_path
    return str(duckdb_path)


def initialize_warehouse(duckdb_path: str):
    """Initialize warehouse schemas and tables."""
    print(f"Initializing DuckDB warehouse at: {duckdb_path}")

    # Ensure data directory exists
    os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)

    # Connect to DuckDB
    conn = duckdb.connect(duckdb_path)
    print("✓ Connected to DuckDB")

    try:
        # UUID is built-in in DuckDB 0.8+, no extension needed
        # Verify uuid() works
        conn.execute("SELECT uuid()")
        print("✓ UUID function available")

        # Read and execute master SQL file
        sql_dir = Path(__file__).parent.parent / "sql"
        schema_files = [
            "04_bronze_schema.sql",
            "05_silver_schema.sql",
            "06_gold_schema.sql",
            "07_metadata_schema.sql",
        ]

        for schema_file in schema_files:
            file_path = sql_dir / schema_file
            if not file_path.exists():
                raise FileNotFoundError(f"SQL file not found: {file_path}")

            with open(file_path, 'r') as f:
                sql_content = f.read()

            conn.execute(sql_content)
            print(f"  ✓ Executed {schema_file}")

        # Verify with validation query
        result = conn.execute("""
            SELECT 'bronze.articles_snapshot' as table_name, count(*) as row_count FROM bronze.articles_snapshot
            UNION ALL
            SELECT 'bronze.article_events', count(*) FROM bronze.article_events
            UNION ALL
            SELECT 'silver.articles_current', count(*) FROM silver.articles_current
            UNION ALL
            SELECT 'gold.article_daily_metrics', count(*) FROM gold.article_daily_metrics
            UNION ALL
            SELECT 'metadata.pipeline_state', count(*) FROM metadata.pipeline_state
            UNION ALL
            SELECT 'metadata.processed_events', count(*) FROM metadata.processed_events
        """).fetchall()

        print("\nTable row counts:")
        for table_name, row_count in result:
            print(f"  {table_name}: {row_count}")

        print("✓ Warehouse schemas and tables created")

        # Verify column structure for key tables
        print("\nVerifying column structures...")

        # Check bronze.articles_snapshot
        cols = conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'bronze' AND table_name = 'articles_snapshot'
            ORDER BY ordinal_position
        """).fetchall()
        print(f"  bronze.articles_snapshot: {len(cols)} columns")
        for col_name, col_type in cols:
            print(f"    {col_name}: {col_type}")

        # Check bronze.article_events
        cols = conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'bronze' AND table_name = 'article_events'
            ORDER BY ordinal_position
        """).fetchall()
        print(f"  bronze.article_events: {len(cols)} columns")
        for col_name, col_type in cols:
            print(f"    {col_name}: {col_type}")

        # Check silver.articles_current
        cols = conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'silver' AND table_name = 'articles_current'
            ORDER BY ordinal_position
        """).fetchall()
        print(f"  silver.articles_current: {len(cols)} columns")

        # Check gold.article_daily_metrics
        cols = conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'gold' AND table_name = 'article_daily_metrics'
            ORDER BY ordinal_position
        """).fetchall()
        print(f"  gold.article_daily_metrics: {len(cols)} columns")

        # Check metadata.processed_events has unique key on event_id
        pk = conn.execute("""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = 'metadata' AND table_name = 'processed_events'
        """).fetchall()
        print(f"  metadata.processed_events primary key: {[c[0] for c in pk]}")

        # Check pipeline_state initial state
        state = conn.execute("SELECT pipeline_id, current_phase FROM metadata.pipeline_state").fetchall()
        print(f"  metadata.pipeline_state: {state}")

        print("\n✓ Warehouse initialization completed successfully")
        return True

    except Exception as e:
        print(f"\n✗ Warehouse initialization failed: {e}")
        raise
    finally:
        conn.close()


def main():
    try:
        duckdb_path = get_duckdb_path()
        initialize_warehouse(duckdb_path)
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()