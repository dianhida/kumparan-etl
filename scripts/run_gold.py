#!/usr/bin/env python3
"""Run Gold layer transformation."""

import sys
import os
from dotenv import load_dotenv
import duckdb


# Load environment variables
load_dotenv()


def get_duckdb_connection():
    """Get DuckDB connection from environment."""
    duckdb_path = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
    if not os.path.isabs(duckdb_path):
        project_root = os.path.dirname(os.path.dirname(__file__))
        duckdb_path = os.path.join(project_root, duckdb_path)
    return duckdb.connect(duckdb_path)


def run_gold_transformation():
    """Run the gold transformation SQL."""
    print("=" * 60)
    print("Phase 5: Gold Transformation")
    print("=" * 60)

    try:
        duck_conn = get_duckdb_connection()
        print("✓ Connected to DuckDB")

        # Read and execute transformation SQL
        sql_file = os.path.join(os.path.dirname(__file__), '..', 'sql', '10_gold_transformation.sql')
        with open(sql_file, 'r') as f:
            sql_content = f.read()

        # Execute the entire SQL as a single statement (CTEs need to stay together)
        try:
            duck_conn.execute(sql_content)
            print("  Gold transformation executed")
        except Exception as e:
            print(f"  Statement failed: {e}")
            raise

        # Verify results
        count = duck_conn.execute("SELECT COUNT(*) FROM gold.article_daily_metrics").fetchone()[0]
        print(f"\n✓ Gold transformation complete: {count} metric dates")

        # Show sample
        sample = duck_conn.execute("""
            SELECT metric_date, articles_created, articles_published, articles_deleted, refreshed_at
            FROM gold.article_daily_metrics
            ORDER BY metric_date
            LIMIT 10
        """).fetchall()
        print("\nSample metrics:")
        for row in sample:
            print(f"  {row[0]}: created={row[1]}, published={row[2]}, deleted={row[3]}, refreshed={row[4]}")

        # Data quality checks
        print("\nData quality checks:")
        # Non-negative metrics
        neg_check = duck_conn.execute("""
            SELECT COUNT(*) FROM gold.article_daily_metrics
            WHERE articles_created < 0 OR articles_published < 0 OR articles_deleted < 0
        """).fetchone()[0]
        print(f"  Non-negative metrics: {'PASS' if neg_check == 0 else 'FAIL'} ({neg_check} violations)")

        # Unique metric_date
        dup_check = duck_conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT metric_date, COUNT(*) FROM gold.article_daily_metrics
                GROUP BY metric_date HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        print(f"  Unique metric_date: {'PASS' if dup_check == 0 else 'FAIL'} ({dup_check} duplicates)")

        # Silver has only active articles
        silver_deleted = duck_conn.execute("""
            SELECT COUNT(*) FROM silver.articles_current WHERE deleted_at IS NOT NULL
        """).fetchone()[0]
        print(f"  Silver only active: {'PASS' if silver_deleted == 0 else 'FAIL'} ({silver_deleted} deleted in Silver)")

        # No duplicate IDs in Silver
        dup_silver = duck_conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT id, COUNT(*) FROM silver.articles_current
                GROUP BY id HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        print(f"  No duplicate Silver IDs: {'PASS' if dup_silver == 0 else 'FAIL'} ({dup_silver} duplicates)")

        return True

    except Exception as e:
        print(f"\n✗ Gold transformation failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        duck_conn.close()


def main():
    success = run_gold_transformation()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()