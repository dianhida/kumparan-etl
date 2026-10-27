#!/usr/bin/env python3
"""Verify DuckDB installation and basic functionality."""

import sys
import duckdb
import os


def verify_duckdb():
    """Verify DuckDB can be imported and used."""
    print("Verifying DuckDB...")

    # 1. Import DuckDB
    print(f"  DuckDB version: {duckdb.__version__}")

    # 2. Create a local DuckDB database file
    db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'test_duckdb.duckdb')
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = duckdb.connect(db_path)
    print(f"  Connected to DuckDB at: {db_path}")

    # 3. Create and query a temporary test table
    conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO test_table VALUES (1, 'test'), (2, 'verify')")
    result = conn.execute("SELECT * FROM test_table").fetchall()
    print(f"  Query result: {result}")

    # Verify the data
    assert result == [(1, 'test'), (2, 'verify')], "Data mismatch"
    print("  Data verification: PASSED")

    # 4. Close the connection cleanly
    conn.close()
    print("  Connection closed cleanly")

    # Clean up test file
    os.remove(db_path)
    print("  Test database file cleaned up")

    print("\n✓ DuckDB verification PASSED")
    return True


if __name__ == "__main__":
    try:
        verify_duckdb()
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ DuckDB verification FAILED: {e}")
        sys.exit(1)