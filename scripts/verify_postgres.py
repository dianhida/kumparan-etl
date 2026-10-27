#!/usr/bin/env python3
"""Verify PostgreSQL connectivity using environment-based configuration."""

import sys
import os
from dotenv import load_dotenv
import psycopg


def verify_postgres():
    """Verify PostgreSQL connection."""
    print("Verifying PostgreSQL connectivity...")

    # Load environment variables
    load_dotenv()

    # Get configuration from environment
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    user = os.getenv("POSTGRES_USER", "kumparan")
    password = os.getenv("POSTGRES_PASSWORD", "kumparan")
    database = os.getenv("POSTGRES_DB", "kumparan")

    print(f"  Connecting to: {host}:{port}/{database} as {user}")

    # 1. Establish a connection
    conn = psycopg.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database,
        connect_timeout=10
    )
    print("  Connection established")

    # 2. Execute a simple query
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        print(f"  PostgreSQL version: {version}")

    # 3. Print successful connection result
    print("  Query executed successfully")

    # 4. Close the connection reliably
    conn.close()
    print("  Connection closed")

    print("\n✓ PostgreSQL connectivity verification PASSED")
    return True


if __name__ == "__main__":
    try:
        verify_postgres()
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ PostgreSQL verification FAILED: {e}")
        sys.exit(1)