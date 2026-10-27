#!/usr/bin/env python3
"""DuckDB file locking mechanism for cross-process concurrency control."""

import os
import fcntl
import time
import logging
import duckdb
from contextlib import contextmanager
from pathlib import Path
import tempfile


logger = logging.getLogger(__name__)


class DuckDBFileLock:
    """
    File-based lock for DuckDB database to prevent concurrent writers.
    
    Uses flock(2) on a lock file in /tmp (always writable).
    Works across processes and containers.
    """
    
    def __init__(self, duckdb_path: str, timeout: int = 300):
        """
        Initialize the lock.
        
        Args:
            duckdb_path: Path to the DuckDB database file
            timeout: Maximum seconds to wait for lock (default 5 minutes)
        """
        self.duckdb_path = Path(duckdb_path)
        # Use a lock file in the same directory as the database (writable by airflow user)
        self.lock_path = self.duckdb_path.with_suffix('.lock')
        self.timeout = timeout
        self._lock_file = None
    
    def acquire(self, blocking: bool = True) -> bool:
        """
        Acquire the lock.
        
        Args:
            blocking: If True, wait for lock. If False, return immediately.
            
        Returns:
            True if lock acquired, False if not (non-blocking mode).
        """
        self._lock_file = open(self.lock_path, 'w')
        
        try:
            if blocking:
                # Wait for lock with timeout
                start = time.time()
                while True:
                    try:
                        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        logger.info(f"Acquired DuckDB lock: {self.lock_path}")
                        return True
                    except BlockingIOError:
                        if time.time() - start > self.timeout:
                            raise TimeoutError(f"Could not acquire DuckDB lock within {self.timeout}s")
                        time.sleep(0.1)
            else:
                try:
                    fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    logger.info(f"Acquired DuckDB lock: {self.lock_path}")
                    return True
                except BlockingIOError:
                    return False
        except Exception:
            self._lock_file.close()
            self._lock_file = None
            raise
    
    def release(self):
        """Release the lock."""
        if self._lock_file:
            try:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                logger.info(f"Released DuckDB lock: {self.lock_path}")
            except Exception as e:
                logger.warning(f"Error releasing DuckDB lock: {e}")
            finally:
                self._lock_file.close()
                self._lock_file = None
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


@contextmanager
def duckdb_lock(duckdb_path: str, timeout: int = 300, read_only: bool = False):
    """
    Context manager for DuckDB file lock with connection.
    
    Usage:
        with duckdb_lock('/path/to/warehouse.duckdb') as conn:
            # conn is a DuckDB connection with lock held
            # ... do work
        # lock automatically released when exiting context
    """
    lock = DuckDBFileLock(duckdb_path, timeout)
    lock.acquire()
    conn = duckdb.connect(duckdb_path, read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()
        lock.release()


def get_locked_duckdb_connection(duckdb_path: str, timeout: int = 300, read_only: bool = False):
    """
    Get a DuckDB connection with file lock held.
    
    The caller is responsible for closing the connection AND releasing the lock.
    Returns (connection, lock) tuple.
    
    Usage:
        conn, lock = get_locked_duckdb_connection('/path/to/warehouse.duckdb')
        try:
            # ... do work
        finally:
            conn.close()
            lock.release()
    """
    lock = DuckDBFileLock(duckdb_path, timeout)
    lock.acquire()
    conn = duckdb.connect(duckdb_path, read_only=read_only)
    return conn, lock