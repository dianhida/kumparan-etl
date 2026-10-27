"""
Airflow DAG: articles_incremental_load
Scheduled hourly incremental load - processes queued events from PostgreSQL to DuckDB.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
import os

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/scripts')

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'execution_timeout': timedelta(minutes=50),  # Max 45 min processing + buffer
}

dag = DAG(
    'articles_incremental_load',
    default_args=default_args,
    description='Hourly incremental load - processes queued events from PostgreSQL to DuckDB',
    schedule_interval='0 * * * *',  # Hourly at minute 0
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['kumparan', 'incremental', 'warehouse', 'hourly'],
)


def check_pipeline_ready(**context):
    """Check if pipeline is READY before processing incrementals."""
    import duckdb

    duckdb_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/warehouse.duckdb")
    conn = duckdb.connect(duckdb_path, read_only=True)

    result = conn.execute("""
        SELECT current_phase FROM metadata.pipeline_state
        WHERE pipeline_id = 'articles_pipeline'
    """).fetchone()

    conn.close()

    if not result:
        raise Exception("Pipeline state not initialized")

    phase = result[0]
    if phase != 'READY':
        # Skip processing if not READY (e.g., full load not done)
        print(f"Pipeline state: {phase} - skipping incremental run (not READY)")
        # Raise to mark as skipped/failed
        raise Exception(f"Pipeline not READY (current: {phase}) - incremental load skipped")

    print(f"Pipeline state: {phase} - proceeding with incremental load")
    return phase


def run_incremental_load(**context):
    """Run incremental event processing with time limit."""
    from scripts.process_events import process_batch, get_postgres_connection
    from scripts.duckdb_lock import duckdb_lock
    import time
    import os
    import traceback
    import json

    batch_size = int(os.getenv("EVENT_BATCH_SIZE", "100"))
    max_runtime_seconds = int(os.getenv("INCREMENTAL_MAX_RUNTIME", "2700"))  # 45 minutes

    pg_conn = get_postgres_connection()

    try:
        # Use file lock for DuckDB access
        duckdb_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/warehouse.duckdb")
        with duckdb_lock(duckdb_path, timeout=60) as duck_conn:
            start_time = time.time()
            total_succeeded = 0
            total_failed = 0
            batch_num = 0

            print(f"Starting incremental load (max runtime: {max_runtime_seconds}s, batch size: {batch_size})")

            while True:
                # Check time limit
                elapsed = time.time() - start_time
                if elapsed >= max_runtime_seconds:
                    print(f"Max runtime ({max_runtime_seconds}s) reached, stopping")
                    break

                # Check if we should yield (Airflow may need to check for signals)
                remaining = max_runtime_seconds - elapsed
                if remaining < 30:  # Less than 30s remaining, don't start new batch
                    print(f"Only {remaining:.0f}s remaining, stopping early")
                    break

                succeeded, failed = process_batch(pg_conn, duck_conn, batch_size)
                total_succeeded += succeeded
                total_failed += failed
                batch_num += 1

                if succeeded == 0 and failed == 0:
                    print("No more events to process")
                    break

                # Small delay between batches (configurable, default 10s as per spec)
                batch_delay = int(os.getenv("INCREMENTAL_BATCH_DELAY", "10"))
                print(f"Batch {batch_num} complete: {succeeded} succeeded, {failed} failed. Waiting {batch_delay}s...")
                time.sleep(batch_delay)

            print(f"\nIncremental load complete: {total_succeeded} succeeded, {total_failed} failed in {batch_num} batches")

            # Get final event status summary
            with pg_conn.cursor() as cur:
                cur.execute("SELECT status, COUNT(*) FROM public.article_event GROUP BY status")
                for row in cur.fetchall():
                    print(f"  {row['status']}: {row['count']}")

            if total_failed > 0:
                raise Exception(f"Incremental load completed with {total_failed} failures")

            result = {
                'batches': batch_num,
                'succeeded': total_succeeded,
                'failed': total_failed,
                'runtime_seconds': str(round(time.time() - start_time, 3))
            }
            print(f"Returning result: {result}")
            # Test JSON serialization
            json.dumps(result)
            print(f"JSON serialization OK")
            return result

    except Exception as e:
        print(f"Error in run_incremental_load: {e}")
        traceback.print_exc()
        raise
    finally:
        try:
            pg_conn.close()
        except Exception as e:
            print(f"Warning: Error closing PostgreSQL connection: {e}")


def run_gold_transformation(**context):
    """Run gold layer transformation after incremental load."""
    from scripts.run_gold import run_gold_transformation

    success = run_gold_transformation()

    if not success:
        raise Exception("Gold transformation failed")

    return "Gold transformation completed successfully"


# Task definitions
check_state = PythonOperator(
    task_id='check_pipeline_ready',
    python_callable=check_pipeline_ready,
    dag=dag,
)

incremental_load = PythonOperator(
    task_id='run_incremental_load',
    python_callable=run_incremental_load,
    dag=dag,
)

gold_transform = PythonOperator(
    task_id='run_gold_transformation',
    python_callable=run_gold_transformation,
    dag=dag,
)

# Task dependencies
check_state >> incremental_load >> gold_transform