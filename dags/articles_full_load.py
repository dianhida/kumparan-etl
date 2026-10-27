"""
Airflow DAG: articles_full_load
Manually triggered initial historical load from PostgreSQL to DuckDB warehouse.
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
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'articles_full_load',
    default_args=default_args,
    description='Initial full load of articles from PostgreSQL to DuckDB warehouse',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['kumparan', 'full-load', 'warehouse'],
)


def check_pipeline_ready(**context):
    """Check if pipeline is in a state that allows full load."""
    import duckdb
    from datetime import timezone

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
    if phase == 'READY':
        raise Exception("Pipeline already READY - full load already completed")
    if phase in ['FULL_LOAD_RUNNING', 'RECONCILING']:
        raise Exception(f"Pipeline currently in {phase} - cannot start new full load")

    # Allow INITIALIZING or FAILED (for retry)
    print(f"Pipeline state: {phase} - proceeding with full load")
    return phase


def run_full_load(**context):
    """Run the initial full load."""
    from scripts.full_load import run_full_load

    chunk_size = int(os.getenv("FULL_LOAD_CHUNK_SIZE", "1000"))
    success = run_full_load(chunk_size)

    if not success:
        raise Exception("Full load failed")

    return "Full load completed successfully"


def run_gold_transformation(**context):
    """Run gold layer transformation after full load."""
    from scripts.run_gold import run_gold_transformation

    success = run_gold_transformation()

    if not success:
        raise Exception("Gold transformation failed")

    return "Gold transformation completed successfully"


def verify_pipeline_ready(**context):
    """Verify pipeline is READY after full load."""
    import duckdb

    duckdb_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/warehouse.duckdb")
    conn = duckdb.connect(duckdb_path, read_only=True)

    result = conn.execute("""
        SELECT current_phase, status_message FROM metadata.pipeline_state
        WHERE pipeline_id = 'articles_pipeline'
    """).fetchone()

    conn.close()

    if not result or result[0] != 'READY':
        raise Exception(f"Pipeline not READY after full load: {result}")

    print(f"Pipeline verified READY: {result[1]}")
    return "Pipeline verified READY"


# Task definitions
check_state = PythonOperator(
    task_id='check_pipeline_state',
    python_callable=check_pipeline_ready,
    dag=dag,
)

full_load = PythonOperator(
    task_id='run_full_load',
    python_callable=run_full_load,
    dag=dag,
)

gold_transform = PythonOperator(
    task_id='run_gold_transformation',
    python_callable=run_gold_transformation,
    dag=dag,
)

verify_ready = PythonOperator(
    task_id='verify_pipeline_ready',
    python_callable=verify_pipeline_ready,
    dag=dag,
)

# Task dependencies
check_state >> full_load >> gold_transform >> verify_ready