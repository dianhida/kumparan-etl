
"""
Airflow DAG: articles_pipeline_reset
Manually triggered full reset of warehouse and event state.
Preserves PostgreSQL articles source table.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, "/opt/airflow/scripts")


def reset_warehouse(**context):
    import duckdb
    conn = duckdb.connect("/opt/airflow/data/warehouse.duckdb")
    conn.execute("DELETE FROM bronze.articles_snapshot WHERE is_initial_snapshot = TRUE")
    conn.execute("DELETE FROM bronze.article_events")
    conn.execute("DELETE FROM silver.articles_current")
    conn.execute("DELETE FROM gold.article_daily_metrics")
    conn.execute("DELETE FROM metadata.processed_events")
    conn.execute("""
        UPDATE metadata.pipeline_state 
        SET current_phase = 'INITIALIZING', 
            status_message = 'Reset for full load',
            loaded_count = 0,
            last_article_id = 0,
            snapshot_batch_id = NULL,
            updated_at = CURRENT_TIMESTAMP,
            completed_at = NULL
        WHERE pipeline_id = 'articles_pipeline'
    """)
    conn.commit()
    conn.close()
    print("✓ DuckDB warehouse reset complete")
    return "Warehouse reset complete"


def reset_postgres_events(**context):
    import psycopg
    from psycopg.rows import dict_row
    import os
    
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "kumparan"),
        password=os.getenv("POSTGRES_PASSWORD", "kumparan"),
        dbname=os.getenv("POSTGRES_DB", "kumparan"),
        row_factory=dict_row,
    )
    
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.article_event")
        conn.commit()
        print(f"✓ PostgreSQL article_event table cleared ({cur.rowcount} rows deleted)")
    
    conn.close()
    return "PostgreSQL events reset complete"


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=10),
}

dag = DAG(
    "articles_pipeline_reset",
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "start_date": datetime(2026, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "execution_timeout": timedelta(minutes=10),
    },
    description="Reset warehouse and event state - preserves PostgreSQL articles source",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["kumparan", "reset", "maintenance"],
)

def reset_warehouse(**context):
    import duckdb
    conn = duckdb.connect("/opt/airflow/data/warehouse.duckdb")
    conn.execute("DELETE FROM bronze.articles_snapshot WHERE is_initial_snapshot = TRUE")
    conn.execute("DELETE FROM bronze.article_events")
    conn.execute("DELETE FROM silver.articles_current")
    conn.execute("DELETE FROM gold.article_daily_metrics")
    conn.execute("DELETE FROM metadata.processed_events")
    conn.execute("""
        UPDATE metadata.pipeline_state 
        SET current_phase = 'INITIALIZING', 
            status_message = 'Reset for full load',
            loaded_count = 0,
            last_article_id = 0,
            snapshot_batch_id = NULL,
            updated_at = CURRENT_TIMESTAMP,
            completed_at = NULL
        WHERE pipeline_id = 'articles_pipeline'
    """)
    conn.commit()
    conn.close()
    print("✓ DuckDB warehouse reset complete")
    return "Warehouse reset complete"


def reset_postgres_events(**context):
    import psycopg
    from psycopg.rows import dict_row
    import os
    
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "kumparan"),
        password=os.getenv("POSTGRES_PASSWORD", "kumparan"),
        dbname=os.getenv("POSTGRES_DB", "kumparan"),
        row_factory=dict_row,
    )
    
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.article_event")
        conn.commit()
        print(f"✓ PostgreSQL article_event table cleared ({cur.rowcount} rows deleted)")
    
    conn.close()
    return "PostgreSQL events reset complete"


from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    "articles_pipeline_reset",
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "start_date": datetime(2026, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "execution_timeout": timedelta(minutes=10),
    },
    description="Reset warehouse and event state - preserves PostgreSQL articles source",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["kumparan", "reset", "maintenance"],
)

reset_warehouse_task = PythonOperator(
    task_id="reset_warehouse",
    python_callable=reset_warehouse,
    dag=dag,
)

reset_postgres_events_task = PythonOperator(
    task_id="reset_postgres_events",
    python_callable=reset_postgres_events,
    dag=dag,
)

reset_warehouse_task >> reset_postgres_events_task
