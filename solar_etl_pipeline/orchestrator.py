# filename: orchestrator.py
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ingestion_service.ingestion_adaptor import ingest_data
from curation_service.curation_processor import curate_data
from transformation_service.transformation_logic import transform_data
from visualization_service.visualization_generator import visualize_data

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'solar_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for solar sensor data',
    schedule_interval=timedelta(days=1),
)

# Define the tasks/operators
t1 = PythonOperator(
    task_id='ingest',
    python_callable=ingest_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='curate',
    python_callable=curate_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

t4 = PythonOperator(
    task_id='visualize',
    python_callable=visualize_data,
    dag=dag,
)

# Set the task sequence
t1 >> t2 >> t3 >> t4