# filename: orchestration/orchestrate_api.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from ..data_ingestion.ingest_data import DataIngestion
from ..data_curation.curate_data import DataCuration
from ..data_transformation.transform_data import DataTransformation
from ..data_visualization.visualize_data import DataVisualization
import config

# Initialize the classes
data_ingestion = DataIngestion()
data_curation = DataCuration()
data_transformation = DataTransformation()
data_visualization = DataVisualization()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['alerts@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'solar_performance_dashboard',
    default_args=default_args,
    description='A DAG to orchestrate the solar performance dashboard data pipeline',
    schedule_interval=config.ORCHESTRATION_SCHEDULE_INTERVAL,
    catchup=False
)

# Define the tasks
ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=data_ingestion.ingest_data,
    dag=dag,
)

curate_task = PythonOperator(
    task_id='curate_data',
    python_callable=data_curation.curate_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=data_transformation.transform_data,
    dag=dag,
)

visualize_task = PythonOperator(
    task_id='visualize_data',
    python_callable=data_visualization.visualize_data,
    dag=dag,
)

# Set the task dependencies
ingest_task >> curate_task >> transform_task >> visualize_task

# Note: The actual orchestration logic will depend on the specific requirements and data pipeline.