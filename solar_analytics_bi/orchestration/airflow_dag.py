# filename: orchestration/airflow_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from data_ingestion.ingestor import DataIngestionService
from data_curation.curator import DataCurationService
from data_transformation.transformer import DataTransformationService
from data_visualization.visualizer import DataVisualizationService
from storage_connector import AzureDataLakeStorageConnector

# Instantiate the storage connector
storage_connector = AzureDataLakeStorageConnector(account_name='your_account_name', account_key='your_account_key')

# Instantiate the services
ingestion_service = DataIngestionService(storage_connector)
curation_service = DataCurationService(storage_connector)
transformation_service = DataTransformationService(storage_connector)
visualization_service = DataVisualizationService(storage_connector)

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

dag = DAG(
    'solar_analytics_bi_dag',
    default_args=default_args,
    description='DAG for Solar Analytics BI data pipeline',
    schedule_interval=timedelta(days=1),
)

def ingest_task(**kwargs):
    ingestion_service.ingest_data('/project_name/data/solar_sensors.csv')

def curate_task(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='ingest_task')
    curation_service.curate_data(raw_data)

def transform_task(**kwargs):
    curated_data = kwargs['ti'].xcom_pull(task_ids='curate_task')
    transformation_service.transform_data(curated_data)

def visualize_task(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_task')
    visualization_service.visualize_data(transformed_data)

ingest = PythonOperator(
    task_id='ingest_task',
    python_callable=ingest_task,
    provide_context=True,
    dag=dag,
)

curate = PythonOperator(
    task_id='curate_task',
    python_callable=curate_task,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

visualize = PythonOperator(
    task_id='visualize_task',
    python_callable=visualize_task,
    provide_context=True,
    dag=dag,
)

ingest >> curate >> transform >> visualize