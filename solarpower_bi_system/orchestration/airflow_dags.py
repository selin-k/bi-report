# filename: orchestration/airflow_dags.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from data_ingestion.ingest_data import DataIngestionService
from data_curation.curate_data import DataCurationService
from data_transformation.transform_data import DataTransformationService
from data_visualization.visualize_data import DataVisualizationService
from storage.azure_storage_connector import AzureDataLakeStorageConnector

# These would typically be stored in a secure location and retrieved as needed
STORAGE_ACCOUNT_NAME = 'your_storage_account_name'
STORAGE_ACCOUNT_KEY = 'your_storage_account_key'
CONTAINER_NAME = 'your_container_name'

# Instantiate services
storage_connector = AzureDataLakeStorageConnector(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
ingestion_service = DataIngestionService(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, CONTAINER_NAME)
curation_service = DataCurationService(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, CONTAINER_NAME)
transformation_service = DataTransformationService(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, CONTAINER_NAME)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'solarpower_bi_system',
    default_args=default_args,
    description='DAG for Solar Power BI System',
    schedule_interval=timedelta(days=1),
)

def ingest_task(**kwargs):
    # Path to the CSV file to be ingested
    source_path = '/project_name/data/solar_sensors.csv'
    raw_data = ingestion_service.ingest_data(source_path)
    kwargs['ti'].xcom_push(key='raw_data', value=raw_data)

def curate_task(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='ingest_task')
    curated_data = curation_service.curate_data(raw_data)
    ti.xcom_push(key='curated_data', value=curated_data)

def transform_task(**kwargs):
    ti = kwargs['ti']
    curated_data = ti.xcom_pull(key='curated_data', task_ids='curate_task')
    transformed_data = transformation_service.transform_data(curated_data)
    ti.xcom_push(key='transformed_data', value=transformed_data)

def visualize_task(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_task')
    visualizations = DataVisualizationService.visualize_data(transformed_data)
    # Here you would typically save or display the visualizations

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