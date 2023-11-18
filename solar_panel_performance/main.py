# filename: main.py
from flask import Flask, request, jsonify
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from data-ingest.data_ingestion_service import DataIngestionService
from data-curate.data_curation_service import DataCurationService
from data-transformation.data_transformation_service import DataTransformationService
from data-visualization.data_visualization_service import DataVisualizationService

# Initialize Flask app
app = Flask(__name__)

# Airflow DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize services
config_path = 'config/config.yaml'
ingestion_service = DataIngestionService(config_path)
curation_service = DataCurationService(config_path)
transformation_service = DataTransformationService(config_path)
visualization_service = DataVisualizationService(config_path)

# Define the DAG
dag = DAG(
    'solar_panel_performance',
    default_args=default_args,
    description='DAG for Solar Panel Performance Data Pipeline',
    schedule_interval=timedelta(days=1),
)

# Define tasks
ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingestion_service.ingest_data,
    dag=dag,
)

curate_task = PythonOperator(
    task_id='curate_data',
    python_callable=curation_service.curate_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transformation_service.transform_data,
    dag=dag,
)

visualize_task = PythonOperator(
    task_id='create_visualizations',
    python_callable=visualization_service.create_visualizations,
    dag=dag,
)

# Set task dependencies
ingest_task >> curate_task >> transform_task >> visualize_task

# Flask API endpoints
@app.route('/ingest', methods=['POST'])
def ingest_data():
    data = request.get_json()
    ingestion_service.ingest_data(data['path'])
    return jsonify({'message': 'Data ingestion successful'}), 200

@app.route('/curate', methods=['GET'])
def curate_data():
    curation_service.curate_data()
    return jsonify({'message': 'Data curation successful'}), 200

@app.route('/transform', methods=['GET'])
def transform_data():
    transformation_service.transform_data()
    return jsonify({'message': 'Data transformation successful'}), 200

@app.route('/dashboard', methods=['GET'])
def dashboard():
    visualization_service.create_visualizations()
    return jsonify({'message': 'Dashboard visualization successful'}), 200

# Run the Flask app if this file is executed as the main program
if __name__ == '__main__':
    app.run(debug=True)

# Note: The Airflow DAG should be placed in the Airflow DAGs directory and the Flask app should be run separately.