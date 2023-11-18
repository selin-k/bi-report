# filename: main.py
from flask import Flask
from airflow import settings
from airflow.models import DagBag, DagRun
from airflow.utils.state import State
from airflow.utils.dates import days_ago
from datetime import datetime
import config

app = Flask(__name__)

# Load the DAG into the DagBag
dag_bag = DagBag(dag_folder=config.DAGS_FOLDER, include_examples=False)

@app.route('/api/ingest', methods=['POST'])
def ingest_data():
    # Trigger the ingest_data task
    DagRun.create(
        dag_id='solar_performance_dashboard',
        run_id="manual__" + datetime.now().isoformat(),
        execution_date=datetime.now(),
        state=State.RUNNING,
        conf={"task_id": "ingest_data"},
        external_trigger=True,
    )
    return "Data ingestion triggered successfully", 200

@app.route('/api/curate', methods=['POST'])
def curate_data():
    # Trigger the curate_data task
    DagRun.create(
        dag_id='solar_performance_dashboard',
        run_id="manual__" + datetime.now().isoformat(),
        execution_date=datetime.now(),
        state=State.RUNNING,
        conf={"task_id": "curate_data"},
        external_trigger=True,
    )
    return "Data curation process triggered successfully", 200

@app.route('/api/transform', methods=['POST'])
def transform_data():
    # Trigger the transform_data task
    DagRun.create(
        dag_id='solar_performance_dashboard',
        run_id="manual__" + datetime.now().isoformat(),
        execution_date=datetime.now(),
        state=State.RUNNING,
        conf={"task_id": "transform_data"},
        external_trigger=True,
    )
    return "Data transformation process triggered successfully", 200

@app.route('/api/visualize', methods=['POST'])
def visualize_data():
    # Trigger the visualize_data task
    DagRun.create(
        dag_id='solar_performance_dashboard',
        run_id="manual__" + datetime.now().isoformat(),
        execution_date=datetime.now(),
        state=State.RUNNING,
        conf={"task_id": "visualize_data"},
        external_trigger=True,
    )
    return "Visualization update triggered successfully", 200

@app.route('/api/orchestrate', methods=['POST'])
def orchestrate_pipeline():
    # Trigger the entire pipeline
    DagRun.create(
        dag_id='solar_performance_dashboard',
        run_id="manual__" + datetime.now().isoformat(),
        execution_date=datetime.now(),
        state=State.RUNNING,
        conf={},
        external_trigger=True,
    )
    return "Pipeline orchestration triggered successfully", 200

if __name__ == '__main__':
    app.run(host=config.API_HOST, port=config.API_PORT)