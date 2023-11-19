# filename: main.py
from flask import Flask, request, jsonify
from data_ingest.data_ingestion import DataIngestion
from data_curate.data_curation import DataCuration
from data_transformation.data_transformation import DataTransformation
from data_visualization.dashboard import Dashboard
from orchestration.logic_app import LogicAppOrchestrator

app = Flask(__name__)

@app.route('/ingest', methods=['POST'])
def ingest_data():
    data_ingestion = DataIngestion()
    data_ingestion.ingest()
    return jsonify({"message": "Data ingestion successful"}), 200

@app.route('/curate', methods=['POST'])
def curate_data():
    data = request.get_json()
    schema_mapping = data.get('schemaMapping', {})
    data_curation = DataCuration()
    data_curation.curate_data(schema_mapping)
    return jsonify({"message": "Data curation successful"}), 200

@app.route('/transform', methods=['POST'])
def transform_data():
    data = request.get_json()
    transformation_logic = data.get('transformationLogic', {})
    data_transformation = DataTransformation()
    data_transformation.transform_data(transformation_logic)
    return jsonify({"message": "Data transformation successful"}), 200

@app.route('/visualize', methods=['POST'])
def visualize_data():
    data = request.get_json()
    visualization_params = data.get('visualizationParams', {})
    dashboard = Dashboard()
    fig = dashboard.visualize_data(visualization_params)
    # In a real scenario, you would return the visualization or save it to a file
    # Here, we'll just return a success message
    return jsonify({"message": "Data visualization successful"}), 200

if __name__ == '__main__':
    orchestrator = LogicAppOrchestrator()
    orchestrator.trigger_etl_pipeline()
    app.run(debug=True)