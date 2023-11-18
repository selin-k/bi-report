# filename: main.py
from flask import Flask, request, jsonify
from data_ingest.data_ingestion import DataIngestionService
from data_curation.data_curation import DataCurationService
from data_transformation.data_transformation import DataTransformationService
from data_visualization.data_visualization import DataVisualizationService
import yaml

app = Flask(__name__)

# Load configuration
with open('config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Initialize services
data_ingestion_service = DataIngestionService(connection_string=config['azure_data_lake_store']['connection_string'],
                                              container_name=config['azure_data_lake_store']['container_name'])
data_curation_service = DataCurationService()
data_transformation_service = DataTransformationService()
data_visualization_service = DataVisualizationService()

@app.route('/ingest', methods=['POST'])
def ingest():
    file_path = request.json['file_path']
    raw_data = data_ingestion_service.ingest_data(data_source=file_path,
                                                  raw_data_path=config['azure_data_lake_store']['raw_directory'])
    return jsonify({"message": "Data ingestion successful"}), 200

@app.route('/curate', methods=['POST'])
def curate():
    raw_data = request.json['raw_data']
    curated_data = data_curation_service.curate_data(raw_data=raw_data)
    return jsonify({"message": "Data curation successful"}), 200

@app.route('/transform', methods=['POST'])
def transform():
    curated_data = request.json['curated_data']
    transformed_data = data_transformation_service.transform_data(curated_data=curated_data)
    return jsonify({"message": "Data transformation successful"}), 200

@app.route('/dashboard', methods=['GET'])
def dashboard():
    # In a real-world scenario, this endpoint would integrate with Power BI to retrieve the dashboard
    # For this example, we will simulate the retrieval of a dashboard
    return jsonify({"message": "Dashboard retrieved successfully"}), 200

if __name__ == '__main__':
    app.run(debug=True)