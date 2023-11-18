# filename: main.py
from flask import Flask, request, jsonify
from data_ingestion.ingestor import DataIngestionService
from data_curation.curator import DataCurationService
from data_transformation.transformer import DataTransformationService
from data_visualization.visualizer import DataVisualizationService
from storage_connector import AzureDataLakeStorageConnector

app = Flask(__name__)

# Instantiate the storage connector
storage_connector = AzureDataLakeStorageConnector(account_name='your_account_name', account_key='your_account_key')

# Instantiate the services
ingestion_service = DataIngestionService(storage_connector)
curation_service = DataCurationService(storage_connector)
transformation_service = DataTransformationService(storage_connector)
visualization_service = DataVisualizationService(storage_connector)

@app.route('/ingest', methods=['POST'])
def ingest():
    # Ingest solar sensor data from the csv file into the raw data lake
    data = ingestion_service.ingest_data('/project_name/data/solar_sensors.csv')
    return jsonify({'message': 'Data ingested successfully', 'data': data.to_dict()}), 200

@app.route('/curate', methods=['POST'])
def curate():
    # Perform data curation including quality checks and data standardization
    raw_data = request.get_json()
    curated_data = curation_service.curate_data(pd.DataFrame(raw_data))
    return jsonify({'message': 'Data curated successfully', 'data': curated_data.to_dict()}), 200

@app.route('/transform', methods=['POST'])
def transform():
    # Transform curated data into business insights
    curated_data = request.get_json()
    transformed_data = transformation_service.transform_data(pd.DataFrame(curated_data))
    return jsonify({'message': 'Data transformed successfully', 'data': transformed_data.to_dict()}), 200

@app.route('/visualize', methods=['POST'])
def visualize():
    # Create visualizations from the transformed data
    transformed_data = request.get_json()
    visualization_message = visualization_service.visualize_data(pd.DataFrame(transformed_data))
    return jsonify({'message': visualization_message}), 200

if __name__ == '__main__':
    app.run(debug=True)