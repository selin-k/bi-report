# filename: main.py
from flask import Flask, jsonify, request
from data_ingest.data_ingestion import DataIngestion
from data_curate.data_curation import DataCuration
from data_transformation.data_transformation import DataTransformation
from data_visualization.dashboard import DataVisualization
from azure_utils.azure_storage_connector import AzureDataLakeStorageConnector

# Initialize Flask app
app = Flask(__name__)

# Initialize Azure Data Lake Storage Connector
# Replace 'your_account_name' and 'your_file_system_name' with actual values
azure_storage_connector = AzureDataLakeStorageConnector(account_name='your_account_name', file_system_name='your_file_system_name')

# Initialize classes for ETL processes
data_ingestion = DataIngestion(source_file_path='/project_name/data/solar_sensors.csv', azure_storage_connector=azure_storage_connector)
data_curation = DataCuration(azure_storage_connector=azure_storage_connector)
data_transformation = DataTransformation(azure_storage_connector=azure_storage_connector)
data_visualization = DataVisualization(azure_storage_connector=azure_storage_connector)

@app.route('/ingest', methods=['POST'])
def ingest_data():
    if data_ingestion.ingest_data():
        return jsonify({"message": "Data ingestion process started."}), 202
    else:
        return jsonify({"message": "Invalid request."}), 400

@app.route('/curate', methods=['POST'])
def curate_data():
    # Placeholder for actual raw data filename and curated data filename
    raw_data_filename = 'raw/solar_sensors.csv'
    curated_data_filename = 'curated/solar_sensors_curated.csv'
    data_curation.curate_data(raw_data_filename, curated_data_filename)
    return jsonify({"message": "Data curation process started."}), 202

@app.route('/transform', methods=['POST'])
def transform_data():
    # Placeholder for actual curated data filename and transformed data filename
    curated_data_filename = 'curated/solar_sensors_curated.csv'
    transformed_data_filename = 'conformed/solar_sensors_transformed.csv'
    data_transformation.transform_data(curated_data_filename, transformed_data_filename)
    return jsonify({"message": "Data transformation process started."}), 202

@app.route('/visualize', methods=['POST'])
def visualize_data():
    # Placeholder for actual transformed data filename
    transformed_data_filename = 'conformed/solar_sensors_transformed.csv'
    data_visualization.generate_visualizations(transformed_data_filename)
    return jsonify({"message": "Data visualization process started and dashboard updated."}), 202

if __name__ == '__main__':
    app.run(debug=True)