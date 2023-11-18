# filename: data-ingest/data_ingestion_service.py
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import yaml

class DataIngestionService:
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.blob_service_client = BlobServiceClient.from_connection_string(self.config['azure_storage']['connection_string'])
        self.container_client = self.blob_service_client.get_container_client(self.config['azure_storage']['data_lake_store_name'])

    def ingest_data(self):
        # Read data from CSV file
        df = pd.read_csv(self.config['data_ingestion']['csv_file_path'])
        
        # Convert DataFrame to bytes
        raw_data = df.to_csv(index=False).encode('utf-8')
        
        # Upload raw data to Azure Data Lake
        blob_client = self.container_client.get_blob_client("raw/solar_sensors.csv")
        blob_client.upload_blob(raw_data, overwrite=True)
        
        return "Data ingestion successful"

# Example usage:
# ingestion_service = DataIngestionService(config_path='config/config.yaml')
# ingestion_service.ingest_data()