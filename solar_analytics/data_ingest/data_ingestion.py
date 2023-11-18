# filename: data_ingest/data_ingestion.py
from azure.storage.blob import BlobServiceClient
from .data_connectors.csv_connector import CSVConnector
from config.config import Config

class DataIngestion:
    def __init__(self):
        config = Config.get_config()
        self.blob_service_client = BlobServiceClient.from_connection_string(config['authentication']['azure_storage_account_key'])
        self.container_name = config['data_paths']['raw']

    def ingest_data(self, file_path):
        csv_connector = CSVConnector(file_path)
        data = csv_connector.read_csv()
        if data is not None:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_path)
            blob_client.upload_blob(data.to_csv(index=False), overwrite=True)
            return True
        return False