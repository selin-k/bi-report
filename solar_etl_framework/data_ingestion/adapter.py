# filename: data_ingestion/adapter.py
import os
import requests
from azure.storage.filedatalake import DataLakeServiceClient
from utilities.config_loader import ConfigLoader
from utilities.exceptions import DataIngestionError

class DataAdapter:
    def __init__(self, config_loader: ConfigLoader):
        self.config_loader = config_loader

    def fetch_from_local(self, file_path):
        """Fetch data from the local file system."""
        if not os.path.exists(file_path):
            raise DataIngestionError(f"File not found: {file_path}")
        with open(file_path, 'r') as file:
            return file.read()

    def fetch_from_api(self, api_url):
        """Fetch data from a web API."""
        response = requests.get(api_url)
        if response.status_code != 200:
            raise DataIngestionError(f"API request failed with status code: {response.status_code}")
        return response.text

    def fetch_from_cloud(self, service_url, file_system_name, file_path):
        """Fetch data from cloud storage."""
        try:
            service_client = DataLakeServiceClient(account_url=service_url, credential=self.config_loader.get('azure_credential'))
            file_system_client = service_client.get_file_system_client(file_system=file_system_name)
            file_client = file_system_client.get_file_client(file_path)
            download = file_client.download_file()
            return download.readall()
        except Exception as e:
            raise DataIngestionError(f"Error fetching from cloud storage: {e}")

    def fetch_data(self, source_type, source_path):
        """Fetch data based on the source type."""
        if source_type == 'local':
            return self.fetch_from_local(source_path)
        elif source_type == 'api':
            return self.fetch_from_api(source_path)
        elif source_type == 'cloud':
            service_url = self.config_loader.get('service_url')
            file_system_name = self.config_loader.get('file_system_name')
            return self.fetch_from_cloud(service_url, file_system_name, source_path)
        else:
            raise DataIngestionError(f"Unsupported source type: {source_type}")

# Example usage:
# config_loader = ConfigLoader('path/to/config.yaml')
# adapter = DataAdapter(config_loader)
# data = adapter.fetch_data('local', '/data/solar_sensors.csv')