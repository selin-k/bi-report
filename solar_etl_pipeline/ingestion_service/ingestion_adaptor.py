# filename: ingestion_service/ingestion_adaptor.py
import yaml
import os
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential

class DataIngestionAdaptor:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, 'r') as config_file:
            self.config = yaml.safe_load(config_file)
        self.service_client = self.authenticate()

    def authenticate(self):
        credential = ClientSecretCredential(
            tenant_id=os.environ['AZURE_TENANT_ID'],
            client_id=os.environ['AZURE_CLIENT_ID'],
            client_secret=os.environ['AZURE_CLIENT_SECRET']
        )
        service_client = DataLakeServiceClient(account_url=f"https://{os.environ['AZURE_STORAGE_ACCOUNT_NAME']}.dfs.core.windows.net", credential=credential)
        return service_client

    def ingest_data(self):
        # Assuming the data source is a file system for simplicity
        # In a real scenario, you would have different adaptors for API, Database, etc.
        file_system_client = self.service_client.get_file_system_client(file_system=self.config['DataIngestion']['ProjectName'])
        directory_client = file_system_client.get_directory_client(directory=datetime.now().strftime('%Y-%m-%d'))
        directory_client.create_directory()

        local_file_path = self.config['DataIngestion']['SourcePath'] + self.config['DataIngestion']['SourceName']
        file_client = directory_client.create_file(file_name=self.config['DataIngestion']['SourceName'])
        
        with open(local_file_path, 'rb') as local_file:
            file_contents = local_file.read()
            file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
            file_client.flush_data(len(file_contents))

        print(f"Data ingestion complete for file: {self.config['DataIngestion']['SourceName']}")

# This function will be called by the Airflow PythonOperator
def ingest_data():
    adaptor = DataIngestionAdaptor()
    adaptor.ingest_data()

if __name__ == "__main__":
    ingest_data()