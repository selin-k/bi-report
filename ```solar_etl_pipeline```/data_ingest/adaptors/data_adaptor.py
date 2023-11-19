# filename: data_ingest/adaptors/data_adaptor.py
from azure.storage.blob import BlobServiceClient
from config.config import Config
import os
import pandas as pd
from datetime import datetime

class DataAdaptor:
    def __init__(self):
        self.config = Config()
        self.blob_service_client = BlobServiceClient.from_connection_string(self._get_azure_storage_connection_string())
        self.container_name = self.config.get('AzureDataLake')['ContainerName']

    def _get_azure_storage_connection_string(self):
        """Construct the connection string for Azure storage account."""
        account_name = self.config.get('AzureDataLake')['StorageAccountName']
        account_key = self.config.get('AzureDataLake')['Password']
        return f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

    def ingest_data(self):
        """Ingest data from the source to Azure Data Lake storage."""
        data_source = self.config.get('DataSources')['LocalFolder']
        file_path = os.path.join(data_source['SourcePath'], data_source['SourceName'])
        data_type = data_source['DataType']

        df = self._read_data(file_path, data_type)

        # Create a new dated sub-folder in the 'raw' folder
        date_str = datetime.now().strftime('%Y%m%d')
        raw_folder_path = f"raw/{date_str}/"
        self._upload_to_azure(df, raw_folder_path, data_source['SourceName'])

    def _read_data(self, file_path, data_type):
        """Read data from the file and convert to CSV if necessary."""
        if data_type.lower() == 'csv':
            return pd.read_csv(file_path)
        else:
            # Add logic for converting other data types to CSV
            raise NotImplementedError(f"Data type '{data_type}' conversion to CSV is not implemented.")

    def _upload_to_azure(self, df, folder_path, file_name):
        """Upload a DataFrame to Azure Data Lake storage."""
        # Convert DataFrame to CSV and upload in chunks if large
        csv_data = df.to_csv(index=False).encode('utf-8')
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=f"{folder_path}{file_name}")
        blob_client.upload_blob(csv_data, overwrite=True, blob_type="BlockBlob")

# Usage example:
# data_adaptor = DataAdaptor()
# data_adaptor.ingest_data()