# filename: data_curate/data_curation.py
from azure.storage.blob import BlobServiceClient, BlobClient
from config.config import Config
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import io

class DataCuration:
    def __init__(self):
        self.config = Config()
        self.blob_service_client = BlobServiceClient.from_connection_string(self._get_azure_storage_connection_string())
        self.container_name = self.config.get('AzureDataLake')['ContainerName']

    def _get_azure_storage_connection_string(self):
        """Construct the connection string for Azure storage account."""
        account_name = self.config.get('AzureDataLake')['StorageAccountName']
        account_key = self.config.get('AzureDataLake')['Password']
        return f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

    def curate_data(self, schema_mapping):
        """Curate raw data and save as Parquet in curated folder."""
        # List blobs in the 'raw' folder and process each blob
        blobs = self.blob_service_client.get_container_client(self.container_name).list_blobs(name_starts_with='raw/')
        for blob in blobs:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob.name)
            raw_data = self._download_from_azure(blob_client)
            curated_data = self._apply_data_quality_checks(raw_data, schema_mapping)
            self._upload_to_azure(curated_data, 'curated/')

    def _download_from_azure(self, blob_client):
        """Download data from Azure Data Lake storage."""
        stream = blob_client.download_blob().readall()
        raw_data = pd.read_csv(io.BytesIO(stream))
        return raw_data

    def _apply_data_quality_checks(self, data, schema_mapping):
        """Apply data quality checks and map data to a structured schema."""
        data = data.drop_duplicates()
        data = data.fillna("NULL")
        # Create a new DataFrame with the mapped columns
        curated_data = pd.DataFrame({new_column: data[column] for column, new_column in schema_mapping.items()})
        return curated_data

    def _upload_to_azure(self, data, folder_path):
        """Upload curated data as Parquet to Azure Data Lake storage."""
        table = pa.Table.from_pandas(data)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression='snappy')
        buf.seek(0)
        date_str = datetime.now().strftime('%Y%m%d')
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=f"{folder_path}{date_str}/curated_data.parquet")
        blob_client.upload_blob(buf.getvalue(), overwrite=True)

# Usage example:
# data_curation = DataCuration()
# schema_mapping = {'old_column_name': 'new_column_name', ...}
# data_curation.curate_data(schema_mapping)