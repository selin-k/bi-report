# filename: data_transformation/data_transformation.py
from azure.storage.blob import BlobServiceClient
from config.config import Config
from data_transformation.data_models import FactSolarProduction, DimDate, DimLocation, DimPanelType
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime

class DataTransformation:
    def __init__(self):
        self.config = Config()
        self.blob_service_client = BlobServiceClient.from_connection_string(self._get_azure_storage_connection_string())
        self.container_name = self.config.get('AzureDataLake')['ContainerName']

    def _get_azure_storage_connection_string(self):
        """Construct the connection string for Azure storage account."""
        account_name = self.config.get('AzureDataLake')['StorageAccountName']
        account_key = self.config.get('AzureDataLake')['Password']
        return f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

    def transform_data(self):
        """Transform curated data into the conformed structure."""
        # Load curated data from Azure Data Lake's 'curated' folder
        blobs = self.blob_service_client.get_container_client(self.container_name).list_blobs(name_starts_with='curated/')
        for blob in blobs:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob.name)
            curated_data = self._download_from_azure(blob_client)

            # Apply transformation logic to compute KPIs
            fact_table = FactSolarProduction(curated_data).create_fact_table()
            date_dim_table = DimDate(curated_data).create_dim_table()
            location_dim_table = DimLocation(curated_data).create_dim_table()
            panel_type_dim_table = DimPanelType(curated_data).create_dim_table()

            # Store the transformed data in the 'conformed' folder
            self._upload_to_azure(fact_table, 'conformed/', 'FACT_SOLAR_PRODUCTION')
            self._upload_to_azure(date_dim_table, 'conformed/', 'DIM_DATE')
            self._upload_to_azure(location_dim_table, 'conformed/', 'DIM_LOCATION')
            self._upload_to_azure(panel_type_dim_table, 'conformed/', 'DIM_PANEL_TYPE')

    def _download_from_azure(self, blob_client):
        """Download data from Azure Data Lake storage."""
        stream = blob_client.download_blob().readall()
        curated_data = pd.read_parquet(io.BytesIO(stream))
        return curated_data

    def _upload_to_azure(self, data, folder_path, table_name):
        """Upload transformed data as Parquet to Azure Data Lake storage."""
        table = pa.Table.from_pandas(data)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression='snappy')
        buf.seek(0)
        date_str = datetime.now().strftime('%Y%m%d')
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=f"{folder_path}{date_str}/{table_name}.parquet")
        blob_client.upload_blob(buf.getvalue(), overwrite=True)

# Usage example:
# data_transformation = DataTransformation()
# data_transformation.transform_data()