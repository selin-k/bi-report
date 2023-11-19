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

    def transform_data(self, transformation_logic):
        """Transform curated data into the conformed structure."""
        # Load curated data from Azure Data Lake's 'curated' folder
        blobs = self.blob_service_client.get_container_client(self.container_name).list_blobs(name_starts_with='curated/')
        for blob in blobs:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob.name)
            curated_data = self._download_from_azure(blob_client)

            # Apply transformation logic to compute KPIs
            transformed_data = self._apply_transformation_logic(curated_data, transformation_logic)

            # Store the transformed data in the 'conformed' folder
            self._upload_to_azure(transformed_data, 'conformed/')

    def _download_from_azure(self, blob_client):
        """Download data from Azure Data Lake storage."""
        stream = blob_client.download_blob().readall()
        curated_data = pd.read_parquet(io.BytesIO(stream))
        return curated_data

    def _apply_transformation_logic(self, data, transformation_logic):
        """Apply the transformation logic to compute KPIs."""
        # This is a placeholder for the actual transformation logic
        # You would use the transformation_logic parameter to apply the necessary transformations
        # For example, you might have a method to calculate each KPI and then combine them into a single DataFrame
        # Here, we'll just call the methods from the data_models module as an example
        fact_solar_production = FactSolarProduction(data).create_fact_table()
        dim_date = DimDate(data).create_dim_table()
        dim_location = DimLocation(data).create_dim_table()
        dim_panel_type = DimPanelType(data).create_dim_table()

        # Combine all the tables into a single DataFrame for this example
        # In a real scenario, you would likely store them separately
        transformed_data = pd.concat([fact_solar_production, dim_date, dim_location, dim_panel_type], axis=1)
        return transformed_data

    def _upload_to_azure(self, data, folder_path):
        """Upload transformed data as Parquet to Azure Data Lake storage."""
        table = pa.Table.from_pandas(data)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression='snappy')
        buf.seek(0)
        date_str = datetime.now().strftime('%Y%m%d')
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=f"{folder_path}{date_str}/transformed_data.parquet")
        blob_client.upload_blob(buf.getvalue(), overwrite=True)

# Usage example:
# data_transformation = DataTransformation()
# transformation_logic = {}  # This would be defined based on your KPI calculations
# data_transformation.transform_data(transformation_logic)