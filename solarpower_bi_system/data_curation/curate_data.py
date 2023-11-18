# filename: data_curation/curate_data.py
import pandas as pd
from storage.azure_storage_connector import AzureDataLakeStorageConnector

class DataCurationService:
    def __init__(self, storage_account_name, storage_account_key, container_name):
        self.storage_connector = AzureDataLakeStorageConnector(storage_account_name, storage_account_key)
        self.container_name = container_name

    def curate_data(self, raw_data):
        """
        Curates the raw data by performing quality checks and aligning to the target schema.
        :param raw_data: DataFrame containing the raw data to be curated.
        :return: DataFrame containing the curated data.
        """
        # Remove duplicates
        curated_data = raw_data.drop_duplicates()

        # Handle null values (this could be expanded with more complex rules)
        curated_data = curated_data.fillna('Unknown')

        # Save the curated data as a parquet file in the 'curated' folder
        filename = 'curated/curated_solar_data.parquet'
        self.storage_connector.save_data(self.container_name, filename, curated_data.to_parquet(index=False))

        return curated_data