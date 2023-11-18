# filename: data_curate/data_curation.py
import pandas as pd
from azure_utils.azure_storage_connector import AzureDataLakeStorageConnector

class DataCuration:
    def __init__(self, azure_storage_connector: AzureDataLakeStorageConnector):
        self.azure_storage_connector = azure_storage_connector

    def curate_data(self, raw_data_filename: str, curated_data_filename: str):
        """
        Reads raw data from Azure Data Lake, applies data deduplication, handles null values,
        and maps data to the logical model.
        :param raw_data_filename: The name of the raw data file to curate.
        :param curated_data_filename: The name of the file to save curated data to.
        """
        # Fetch the raw data
        raw_data = self.azure_storage_connector.fetch_data(raw_data_filename)

        # Deduplicate data
        curated_data = raw_data.drop_duplicates()

        # Handle null values
        # Here you can define rules for handling nulls, such as filling with averages or dropping rows
        # For example, to fill missing values in 'temperature' with the column's mean:
        # curated_data['temperature'].fillna(curated_data['temperature'].mean(), inplace=True)

        # Map data to the logical model
        # This step would involve renaming columns, converting data types, etc., to fit the logical data model

        # Save the curated data back to the Azure Data Lake in the 'curated' folder
        self.azure_storage_connector.save_data(curated_data_filename, curated_data)
        print(f"Data curated successfully and saved to {curated_data_filename}")