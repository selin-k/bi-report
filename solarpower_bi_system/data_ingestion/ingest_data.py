# filename: data_ingestion/ingest_data.py
import pandas as pd
from storage.azure_storage_connector import AzureDataLakeStorageConnector

class DataIngestionService:
    def __init__(self, storage_account_name, storage_account_key, container_name):
        self.storage_connector = AzureDataLakeStorageConnector(storage_account_name, storage_account_key)
        self.container_name = container_name

    def ingest_data(self, source_path):
        """
        Ingests data from a CSV file and uploads it to Azure Data Lake Storage.
        :param source_path: Path to the CSV file to be ingested.
        :return: DataFrame containing the ingested data.
        """
        try:
            # Read the CSV file into a DataFrame
            data = pd.read_csv(source_path)

            # Validate the data (this could be expanded with more complex validation rules)
            if data.empty:
                raise ValueError("The CSV file is empty.")

            # Save the data to Azure Data Lake in the 'raw' folder
            filename = 'raw/solar_sensors.csv'
            self.storage_connector.save_data(self.container_name, filename, data)

            return data
        except Exception as e:
            print(f"An error occurred during data ingestion: {e}")
            raise