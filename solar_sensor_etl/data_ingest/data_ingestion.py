# filename: data_ingest/data_ingestion.py
import pandas as pd
from azure_utils.azure_storage_connector import AzureDataLakeStorageConnector
from datetime import datetime

class DataIngestion:
    def __init__(self, source_file_path: str, azure_storage_connector: AzureDataLakeStorageConnector):
        self.source_file_path = source_file_path
        self.azure_storage_connector = azure_storage_connector

    def ingest_data(self):
        """
        Reads the CSV file, validates the data, and ingests it into Azure Data Lake Storage.
        """
        # Read the CSV file into a pandas DataFrame
        try:
            data = pd.read_csv(self.source_file_path)
        except FileNotFoundError:
            print(f"The file {self.source_file_path} does not exist.")
            return False
        except pd.errors.EmptyDataError:
            print(f"The file {self.source_file_path} is empty.")
            return False

        # Perform validation checks
        if not self.validate_data(data):
            return False

        # Save the DataFrame to the Azure Data Lake 'raw' folder with a timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"raw/solar_sensors_{timestamp}.csv"
        self.azure_storage_connector.save_data(filename, data)
        print(f"Data ingested successfully to {filename}")
        return True

    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        Validates the DataFrame to ensure data integrity.
        :param data: The pandas DataFrame to validate.
        :return: A boolean indicating whether the data is valid.
        """
        required_columns = {'sensor_id', 'timestamp', 'temperature', 'voltage'}
        if not required_columns.issubset(data.columns):
            print("Validation Error: Missing required columns.")
            return False

        # Add more validation checks as needed
        # For example, check for correct data types, check for negative values in columns that should only have positive values, etc.

        return True