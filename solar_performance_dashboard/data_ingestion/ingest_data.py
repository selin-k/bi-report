# filename: data_ingestion/ingest_data.py
import pandas as pd
from azure_data_lake.storage_connector import AzureDataLakeStorageConnector
import config

class DataIngestion:
    def __init__(self):
        self.storage_connector = AzureDataLakeStorageConnector()

    def ingest_data(self) -> None:
        """
        Process and ingest CSV files into Azure Data Lake's 'raw' data store.
        """
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(config.CSV_FILE_PATH)

            # Perform initial cleaning of the CSV data
            df_cleaned = self.clean_data(df)

            # Ingest the clean data into the Azure Data Lake 'raw' data store
            self.storage_connector.save_data(config.RAW_DATA_FOLDER, 'solar_sensors_cleaned.csv', df_cleaned)
            print("Data ingestion completed successfully.")
        except Exception as e:
            print(f"Data ingestion failed: {e}")
            raise

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform an initial cleaning of the CSV data by removing improperly formatted rows.
        """
        # Clean the data according to the specific requirements and data schema.
        # For example, filling NaN values with the mean of the column
        df_cleaned = df.fillna(df.mean())
        return df_cleaned

# Note: The actual cleaning logic will depend on the specific requirements and data schema.