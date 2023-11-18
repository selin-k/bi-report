# filename: data_ingest/data_ingestion.py
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.identity import DefaultAzureCredential

class DataIngestionService:
    def __init__(self, connection_string, container_name):
        # Initialize the BlobServiceClient with the connection string and container name
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = container_name

    def ingest_data(self, data_source: str, raw_data_path: str) -> pd.DataFrame:
        """
        Ingest data from the specified CSV data source and upload it to Azure Data Lake Store.

        :param data_source: The file path of the CSV data source.
        :param raw_data_path: The path in the Azure Data Lake Store where the raw data will be saved.
        :return: The ingested data as a pandas DataFrame.
        """
        # Read data from the CSV file into a pandas DataFrame
        df = pd.read_csv(data_source)
        
        # Convert the DataFrame to a CSV formatted string
        csv_data = df.to_csv(index=False)
        
        # Get a BlobClient to interact with the specified blob (file)
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=raw_data_path)
        
        # Upload the CSV data to the Azure Data Lake Store, overwriting any existing blob with the same name
        blob_client.upload_blob(csv_data, overwrite=True)
        
        return df