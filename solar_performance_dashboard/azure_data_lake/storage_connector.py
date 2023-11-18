# filename: azure_data_lake/storage_connector.py
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import pandas as pd
from io import StringIO
import config

class AzureDataLakeStorageConnector:
    def __init__(self):
        try:
            self.service_client = BlobServiceClient(
                account_url=f"https://{config.ADLS_GEN2_ACCOUNT_NAME}.dfs.core.windows.net",
                credential=DefaultAzureCredential()
            )
            self.file_system_client = self.service_client.get_file_system_client(
                file_system=config.ADLS_GEN2_FILE_SYSTEM_NAME
            )
        except Exception as e:
            # Handle exceptions related to Azure service client initialization
            print(f"Failed to initialize AzureDataLakeStorageConnector: {e}")
            raise

    def fetch_data(self, container_name: str, filename: str) -> pd.DataFrame:
        """
        Fetch data from Azure Data Lake Storage and return it as a DataFrame.
        """
        try:
            blob_client = self.file_system_client.get_blob_client(container_name, filename)
            download_stream = blob_client.download_blob()
            return pd.read_csv(StringIO(download_stream.content_as_text()))
        except Exception as e:
            # Handle exceptions related to fetching data
            print(f"Failed to fetch data from Azure Data Lake Storage: {e}")
            raise

    def save_data(self, container_name: str, filename: str, data: pd.DataFrame) -> bool:
        """
        Save DataFrame to Azure Data Lake Storage.
        """
        try:
            blob_client = self.file_system_client.get_blob_client(container_name, filename)
            data_str = data.to_csv(index=False)
            blob_client.upload_blob(data_str, overwrite=True)
            return True
        except Exception as e:
            # Handle exceptions related to saving data
            print(f"Failed to save data to Azure Data Lake Storage: {e}")
            raise

# Note: The DefaultAzureCredential will work differently depending on the environment.
# In a production environment, it could be managed identity; in development, it could be user credentials.