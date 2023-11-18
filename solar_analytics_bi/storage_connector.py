# filename: storage_connector.py
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from io import StringIO

class AzureDataLakeStorageConnector:
    def __init__(self, account_name, account_key):
        self.service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net",
                                                    credential=account_key)

    def fetch_data(self, container_name, filename):
        """
        Fetches data from Azure Data Lake and returns it as a DataFrame.
        """
        file_system_client = self.service_client.get_file_system_client(file_system=container_name)
        file_client = file_system_client.get_file_client(filename)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        s = str(downloaded_bytes, 'utf-8')
        data = StringIO(s)
        df = pd.read_csv(data)
        return df

    def save_data(self, container_name, filename, data):
        """
        Saves data to Azure Data Lake. The data is expected to be a DataFrame.
        """
        file_system_client = self.service_client.get_file_system_client(file_system=container_name)
        file_client = file_system_client.create_file(filename)
        data_str = data.to_csv(index=False)
        file_client.upload_data(data_str, overwrite=True)
        return True