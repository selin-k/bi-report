# filename: storage/azure_storage_connector.py
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from io import StringIO, BytesIO

class AzureDataLakeStorageConnector:
    def __init__(self, storage_account_name, storage_account_key):
        self.service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net",
                                                    credential=storage_account_key)

    def fetch_data(self, container_name, filename):
        """
        Fetches data from Azure Data Lake Storage and returns it as a DataFrame.
        :param container_name: Name of the container where the file is located.
        :param filename: Name of the file to fetch.
        :return: DataFrame containing the fetched data.
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
        Saves data to Azure Data Lake Storage.
        :param container_name: Name of the container where the file will be saved.
        :param filename: Name of the file to save.
        :param data: DataFrame containing the data to be saved.
        :return: Boolean indicating the success of the operation.
        """
        file_system_client = self.service_client.get_file_system_client(file_system=container_name)
        file_client = file_system_client.get_file_client(filename)
        data_bytes = data.to_csv(index=False).encode('utf-8')
        file_client.upload_data(data_bytes, overwrite=True)
        return True