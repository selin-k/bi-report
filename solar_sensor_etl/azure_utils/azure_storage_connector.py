# filename: azure_utils/azure_storage_connector.py
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
import pandas as pd
from io import StringIO

class AzureDataLakeStorageConnector:
    def __init__(self, account_name: str, file_system_name: str):
        self.account_name = account_name
        self.file_system_name = file_system_name
        self.service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net",
                                                    credential=DefaultAzureCredential())
        self.file_system_client = self.service_client.get_file_system_client(file_system=file_system_name)

    def fetch_data(self, filename: str) -> pd.DataFrame:
        """
        Fetches data from Azure Data Lake Storage and returns it as a pandas DataFrame.
        :param filename: The name of the file to fetch.
        :return: A pandas DataFrame containing the data from the file.
        """
        file_client = self.file_system_client.get_file_client(filename)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        s = str(downloaded_bytes, 'utf-8')
        data = StringIO(s)
        df = pd.read_csv(data)
        return df

    def save_data(self, filename: str, data: pd.DataFrame) -> bool:
        """
        Saves a pandas DataFrame to Azure Data Lake Storage.
        :param filename: The name of the file to save the data to.
        :param data: The pandas DataFrame to save.
        :return: A boolean indicating whether the operation was successful.
        """
        file_client = self.file_system_client.get_file_client(filename)
        output = StringIO()
        data.to_csv(output, index=False)
        output.seek(0)
        file_client.upload_data(output.read(), overwrite=True)
        return True