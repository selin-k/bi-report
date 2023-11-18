# filename: data_transformation/transform_data.py
import pandas as pd
from storage.azure_storage_connector import AzureDataLakeStorageConnector

class DataTransformationService:
    def __init__(self, storage_account_name, storage_account_key, container_name):
        self.storage_connector = AzureDataLakeStorageConnector(storage_account_name, storage_account_key)
        self.container_name = container_name

    def transform_data(self, curated_data):
        """
        Transforms the curated data based on the logical data model.
        :param curated_data: DataFrame containing the curated data to be transformed.
        :return: DataFrame containing the transformed data.
        """
        # Example transformation: Calculate average daily production
        # This is a placeholder for actual transformation logic, which would be more complex
        # and based on the logical data model provided in the context.
        transformed_data = curated_data.copy()
        transformed_data['AverageDailyProduction'] = transformed_data['TotalAmps'] * transformed_data['TotalVolts']

        # Save the transformed data as a parquet file in the 'conformed' folder
        filename = 'conformed/transformed_solar_data.parquet'
        self.storage_connector.save_data(self.container_name, filename, transformed_data.to_parquet(index=False))

        return transformed_data