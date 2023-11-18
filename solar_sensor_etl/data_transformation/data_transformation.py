# filename: data_transformation/data_transformation.py
import pandas as pd
import numpy as np
from azure_utils.azure_storage_connector import AzureDataLakeStorageConnector

class DataTransformation:
    def __init__(self, azure_storage_connector: AzureDataLakeStorageConnector):
        self.azure_storage_connector = azure_storage_connector

    def transform_data(self, curated_data_filename: str, transformed_data_filename: str):
        """
        Applies business logic to calculate KPIs and populate the Fact and Dimension tables
        based on the star schema using pandas and numpy libraries.
        :param curated_data_filename: The name of the curated data file to transform.
        :param transformed_data_filename: The name of the file to save transformed data to.
        """
        # Fetch the curated data
        curated_data = self.azure_storage_connector.fetch_data(curated_data_filename)

        # Apply transformations to calculate KPIs
        # Example KPI: Production Efficiency
        # This is a placeholder for the actual calculation logic
        curated_data['EfficiencyRating'] = np.random.rand(len(curated_data))

        # Populate Fact and Dimension tables
        # This would involve creating separate DataFrames for each table according to the star schema
        # For example:
        # fact_solar_production = curated_data[['ProductionID', 'DateKey', 'LocationKey', 'PanelTypeKey', 'SensorKey', 'EfficiencyRating']]
        # dim_date = curated_data[['DateKey', 'Date', 'Week', 'Month', 'Quarter', 'Year']].drop_duplicates()

        # Save the transformed data back to the Azure Data Lake in the 'conformed' folder
        self.azure_storage_connector.save_data(transformed_data_filename, curated_data)
        print(f"Data transformed successfully and saved to {transformed_data_filename}")