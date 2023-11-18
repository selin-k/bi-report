# filename: visualization_service/visualization_generator.py
import pandas as pd
import plotly.express as px
import yaml
from azure.storage.filedatalake import DataLakeServiceClient
from transformation_service.transformation_logic import DataTransformationProcessor

class DataVisualizationGenerator:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, 'r') as config_file:
            self.config = yaml.safe_load(config_file)
        self.transformation_processor = DataTransformationProcessor(config_path)

    def visualize_data(self):
        # Fetch the transformed data from Azure Data Lake
        transformed_data_path = f"conformed/{self.config['DataIngestion']['SourceName'].replace('.csv', '.parquet')}"
        transformed_data = self.transformation_processor.fetch_data(
            container_name=self.config['DataIngestion']['ProjectName'],
            filename=transformed_data_path
        )

        # Convert transformed data to DataFrame
        df = pd.read_parquet(transformed_data)

        # Visualization steps
        # Generate visualizations using Plotly based on the transformed data
        # For example, creating line charts for time series data, bar charts for categorical data, etc.
        # ...

        # Save or display the visualizations
        # ...

        print("Data visualization generation complete.")

# This function will be called by the Airflow PythonOperator
def visualize_data():
    generator = DataVisualizationGenerator()
    generator.visualize_data()

if __name__ == "__main__":
    visualize_data()