# filename: data-visualization/data_visualization_service.py
import pandas as pd
import plotly.express as px
from azure.storage.blob import BlobServiceClient
import yaml

class DataVisualizationService:
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.blob_service_client = BlobServiceClient.from_connection_string(self.config['azure_storage']['connection_string'])
        self.container_client = self.blob_service_client.get_container_client(self.config['azure_storage']['data_lake_store_name'])

    def create_visualizations(self):
        # Download transformed data from Azure Data Lake
        blob_client = self.container_client.get_blob_client("transformed/solar_sensors.csv")
        downloader = blob_client.download_blob()
        transformed_data = downloader.readall()
        
        # Convert bytes to DataFrame
        df = pd.read_csv(pd.compat.StringIO(transformed_data.decode('utf-8')))
        
        # Create visualizations
        # Assuming 'Date' and 'DailyProduction' columns exist for creating a time series line chart
        fig = px.line(df, x='Date', y='DailyProduction', title='Average Daily Production Over Time')
        fig.write_html('visualizations/average_daily_production.html')
        
        # Additional visualizations can be created based on the mapping of metrics to visualization components
        
        return "Dashboard visualization successful"

# Example usage:
# visualization_service = DataVisualizationService(config_path='config/config.yaml')
# visualization_service.create_visualizations()