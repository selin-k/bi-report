# filename: data_visualization/dashboard.py
import pandas as pd
import plotly.express as px
from azure_utils.azure_storage_connector import AzureDataLakeStorageConnector

class DataVisualization:
    def __init__(self, azure_storage_connector: AzureDataLakeStorageConnector):
        self.azure_storage_connector = azure_storage_connector

    def generate_visualizations(self, transformed_data_filename: str):
        """
        Extracts transformed data and generates interactive visualizations using the plotly library.
        :param transformed_data_filename: The name of the transformed data file to visualize.
        """
        # Fetch the transformed data
        transformed_data = self.azure_storage_connector.fetch_data(transformed_data_filename)

        # Generate Time-Series chart
        fig_time_series = px.line(transformed_data, x='Date', y='EfficiencyRating', title='Time Series of Efficiency Rating')
        fig_time_series.show()

        # Generate Heat map
        fig_heat_map = px.density_heatmap(transformed_data, x='Date', y='EfficiencyRating', title='Heat Map of Efficiency Rating')
        fig_heat_map.show()

        # Generate Bar graph
        fig_bar_graph = px.bar(transformed_data, x='SensorType', y='EfficiencyRating', title='Bar Graph of Efficiency Rating by Sensor Type')
        fig_bar_graph.show()

        # Additional visualizations can be added as needed, following the mapping of metrics to visualization components

        # Save the visualizations as HTML files or update the dashboard as required
        # This is a placeholder for the actual logic to save or update the dashboard
        # For example:
        # fig_time_series.write_html('dashboard/time_series.html')
        # fig_heat_map.write_html('dashboard/heat_map.html')
        # fig_bar_graph.write_html('dashboard/bar_graph.html')

        print("Visualizations generated successfully.")