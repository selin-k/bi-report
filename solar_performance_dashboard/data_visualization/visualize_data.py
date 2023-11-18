# filename: data_visualization/visualize_data.py
import pandas as pd
import plotly.express as px
from azure_data_lake.storage_connector import AzureDataLakeStorageConnector
import config

class DataVisualization:
    def __init__(self):
        self.storage_connector = AzureDataLakeStorageConnector()

    def visualize_data(self) -> None:
        """
        Generate visualizations for the dashboard using Plotly.
        """
        try:
            # Load the DataFrame from Azure Data Lake
            df_conformed = self.storage_connector.fetch_data(config.CONFORMED_DATA_FOLDER, 'solar_sensors_transformed.parquet')

            # Generate visualizations for each defined KPI and metric
            # Example: Generating a line chart for a time series KPI
            self.generate_line_chart(df_conformed, 'Time', 'KPI', 'KPI over Time', 'dashboard/kpi_line_chart.html')

            print("Visualization updated successfully.")
        except Exception as e:
            print(f"Visualization update failed: {e}")
            raise

    def generate_line_chart(self, df: pd.DataFrame, x_column: str, y_column: str, title: str, output_path: str) -> None:
        """
        Generate a line chart for a time series KPI and save it as an HTML file.
        """
        fig = px.line(df, x=x_column, y=y_column, title=title)
        fig.write_html(output_path)

# Note: The actual visualization logic will depend on the specific KPIs and metrics.