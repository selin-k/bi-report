# filename: data_ingestion/ingestion_manager.py
import os
from data_ingestion.adapter import DataAdapter
from utilities.config_loader import ConfigLoader
from utilities.exceptions import DataIngestionError

class IngestionManager:
    def __init__(self, config_loader: ConfigLoader):
        self.config_loader = config_loader
        self.data_adapter = DataAdapter(config_loader)
        self.raw_data_path = self.config_loader.get('raw_data_path', 'raw')

    def ingest_data(self, source_type, source_path):
        """Orchestrates the data ingestion process."""
        try:
            # Fetch data using the adapter
            data = self.data_adapter.fetch_data(source_type, source_path)
            
            # Ensure the raw data directory exists
            if not os.path.exists(self.raw_data_path):
                os.makedirs(self.raw_data_path)
            
            # Define the path for the raw data file
            raw_file_path = os.path.join(self.raw_data_path, os.path.basename(source_path))
            
            # Save the data to the raw folder
            with open(raw_file_path, 'w') as raw_file:
                raw_file.write(data)
                
            return True
        except Exception as e:
            raise DataIngestionError(f"Data ingestion failed: {e}")

# Example usage:
# config_loader = ConfigLoader('path/to/config.yaml')
# ingestion_manager = IngestionManager(config_loader)
# ingestion_manager.ingest_data('local', '/data/solar_sensors.csv')