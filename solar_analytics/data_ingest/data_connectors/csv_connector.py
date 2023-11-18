# filename: data_ingest/data_connectors/csv_connector.py
import pandas as pd

class CSVConnector:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_csv(self):
        try:
            return pd.read_csv(self.file_path)
        except Exception as e:
            print(f"Error reading CSV file at {self.file_path}: {e}")
            return None