# filename: data_ingest/data_ingestion.py
from data_ingest.adaptors.data_adaptor import DataAdaptor
from config.config import Config
import os

class DataIngestion:
    def __init__(self):
        self.config = Config()
        self.data_adaptor = DataAdaptor()

    def ingest(self):
        """Orchestrate the process of data ingestion."""
        # Here you can add any preprocessing or setup needed before ingestion
        self.data_adaptor.ingest_data()
        # Here you can add any postprocessing or cleanup needed after ingestion

# Usage example:
# data_ingestion = DataIngestion()
# data_ingestion.ingest()