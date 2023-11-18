# filename: curation_service/curation_processor.py
import pandas as pd
import yaml
from azure.storage.filedatalake import DataLakeServiceClient
from ingestion_service.ingestion_adaptor import DataIngestionAdaptor

class DataCurationProcessor:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, 'r') as config_file:
            self.config = yaml.safe_load(config_file)
        self.ingestion_adaptor = DataIngestionAdaptor(config_path)

    def curate_data(self):
        # Fetch the raw data from Azure Data Lake
        raw_data = self.ingestion_adaptor.fetch_data(
            container_name=self.config['DataIngestion']['ProjectName'],
            filename=self.config['DataIngestion']['SourceName']
        )

        # Convert raw data to DataFrame
        df = pd.read_csv(raw_data)

        # Data curation steps
        df = df.drop_duplicates()
        df = df.fillna(self.config['DataCuration']['PlaceholderValue'])

        # Assuming field validation and mapping is done here
        # ...

        # Save curated data back to Azure Data Lake as a parquet file
        curated_data_path = f"curated/{self.config['DataIngestion']['SourceName'].replace('.csv', '.parquet')}"
        self.ingestion_adaptor.save_data(
            container_name=self.config['DataIngestion']['ProjectName'],
            filename=curated_data_path,
            data=df
        )

        print(f"Data curation complete for file: {curated_data_path}")

# This function will be called by the Airflow PythonOperator
def curate_data():
    processor = DataCurationProcessor()
    processor.curate_data()

if __name__ == "__main__":
    curate_data()