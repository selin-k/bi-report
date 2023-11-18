# filename: transformation_service/transformation_logic.py
import pandas as pd
import yaml
from azure.storage.filedatalake import DataLakeServiceClient
from curation_service.curation_processor import DataCurationProcessor

class DataTransformationProcessor:
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, 'r') as config_file:
            self.config = yaml.safe_load(config_file)
        self.curation_processor = DataCurationProcessor(config_path)

    def transform_data(self):
        # Fetch the curated data from Azure Data Lake
        curated_data_path = f"curated/{self.config['DataIngestion']['SourceName'].replace('.csv', '.parquet')}"
        curated_data = self.curation_processor.fetch_data(
            container_name=self.config['DataIngestion']['ProjectName'],
            filename=curated_data_path
        )

        # Convert curated data to DataFrame
        df = pd.read_parquet(curated_data)

        # Data transformation steps
        # Here you would apply transformations according to the logical data model
        # For example, calculating KPIs, joining with dimension tables, etc.
        # ...

        # Save transformed data back to Azure Data Lake in the 'conformed' folder
        transformed_data_path = f"conformed/{self.config['DataIngestion']['SourceName'].replace('.csv', '.parquet')}"
        self.curation_processor.save_data(
            container_name=self.config['DataIngestion']['ProjectName'],
            filename=transformed_data_path,
            data=df
        )

        print(f"Data transformation complete for file: {transformed_data_path}")

# This function will be called by the Airflow PythonOperator
def transform_data():
    processor = DataTransformationProcessor()
    processor.transform_data()

if __name__ == "__main__":
    transform_data()