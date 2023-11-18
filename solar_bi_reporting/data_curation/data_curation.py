# filename: data_curation/data_curation.py
import pandas as pd

class DataCurationService:
    def __init__(self):
        pass

    def curate_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        Curate the ingested raw data by cleaning and transforming it.

        :param raw_data: The raw data as a pandas DataFrame.
        :return: The curated data as a pandas DataFrame.
        """
        # Remove duplicates
        curated_data = raw_data.drop_duplicates()

        # Handle null values (simple example: fill with mean of the column)
        for column in curated_data.columns:
            if curated_data[column].isnull().any():
                if curated_data[column].dtype == 'float64' or curated_data[column].dtype == 'int64':
                    curated_data[column].fillna(curated_data[column].mean(), inplace=True)
                else:
                    curated_data[column].fillna(method='ffill', inplace=True)  # forward fill for non-numeric columns

        # Ensure data consistency (example: convert all strings to lowercase)
        for column in curated_data.select_dtypes(include=['object']).columns:
            curated_data[column] = curated_data[column].str.lower()

        return curated_data