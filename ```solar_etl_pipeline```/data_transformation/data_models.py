# filename: data_transformation/data_models.py
from datetime import datetime
import pandas as pd

class FactSolarProduction:
    def __init__(self, data):
        self.data = data

    def create_fact_table(self):
        """Transforms the data into the FACT_SOLAR_PRODUCTION table."""
        # Assuming 'data' contains the necessary columns for the FACT table
        # Add any necessary transformations here
        self.data['ProductionDate'] = pd.to_datetime(self.data['ProductionDate'])
        return self.data

class DimDate:
    def __init__(self, data):
        self.data = data

    def create_dim_table(self):
        """Transforms the data into the DIM_DATE table."""
        # Assuming 'data' contains a 'ProductionDate' column
        dates = self.data['ProductionDate'].drop_duplicates().sort_values()
        dim_date = pd.DataFrame({
            'DateKey': dates.dt.strftime('%Y%m%d').astype(int),
            'Date': dates.dt.date,
            'Year': dates.dt.year,
            'Month': dates.dt.month,
            'Day': dates.dt.day,
            'Weekday': dates.dt.weekday,
            'WeekOfYear': dates.dt.isocalendar().week
        })
        return dim_date

class DimLocation:
    def __init__(self, data):
        self.data = data

    def create_dim_table(self):
        """Transforms the data into the DIM_LOCATION table."""
        # Assuming 'data' contains 'LocationID' and 'LocationName' columns
        locations = self.data[['LocationID', 'LocationName']].drop_duplicates()
        return locations

class DimPanelType:
    def __init__(self, data):
        self.data = data

    def create_dim_table(self):
        """Transforms the data into the DIM_PANEL_TYPE table."""
        # Assuming 'data' contains 'PanelTypeID' and 'PanelTypeName' columns
        panel_types = self.data[['PanelTypeID', 'PanelTypeName']].drop_duplicates()
        return panel_types

# Usage example:
# fact_data = FactSolarProduction(some_dataframe)
# fact_table = fact_data.create_fact_table()

# date_data = DimDate(some_dataframe)
# date_dim_table = date_data.create_dim_table()

# location_data = DimLocation(some_dataframe)
# location_dim_table = location_data.create_dim_table()

# panel_type_data = DimPanelType(some_dataframe)
# panel_type_dim_table = panel_type_data.create_dim_table()