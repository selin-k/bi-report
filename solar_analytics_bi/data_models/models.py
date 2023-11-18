# filename: data_models/models.py
import pandas as pd

class Fact_SolarProduction:
    def __init__(self, data=None):
        self.data = pd.DataFrame(data) if data is not None else pd.DataFrame(columns=[
            'ProductionID', 'DateKey', 'PanelKey', 'WeatherKey', 'TotalAmps', 'TotalVolts',
            'LightIntensity', 'Temperature', 'AverageDailyProduction', 'TotalCapacity', 'PanelEfficiency'
        ])

    def insert_data(self, records):
        self.data = pd.concat([self.data, pd.DataFrame.from_records(records)], ignore_index=True)

class Dim_Date:
    def __init__(self, data=None):
        self.data = pd.DataFrame(data) if data is not None else pd.DataFrame(columns=[
            'DateKey', 'Date', 'Week', 'Month', 'Quarter', 'Year'
        ])

    def insert_data(self, records):
        self.data = pd.concat([self.data, pd.DataFrame.from_records(records)], ignore_index=True)

class Dim_Panel:
    def __init__(self, data=None):
        self.data = pd.DataFrame(data) if data is not None else pd.DataFrame(columns=[
            'PanelKey', 'PanelID', 'MaxOutputCapacity', 'InstallationDate', 'Location', 'State'
        ])

    def insert_data(self, records):
        self.data = pd.concat([self.data, pd.DataFrame.from_records(records)], ignore_index=True)

class Dim_Weather:
    def __init__(self, data=None):
        self.data = pd.DataFrame(data) if data is not None else pd.DataFrame(columns=[
            'WeatherKey', 'WeatherCondition', 'TemperatureRange', 'LightIntensityRange'
        ])

    def insert_data(self, records):
        self.data = pd.concat([self.data, pd.DataFrame.from_records(records)], ignore_index=True)