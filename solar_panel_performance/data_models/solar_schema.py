# filename: data_models/solar_schema.py

class FACT_SOLARPRODUCTION:
    def __init__(self, ProductionID, DateKey, PanelKey, WeatherKey, DailyProduction, Efficiency):
        self.ProductionID = ProductionID
        self.DateKey = DateKey
        self.PanelKey = PanelKey
        self.WeatherKey = WeatherKey
        self.DailyProduction = DailyProduction
        self.Efficiency = Efficiency

class FACT_MAINTENANCEFORECAST:
    def __init__(self, ForecastID, DateKey, PanelKey, ForecastedMaintenanceDate, ConfidenceLevel):
        self.ForecastID = ForecastID
        self.DateKey = DateKey
        self.PanelKey = PanelKey
        self.ForecastedMaintenanceDate = ForecastedMaintenanceDate
        self.ConfidenceLevel = ConfidenceLevel

class DIM_DATE:
    def __init__(self, DateKey, Date, Week, Month, Year):
        self.DateKey = DateKey
        self.Date = Date
        self.Week = Week
        self.Month = Month
        self.Year = Year

class DIM_PANEL:
    def __init__(self, PanelKey, PanelType, InstallationDate, Capacity, Location):
        self.PanelKey = PanelKey
        self.PanelType = PanelType
        self.InstallationDate = InstallationDate
        self.Capacity = Capacity
        self.Location = Location

class DIM_WEATHER:
    def __init__(self, WeatherKey, WeatherCondition, LightIntensity, Temperature):
        self.WeatherKey = WeatherKey
        self.WeatherCondition = WeatherCondition
        self.LightIntensity = LightIntensity
        self.Temperature = Temperature

class DIM_STATE:
    def __init__(self, StateKey, OperationalState):
        self.StateKey = StateKey
        self.OperationalState = OperationalState