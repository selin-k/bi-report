# filename: models/data_models.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Date

Base = declarative_base()

class FACT_SOLARPRODUCTION(Base):
    __tablename__ = 'fact_solarproduction'
    ProductionID = Column(Integer, primary_key=True)
    DateKey = Column(Integer, index=True)
    PanelKey = Column(Integer, index=True)
    WeatherKey = Column(Integer, index=True)
    TotalAmps = Column(Float)
    TotalVolts = Column(Float)
    LightIntensity = Column(Float)
    Temperature = Column(Float)
    AverageDailyProduction = Column(Float)
    TotalCapacity = Column(Float)
    PanelEfficiency = Column(Float)

class DIM_DATE(Base):
    __tablename__ = 'dim_date'
    DateKey = Column(Integer, primary_key=True)
    Date = Column(Date)
    Week = Column(Integer)
    Month = Column(Integer)
    Quarter = Column(Integer)
    Year = Column(Integer)

class DIM_PANEL(Base):
    __tablename__ = 'dim_panel'
    PanelKey = Column(Integer, primary_key=True)
    PanelID = Column(String)
    MaxOutputCapacity = Column(Float)
    InstallationDate = Column(Date)
    Location = Column(String)
    State = Column(String)

class DIM_WEATHER(Base):
    __tablename__ = 'dim_weather'
    WeatherKey = Column(Integer, primary_key=True)
    WeatherCondition = Column(String)
    TemperatureRange = Column(String)
    LightIntensityRange = Column(String)