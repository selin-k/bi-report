# filename: config.py
# Description: Configuration settings for the solar_performance_dashboard application.

# Azure Data Lake Storage Settings
ADLS_GEN2_ACCOUNT_NAME = 'your_account_name_here'
ADLS_GEN2_ACCOUNT_KEY = 'your_account_key_here'
ADLS_GEN2_FILE_SYSTEM_NAME = 'your_file_system_name_here'

# Data Lake Folders
RAW_DATA_FOLDER = 'raw'
CURATED_DATA_FOLDER = 'curated'
CONFORMED_DATA_FOLDER = 'conformed'

# CSV Ingestion Settings
CSV_FILE_PATH = '/project_name/data/solar_sensors.csv'

# Data Curation Settings
DUPLICATE_STRATEGY = 'drop'  # Options: 'drop', 'keep', 'flag'
MISSING_VALUES_STRATEGY = 'mean'  # Options: 'mean', 'median', 'mode', 'drop', 'fill'

# Data Transformation Settings
FACT_SOLAR_PRODUCTION = 'FACT_SOLAR_PRODUCTION'

# Visualization Settings
VISUALIZATION_FORMAT = 'plotly'

# Orchestration Settings
ORCHESTRATION_SCHEDULE_INTERVAL = '0 6 * * *'  # Every day at 6 AM

# API Settings
API_HOST = '0.0.0.0'
API_PORT = 5000