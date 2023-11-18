# Solar Performance Dashboard

## Project Description
The Solar Performance Dashboard is a business intelligence tool designed to report on the performance of solar panels. The application is built using a microservices architecture with services for data ingestion, curation, transformation, visualization, and orchestration.

## Setup Instructions

### Prerequisites
- Python 3.8 or higher
- Airflow 2.1.2
- Azure Data Lake Storage account

### Installation
1. Clone the repository to your local machine.
2. Install the required Python dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Configure Airflow:
   - Initialize the Airflow database:
     ```
     airflow db init
     ```
   - Create an Airflow user (follow the prompts after the command):
     ```
     airflow users create
     ```
   - Start the Airflow web server:
     ```
     airflow webserver
     ```
   - In a new terminal, start the Airflow scheduler:
     ```
     airflow scheduler
     ```

### Configuration
Before running the application, you need to configure the following settings in the `config.py` file:

- `ADLS_GEN2_ACCOUNT_NAME`: Your Azure Data Lake Storage Gen2 account name.
- `ADLS_GEN2_ACCOUNT_KEY`: Your Azure Data Lake Storage Gen2 account key.
- `ADLS_GEN2_FILE_SYSTEM_NAME`: The name of the file system in your Azure Data Lake Storage Gen2 account.
- `RAW_DATA_FOLDER`: The folder name in Azure Data Lake Storage Gen2 where raw data will be stored.
- `CURATED_DATA_FOLDER`: The folder name in Azure Data Lake Storage Gen2 where curated data will be stored.
- `CONFORMED_DATA_FOLDER`: The folder name in Azure Data Lake Storage Gen2 where transformed data will be stored.
- `CSV_FILE_PATH`: The local file path to the CSV file containing solar panel performance data.
- `DUPLICATE_STRATEGY`: The strategy for handling duplicate records during data curation.
- `MISSING_VALUES_STRATEGY`: The strategy for handling missing values during data curation.
- `FACT_SOLAR_PRODUCTION`: The name of the logical data model table for solar production.
- `VISUALIZATION_FORMAT`: The format for data visualization (e.g., 'plotly').
- `ORCHESTRATION_SCHEDULE_INTERVAL`: The schedule interval for the orchestration DAG.
- `API_HOST`: The host for the Flask API.
- `API_PORT`: The port for the Flask API.

### Running the Application
To start the Flask API server, run the following command from the root directory of the project:
```
python main.py
```

## API Endpoints
The application provides the following API endpoints to trigger the data pipeline:

- POST `/api/ingest`: Trigger ingestion of new solar sensor data.
- POST `/api/curate`: Trigger data curation process.
- POST `/api/transform`: Trigger data transformation process.
- POST `/api/visualize`: Trigger update of data visualizations.
- POST `/api/orchestrate`: Trigger pipeline orchestration.

## License
This project is licensed under the terms of the MIT license.