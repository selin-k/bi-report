# filename: README.md
# Solar Analytics BI

## Purpose
This project, Solar Analytics BI, is designed to analyze solar panel efficiency and production capacity. It leverages a microservices architecture to ingest, curate, transform, and visualize solar sensor data.

## Configuration
Configuration parameters such as Azure storage account details and data lake container names are stored in `config/config.yaml`. Update this file with the correct values before running the application.

## Dependencies
The project depends on the following Python packages:
- pandas==2.1.3
- apache-airflow==2.2.3
- plotly==5.3.1
- azure-storage-file-datalake==12.9.0
- pyarrow==6.0.1

Ensure that these packages are installed in your Python environment. You can install them using the following command: