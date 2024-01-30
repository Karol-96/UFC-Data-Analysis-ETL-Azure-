# UFC-Data-Analysis-ETL-Azure-

This Python-based project crawls data from Wikipedia using Apache Airflow, cleans it and pushes it Azure Data Lake for processing.

Requirements
Python 3.9 (minimum)
Docker
PostgreSQL
Apache Airflow 2.6 (minimum)
Getting Started

Install Python dependencies.

pip install -r requirements.txt
Running the Code With Docker
Start your services on Docker with
docker compose up -d
Trigger the DAG on the Airflow UI.
How It Works
Fetches data from Wikipedia.
Cleans the data.
Transforms the data.
Pushes the data to Azure Data Lake.


![Architecture](https://github.com/Karol-96/UFC-Data-Analysis-ETL-Azure-/assets/70049752/d2c12041-a535-41c5-8f87-03eb47a1bccb)

