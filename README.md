# UFC-Data-Analysis-ETL-Azure-

This Python-based project crawls data from Wikipedia using Apache Airflow, cleans it and pushes it Azure Data Lake for processing. <br />

Requirements <br />
Python 3.9 (minimum) <br />
Docker <br />
PostgreSQL <br />
Apache Airflow 2.6 (minimum) <br />

<br />
<br />
<br />
<br />
Getting Started<br /><br />

Install Python dependencies.<br />

pip install -r requirements.txt<br />
Running the Code With Docker<br />
Start your services on Docker with<br />
docker compose up -d<br />
Trigger the DAG on the Airflow UI.<br /><br /><br /><br />


How It Works<br />
Fetches data from Wikipedia.<br />
Cleans the data.<br />
Transforms the data.<br />
Pushes the data to Azure Data Lake.<br /><br /><br />


![Architecture](https://github.com/Karol-96/UFC-Data-Analysis-ETL-Azure-/assets/70049752/d2c12041-a535-41c5-8f87-03eb47a1bccb)

