#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
    $(command -v pip) install --user -r requirements.txt
fi

# Initialize the database if it hasn't been initialized yet
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username karol \
    --firstname karol \
    --lastname bhandari \
    --role Admin \
    --email admin@airscholar.com \
    --password karol
fi

$(command -v airflow) db upgrade

exec airflow webserver
