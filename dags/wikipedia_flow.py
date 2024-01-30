import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.wikipedia_pipeline import get_ufc_data, ufc_data_cleaning, write_ufc_data

dag = DAG(
    dag_id='ufc_flow',
    default_args={
        "owner": "Karol Bhandari",
        "start_date": datetime(2024, 1, 23),
    },
    schedule_interval=None,
    catchup=False
)

get_ufc_data = PythonOperator(
    task_id="get_ufc_data",
    python_callable=get_ufc_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_current_UFC_fighters"},
    dag=dag
)

ufc_data_cleaning = PythonOperator(
    task_id='ufc_data_cleaning',
    provide_context=True,
    python_callable=ufc_data_cleaning,
    dag=dag
)

write_ufc_data = PythonOperator(
    task_id='write_ufc_data',
    provide_context=True,
    python_callable= write_ufc_data,
    dag=dag
)

get_ufc_data >> ufc_data_cleaning >> write_ufc_data
