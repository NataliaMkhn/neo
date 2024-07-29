import logging
from typing import *
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable



def hello_world(name: str) -> str:
    print(f"Hello {name}, from python!")


with DAG(
    dag_id="hello_world",
    catchup=False,
    start_date=days_ago(1),
    # schedule_interval=None,
    schedule_interval='0 12 * * *', # каждый день в 12 часов
    tags=[],
) as dag:

    name = Variable.get("hello_world.name", "Unknown")

    task1 = PythonOperator(
        task_id="python_operator_example",
        python_callable=hello_world,
        op_kwargs={"name": name}
    )

    task2 = BashOperator(
        task_id="bash_example",
        bash_command=f"echo 'Hello {name}, from bash!'"
    )

    task1 >> task2

