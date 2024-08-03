from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.configuration import conf

from datetime import datetime


default_args = {
         "owner": "src_user", 
         "start_date": datetime(2024, 7, 15),
         "retries":2
}

with DAG('de_2_db_initialized', 
          default_args = default_args,
          description = 'Проект 2 создание таблиц',
          catchup = False,
          schedule = None
) as dag:
            
    start = DummyOperator(
            task_id = "start"
    )

    de_2_create_table = SQLExecuteQueryOperator(
            task_id = "de_2_create_table",
            conn_id = "src",
            sql = "sql/DATABASE_DUMP.sql"
    )
    

    end = DummyOperator(
            task_id = "end"
    )

    (
        start 
        >> de_2_create_table
        >> end
    )
