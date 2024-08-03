from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.configuration import conf

from datetime import datetime

default_args = {
         "owner": "dwh_user", 
         "start_date": datetime(2024, 7, 15),
         "retries":2
}

with DAG('de_1_1_create_tables_and_procedures', 
          default_args = default_args,
          description = 'создание таблиц и процедур',
          catchup = False,
          schedule = '0 0 1 1 *'
) as dag:
            
    start = DummyOperator(
            task_id = "start"
    )

    sql_create_table = SQLExecuteQueryOperator(
            task_id = "sql_create_table",
            conn_id = "dwh",
            sql = "sql/1_1 create_table.sql"
    )
    
    sql_insert_data = SQLExecuteQueryOperator(
            task_id = "sql_insert_data",
            conn_id = "dwh",
            sql = "sql/1_1 insert_data (spark).sql"
    )

    sql_fill_balance_turnover = SQLExecuteQueryOperator(
            task_id = "sql_fill_balance_turnover",
            conn_id = "dwh",
            sql = "sql/1_2 fill_balance_turnover.sql"
    )

    sql_fill_month = SQLExecuteQueryOperator(
            task_id = "sql_fill_month",
            conn_id = "dwh",
            sql = "sql/1_2 fill_month.sql"
    )

    sql_fill_101_month = SQLExecuteQueryOperator(
            task_id = "sql_fill_101_month",
            conn_id = "dwh",
            sql = "sql/1_3 fill_101.sql"
    )


    end = DummyOperator(
            task_id = "end"
    )

    (
        start 
        >> sql_create_table
        >> sql_insert_data
        >> sql_fill_balance_turnover
        >> sql_fill_month
        >> sql_fill_101_month 
        >> end
    )
    
