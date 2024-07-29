from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.configuration import conf

import pandas
from datetime import datetime

PATH = Variable.get("my_path")

conf.set("core", "template_searchpath", PATH)

def insert_data(table_name):
    df = pandas.read_csv(PATH + f'{table_name}.csv', delimiter=';')
    postgres_hook = PostgresHook("hw_0715")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="stage", if_exists="replace", index=False)


default_args = {
         "owner": "dwh_user", 
         "start_date": datetime(2024, 7, 15),
         "retries":2
}

with DAG('new_insert_data', 
          default_args = default_args,
          description = 'загрузка данных в posgres',
          catchup = False,
          schedule = '0 0 1 * *'
) as dag:
            
    start = DummyOperator(
            task_id = "start"
    )

    ft_balance_f = PythonOperator(
            task_id = "ft_balance_f",
            python_callable = insert_data,
            op_kwargs = {"table_name": "ft_balance_f"}
    )

    ft_posting_f = PythonOperator(
            task_id = "ft_posting_f",
            python_callable = insert_data,
            op_kwargs = {"table_name": "ft_posting_f"}
    )

    md_account_d = PythonOperator(
            task_id = "md_account_d",
            python_callable = insert_data,
            op_kwargs = {"table_name": "md_account_d"}
    )

    md_currency_d = PythonOperator(
            task_id = "md_currency_d",
            python_callable = insert_data,
            op_kwargs = {"table_name": "md_currency_d"}
    )

    md_exchange_rate_d = PythonOperator(
            task_id = "md_exchange_rate_d",
            python_callable = insert_data,
            op_kwargs = {"table_name": "md_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
            task_id = "md_ledger_account_s",
            python_callable = insert_data,
            op_kwargs = {"table_name": "md_ledger_account_s"}
    )

    split = DummyOperator(
            task_id = "split"
    )

    sql_ft_balance_f = SQLExecuteQueryOperator(
            task_id = "sql_ft_balance_f",
            conn_id = "hw_0715",
            sql = "sql/ft_balance_f.sql"
    )
    
    sql_ft_posting_f = SQLExecuteQueryOperator(
            task_id = "sql_ft_posting_f",
            conn_id = "hw_0715",
            sql = "sql/ft_posting_f.sql"
    )

    sql_md_account_d = SQLExecuteQueryOperator(
            task_id = "sql_md_account_d",
            conn_id = "hw_0715",
            sql = "sql/md_account_d.sql"
    )

    sql_md_currency_d = SQLExecuteQueryOperator(
            task_id = "sql_md_currency_d",
            conn_id = "hw_0715",
            sql = "sql/md_currency_d.sql"
    )

    sql_md_exchange_rate_d = SQLExecuteQueryOperator(
            task_id = "sql_md_exchange_rate_d",
            conn_id = "hw_0715",
            sql = "sql/md_exchange_rate_d.sql"
    )

    sql_md_ledger_account_s = SQLExecuteQueryOperator(
            task_id = "sql_md_ledger_account_s",
            conn_id = "hw_0715",
            sql = "sql/md_ledger_account_s.sql"
    )

    end = DummyOperator(
            task_id = "end"
    )

    (
        start 
        >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s]
        >> split
        >> [sql_ft_balance_f, sql_ft_posting_f, sql_md_account_d, sql_md_currency_d, sql_md_exchange_rate_d, sql_md_ledger_account_s]
        >> end
    )
    
