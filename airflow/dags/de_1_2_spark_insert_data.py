from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import findspark
findspark.init()

from pyspark.sql import SparkSession

def insert_data(csv_name, t_table_name, ):

    df = spark.read.csv(f'/opt/airflow/files/input1/{csv_name}.csv',
        sep=';',
        header=True,
        ).distinct()

    df.write \
        .mode('overwrite') \
        .format("jdbc") \
        .option('driver', 'org.postgresql.Driver') \
        .option("url", url_dwh) \
        .option("dbtable", t_table_name) \
        .option("user", user_name) \
        .option("password", user_password) \
        .save()
        
default_args = {
         "owner": "dwh_user", 
         "start_date": datetime(2024, 6, 15),
         "retries":2
}

postgres_hook = PostgresHook("dwh")
conn = postgres_hook.get_connection("dwh")
host = conn.host
user_name = conn.login
user_password = conn.password
dbname = conn.schema
port = conn.port    

url_dwh = f'jdbc:postgresql://{host}:{port}/{dbname}'

spark = SparkSession.builder\
        .appName('PySpark')\
        .config('spark.jars', '/opt/airflow/files/postgresql-42.6.2.jar')\
        .getOrCreate()



with DAG('de_1_2_spark_insert_data', 
          default_args = default_args,
          description = 'Загрузка данных из csv ',
          catchup = False,
          schedule = '0 0 1 1 *'
) as dag:
            
    start = DummyOperator(
            task_id = "start"
    )

    ft_balance_f = PythonOperator(
            task_id = "ft_balance_f",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "ft_balance_f", "t_table_name": "ds.t_balance_f"}
    )

    ft_posting_f = PythonOperator(
            task_id = "ft_posting_f",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "ft_posting_f", "t_table_name": "ds.t_posting_f"}
    )

    md_account_d = PythonOperator(
            task_id = "md_account_d",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "md_account_d", "t_table_name": "ds.t_account_d"}
    )

    md_currency_d = PythonOperator(
            task_id = "md_currency_d",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "md_currency_d", "t_table_name": "ds.t_currency_d"}
    )

    md_exchange_rate_d = PythonOperator(
            task_id = "md_exchange_rate_d",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "md_exchange_rate_d", "t_table_name": "ds.t_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
            task_id = "md_ledger_account_s",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "md_ledger_account_s", "t_table_name": "ds.t_ledger_account_s"}
    )

    fill_all_table = PostgresOperator(
            task_id='fill_all_table',
            postgres_conn_id='dwh',
            sql="""
                call ds.fill_all_table();
            """
    )

    end = DummyOperator(
            task_id = "end"
    )

    (
        start 
        >> md_account_d
        >> md_currency_d
        >> md_exchange_rate_d
        >> md_ledger_account_s
        >> ft_balance_f
        >> ft_posting_f
        >> fill_all_table
        >> end
    )
   