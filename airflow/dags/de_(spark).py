from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, regexp_replace
from pyspark.sql.types import *

def insert_data(csv_name, t_table_name, ):

    df = spark.read.csv(f'/opt/airflow/files/{csv_name}.csv',
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
        


def save_data(csv_name, table_name):

    df = spark.read \
                 .format("jdbc") \
                 .option('driver', 'org.postgresql.Driver') \
                 .option("url", url_dwh) \
                 .option("dbtable", table_name) \
                 .option("user", user_name) \
                 .option("password", user_password) \
                 .load()


    cols = ['balance_in_rub', 
            'balance_in_val', 
            'balance_in_total', 
            'turn_deb_rub',
            'turn_deb_val', 
            'turn_deb_total', 
            'turn_cre_rub',
            'turn_cre_val',
            'turn_cre_total',
            'balance_out_rub',
            'balance_out_val',
            'balance_out_total'
            ]

    for col_name in cols:
        df = df.withColumn(col_name, regexp_replace(format_number(col_name, 8), ',', ''))
        #преобразовываем в строки, чтобы не менялся формат нулевых значений

    df.write \
        .format("csv") \
        .mode('overwrite') \
        .options(header=True, sep=";") \
        .save(f'/opt/airflow/files/_{csv_name}.csv')
        

    


default_args = {
         "owner": "dwh_user", 
         "start_date": datetime(2024, 6, 15),
         "retries":2
}

url_dwh = 'jdbc:postgresql://postgres_dwh:5432/dwh_db'
user_name = 'dwh_user'
user_password = 'dwh_password'    

spark = SparkSession.builder\
        .appName('PySpark')\
        .config('spark.jars', '/opt/airflow/files/postgresql-42.6.2.jar')\
        .getOrCreate()



with DAG('de_pr_spark', 
          default_args = default_args,
          description = 'Загрузка данных из csv и расчет формы 101',
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

    fill_jan_2018 = PostgresOperator(
            task_id='fill_jan_2018',
            postgres_conn_id='dwh',
            sql="""
                call ds.fill_month_dm('2018-02-01'::date);
            """
    )

    fill_101_jan_2018 = PostgresOperator(
            task_id='fill_101_jan_2018',
            postgres_conn_id='dwh',
            sql="""
                call dm.fill_f101_round_f('2018-02-01'::date);
            """
    )

    save_to_csv = PythonOperator(
            task_id = "save_to_csv",
            python_callable = save_data,
            op_kwargs = {"csv_name": "dm_f101_round_f", "table_name": "dm.dm_f101_round_f"}
    )

    log_end = PostgresOperator(
            task_id='log_end',
            postgres_conn_id='dwh',
            sql="""
                call logs.log_write('info', 'форма 101 выгружена в файл dm_f101_round_f.csv ');
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
        >> fill_jan_2018
        >> fill_101_jan_2018
        >> save_to_csv
        >> log_end
        >> end
    )
   