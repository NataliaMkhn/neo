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
        .save(f'/opt/airflow/files/output1/{csv_name}.csv')
        

    
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



with DAG('de_1_3_fill_month', 
          default_args = default_args,
          description = 'Загрузка данных из csv и расчет формы 101',
          catchup = False,
          schedule = '0 0 1 1 *',
          params = {"param_date": "2018-02-01"},
) as dag:
           
    start = DummyOperator(
            task_id = "start"
    )

    fill_month = PostgresOperator(
            task_id='fill_month',
            postgres_conn_id='dwh',
            sql="""
                call ds.fill_month_dm(to_date('{{ dag_run.conf["param_date"] }}','yyy-MM-dd'));
            """
    )

    fill_101_month = PostgresOperator(
            task_id='fill_101_month',
            postgres_conn_id='dwh',
            sql="""
                call dm.fill_f101_round_f(to_date('{{ dag_run.conf["param_date"] }}','yyy-MM-dd'));
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
        >> fill_month
        >> fill_101_month
        >> save_to_csv
        >> log_end
        >> end
    )
   