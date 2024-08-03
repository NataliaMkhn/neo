from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.configuration import conf

from datetime import datetime

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *

def show_data_count():

    print()
    print('*****  АНАЛИЗ ДУБЛЕЙ В ТАБЛИЦЕ dm.client  *****')
    
    df = spark.read.format("jdbc") \
                 .option('driver', 'org.postgresql.Driver') \
                 .option("url", url_dwh) \
                 .option("user", user_name) \
                 .option("password", user_password) \
                 .option("query", """ 
                             select 'всего записей' as title, 
                                    count(1) 
                             from dm.client 
                             union 
                             select 'уникальных записей' as title, 
                                    count(1) 
                             from (select distinct * from dm.client ) t
                                  """) \
                 .load()
    
    print()
    df.show()  

def show_duplicate_key():

    print('*****  Дубли по ключевым столбцам  *****')
    df = spark.read.format("jdbc") \
                 .option('driver', 'org.postgresql.Driver') \
                 .option("url", url_dwh) \
                 .option("user", user_name) \
                 .option("password", user_password) \
                 .option("query", """ 
                                  select *
                                  from dm.client 
                                  inner join (
	                                        select client_rk, effective_from_date
                                            from dm.client
                                            group by client_rk, effective_from_date
                                            having count(1) > 1
	                                          ) t
                                  using (client_rk, effective_from_date)
                                  """) \
                 .load()
    
    df.show()  


default_args = {
         "owner": "src_user", 
         "start_date": datetime(2024, 7, 29),
         "retries":2
}

postgres_hook = PostgresHook("src")
conn = postgres_hook.get_connection("src")
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



with DAG('de_2_1', 
          default_args = default_args,
          description = 'Задача 2.1 проектного задания',
          catchup = False,
          schedule = '0 0 1 1 *'
) as dag:
            
    start = DummyOperator(
            task_id = "start"
    )

    show_data_count = PythonOperator(
            task_id = "show_data_count",
            python_callable = show_data_count,
            op_kwargs = { }
    )

    del_duplicate = PostgresOperator(
            task_id='del_duplicate',
            postgres_conn_id='src',
            sql="""
                  delete
                  from dm.client 
                  where ctid not in (
	                                 select distinct on (t.*)   -- совпадение по всем столбцам
	                                        ctid                -- системный столбец - физическое расположение 
	                                                            -- данной версии строки в таблице 
                                     from dm.client t
	                                 )
            """
    )


    show_dupl_key = PythonOperator(
            task_id = "show_dupl_key",
            python_callable = show_duplicate_key,
            op_kwargs = { }
    )

    end = DummyOperator(
            task_id = "end"
    )

    (
        start 
        >> show_data_count
        >> del_duplicate
        >> show_dupl_key
        >> end
    )
   