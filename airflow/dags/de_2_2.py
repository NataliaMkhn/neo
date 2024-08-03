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

def insert_data(csv_name, t_table_name, final_struc):

    df = spark.read.csv(f'/opt/airflow/files/input2/{csv_name}.csv',
        sep=',',
        header=True,
        encoding='Windows-1251',
        schema=final_struc 
        )

    df.write \
        .mode('overwrite') \
        .format("jdbc") \
        .option('driver', 'org.postgresql.Driver') \
        .option("url", url_dwh) \
        .option("dbtable", t_table_name) \
        .option("user", user_name) \
        .option("password", user_password) \
        .save()
        
def show_data_info():

    print()
    print('*****  АНАЛИЗ ЗАПОЛНЕНИЯ ТАБЛИЦЫ dm.loan_holiday_info  *****')
    
    df = spark.read.format("jdbc") \
                 .option('driver', 'org.postgresql.Driver') \
                 .option("url", url_dwh) \
                 .option("user", user_name) \
                 .option("password", user_password) \
                 .option("query", """ 
                            select effective_from_date
                                , effective_to_date
                                , count(effective_from_date) as effective_from_date_c
                                , count(effective_to_date) as effective_to_date_c
                                , count(deal_rk) as deal_rk
                                , count(agreement_rk) as agreement_rk
                                , count(client_rk) as client_rk
                                , count(department_rk) as department_rk
                                , count(product_rk) as product_rk
                                , count(product_name) as product_name
                                , count(deal_type_cd) as deal_type_cd
                                , count(deal_start_date) as deal_start_date
                                , count(deal_name) as deal_name
                                , count(deal_number) as deal_number
                                , count(deal_sum) as deal_sum
                                , count(loan_holiday_type_cd) as loan_holiday_type_cd
                                , count(loan_holiday_start_date) as loan_holiday_start_date
                                , count(loan_holiday_finish_date) as loan_holiday_finish_date
                                , count(loan_holiday_fact_finish_date) as loan_holiday_fact_finish_date
                                , count(loan_holiday_finish_flg) as loan_holiday_finish_flg
                                , count(loan_holiday_last_possible_date) as loan_holiday_last_possible_date
                           from dm.loan_holiday_info
                           group by effective_from_date
                                  , effective_to_date                                  
                                 """) \
                 .load()
    
    print()
    df.show()  



default_args = {
         "owner": "src_user", 
         "start_date": datetime(2024, 7, 29),
         "retries":2
}

data_schema_product = [
               StructField('product_rk', LongType(), True),
               StructField('product_name', StringType(), True),
               StructField('effective_from_date',DateType(), True),
               StructField('effective_to_date', DateType(), True),
                          ]
final_struc_product = StructType(fields = data_schema_product)

data_schema_deal = [
               StructField('deal_rk', LongType(), True),
               StructField('deal_num', StringType(), True),
               StructField('deal_name',StringType(), True),
               StructField('deal_sum', DecimalType(), True),
               StructField('client_rk', LongType(), True),
               StructField('account_rk', LongType(), True),
               StructField('agreement_rk', LongType(), True),
               StructField('deal_start_date', DateType(), True),
               StructField('department_rk', LongType(), True),
               StructField('product_rk', LongType(), True),
               StructField('deal_type_cd', StringType(), True),
               StructField('effective_from_date', DateType(), True),
               StructField('effective_to_date', DateType(), True)
                          ]
final_struc_deal = StructType(fields = data_schema_deal)



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

with DAG('de_2_2', 
          default_args = default_args,
          description = 'Задача 2.2 проектного задания',
          catchup = False,
          schedule = '0 0 1 1 *'
) as dag:
            
    start = DummyOperator(
            task_id = "start"
    )

    rd_product = PythonOperator(
            task_id = "rd_product",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "product_info", "t_table_name": "public.product", "final_struc": final_struc_product}
    )

    rd_deal_info = PythonOperator(
            task_id = "rd_deal_info",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "deal_info", "t_table_name": "public.deal_info", "final_struc": final_struc_deal}
    )

    show_info = PythonOperator(
            task_id = "show_info",
            python_callable = show_data_info,
            op_kwargs = { }
    )

    fill_rd_product = PostgresOperator(
            task_id='fill_rd_product',
            postgres_conn_id='src',
            sql="""
                   truncate table rd.product;
                   insert into rd.product (
                        select distinct * 
                        from public.product
                        );
            """
    )

    fill_rd_deal = PostgresOperator(
            task_id='fill_rd_deal',
            postgres_conn_id='src',
            sql="""
                    insert into rd.deal_info (
                          select * from public.deal_info
                          except
                          select * from rd.deal_info
                         );
            """
    )

    sql_fill_loan_hol = SQLExecuteQueryOperator(
            task_id = "sql_fill_loan_hol",
            conn_id = "src",
            sql = "sql/2_2_proc_fill_loan_h.sql"
    )


    fill_dm_loan_h = PostgresOperator(
            task_id='fill_dm_loan_h',
            postgres_conn_id='src',
            sql="""
                call dm.fill_loan_holiday_info();
            """
    )

    show_info_after = PythonOperator(
            task_id = "show_info_after",
            python_callable = show_data_info,
            op_kwargs = { }
    )
 
    end = DummyOperator(
            task_id = "end"
    )

    (
        start 
        >> rd_product
        >> rd_deal_info
        >> show_info
        >> fill_rd_product
        >> fill_rd_deal
        >> sql_fill_loan_hol
        >> fill_dm_loan_h
        >> show_info_after
        >> end
    )
   