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


def corr_in_sum( ):

    print()
    print('*****  РАСЧЕТ КОРРЕКТНОГО ЗНАЧЕНИЯ  account_in_sum  *****')
    
    df = spark.read.format("jdbc") \
                 .option('driver', 'org.postgresql.Driver') \
                 .option("url", url_dwh) \
                 .option("user", user_name) \
                 .option("password", user_password) \
                 .option("query", """ 
                                  select *
                                  	 , case
                                  	     when account_in_sum <> lag(account_out_sum) over (partition by account_rk order by effective_date)
                                  	          then lag(account_out_sum) over (partition by account_rk order by effective_date)
	                                       else account_in_sum
	                                     end as correct_account_in_sum
                                  from rd.account_balance
                                  limit 40
                              """) \
                 .load()
    
    print()
    df.show(40)  

def corr_out_sum( ):

    print()
    print('*****  РАСЧЕТ КОРРЕКТНОГО ЗНАЧЕНИЯ  account_out_sum  *****')
    
    df = spark.read.format("jdbc") \
                 .option('driver', 'org.postgresql.Driver') \
                 .option("url", url_dwh) \
                 .option("user", user_name) \
                 .option("password", user_password) \
                 .option("query", """ 
                                   select *
                                  	 , case
                                  	     when account_out_sum <> lead(account_in_sum) over (partition by account_rk order by effective_date)
                                  	          then lead(account_in_sum) over (partition by account_rk order by effective_date)
                                  	     else account_out_sum
                                  	   end as correct_account_out_sum
                                  from rd.account_balance
                                  limit 40
                                 """) \
                 .load()
    
    print()
    df.show(40)  

def show_acc_bal_turn( ):

    print()
    print('*****  ПРОСМОТР ВИТРИНЫ  rd.account_balance  *****')
    
    df = spark.read.format("jdbc") \
                 .option('driver', 'org.postgresql.Driver') \
                 .option("url", url_dwh) \
                 .option("user", user_name) \
                 .option("password", user_password) \
                 .option("query", """ 
                                      select * 
                                      from dm.account_balance_turnover 
                                      order by account_rk
                                             , effective_date
                                  """) \
                 .load()
    
    print()
    df.show(30)  

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



with DAG('de_2_3', 
          default_args = default_args,
          description = 'Задача 2.3 проектного задания',
          catchup = False,
          schedule = '0 0 1 1 *'
) as dag:
            
    start = DummyOperator(
            task_id = "start"
    )

    show_corr_in_sum = PythonOperator(
            task_id = "show_corr_in_sum",
            python_callable = corr_in_sum,
            op_kwargs = { }
    )
    
    show_corr_out_sum = PythonOperator(
            task_id = "show_corr_out_sum",
            python_callable = corr_out_sum,
            op_kwargs = { }
    )
    
    edit_rd_account = PostgresOperator(
            task_id='edit_rd_account',
            postgres_conn_id='src',
            sql="""
                  update rd.account_balance as b
                  set account_in_sum = t.correct_account_in_sum
                  from (
                        select account_rk
                  		 , effective_date
                  		 , case
	                           when account_in_sum <> lag(account_out_sum) over (partition by account_rk order by effective_date)
	                                then lag(account_out_sum) over (partition by account_rk order by effective_date)
	                           else account_in_sum
	                         end as correct_account_in_sum
                        from rd.account_balance
	                   ) as t
                  where b.account_rk = t.account_rk
                        and b.effective_date = t.effective_date
                        and b.account_in_sum <> t.correct_account_in_sum;
            """
    )

    sql_fill_bal_turn = SQLExecuteQueryOperator(
            task_id = "sql_fill_bal_turn",
            conn_id = "src",
            sql = "sql/2_3_proc_fill_bal_turn.sql"
    )


    fill_account = PostgresOperator(
            task_id='fill_account',
            postgres_conn_id='src',
            sql="""
                call dm.fill_account_balance_turnover();;
            """
    )
   
    show_bal_turn = PythonOperator(
            task_id = "show_bal_turn",
            python_callable = show_acc_bal_turn,
            op_kwargs = { }
    )

    end = DummyOperator(
            task_id = "end"
    )

    (
        start 
        >> show_corr_in_sum
        >> show_corr_out_sum
        >> edit_rd_account
        >> sql_fill_bal_turn
        >> fill_account
        >> show_bal_turn
        >> end
    )
   