from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *

def insert_data(csv_name):
    spark = SparkSession.builder\
        .appName('PySpark')\
        .config('spark.jars', '/opt/airflow/files/postgresql-42.6.2.jar')\
        .getOrCreate()

    url_dwh = 'jdbc:postgresql://postgres_dwh:5432/dwh_db'
    user_name = 'dwh_user'
    user_password = 'dwh_password'    
    table_name = 'dm.dm_f101_round_f_v2'

    data_schema = [
                   StructField('from_date', DateType(), True),
                   StructField('to_date', DateType(), True),
                   StructField('chapter',StringType(), True),
                   StructField('ledger_account', StringType(), True),
                   StructField('characteristic', StringType(), True),
                   StructField('balance_in_rub', DecimalType(23, 8), True),
                   StructField('r_balance_in_rub', DecimalType(23, 8), True),
                   StructField('balance_in_val', DecimalType(23, 8), True),
                   StructField('r_balance_in_val', DecimalType(23, 8), True),
                   StructField('balance_in_total', DecimalType(23, 8), True),
                   StructField('r_balance_in_total', DecimalType(23, 8), True),
                   StructField('turn_deb_rub', DecimalType(23, 8), True),
                   StructField('r_turn_deb_rub', DecimalType(23, 8), True),
                   StructField('turn_deb_val', DecimalType(23, 8), True),
                   StructField('r_turn_deb_val', DecimalType(23, 8), True),
                   StructField('turn_deb_total', DecimalType(23, 8), True),
                   StructField('r_turn_deb_total', DecimalType(23, 8), True),
                   StructField('turn_cre_rub', DecimalType(23, 8), True),
                   StructField('r_turn_cre_rub', DecimalType(23, 8), True),
                   StructField('turn_cre_val', DecimalType(23, 8), True),
                   StructField('r_turn_cre_val', DecimalType(23, 8), True),
                   StructField('turn_cre_total', DecimalType(23, 8), True),
                   StructField('r_turn_cre_total', DecimalType(23, 8), True),
                   StructField('balance_out_rub', DecimalType(23, 8), True),
                   StructField('r_balance_out_rub', DecimalType(23, 8), True),
                   StructField('balance_out_val', DecimalType(23, 8), True),
                   StructField('r_balance_out_val', DecimalType(23, 8), True),
                   StructField('balance_out_total', DecimalType(23, 8), True),
                   StructField('r_balance_out_total', DecimalType(23, 8), True)
                            ]
    final_struc = StructType(fields = data_schema)

    df = spark.read.csv(f'/opt/airflow/files/{csv_name}.csv', 
        sep=';',
        header=True,
        schema=final_struc
        )

    df.write \
        .mode('overwrite') \
        .format("jdbc") \
        .option('driver', 'org.postgresql.Driver') \
        .option("url", url_dwh) \
        .option("dbtable", table_name) \
        .option("user", user_name) \
        .option("password", user_password) \
        .save()


default_args = {
         "owner": "dwh_user", 
         "start_date": datetime(2024, 6, 15),
         "retries":2
}

with DAG('fro_m_csv_101_f', 
          default_args = default_args,
          description = 'Загрузка 101 формы из csv',
          catchup = False,
          schedule = '0 0 1 1 *'
) as dag:
            
    start = DummyOperator(
            task_id = "start"
    )

    from_csv_101_f = PythonOperator(
            task_id = "from_csv_101_f",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "dm_f101_round_f"}
    )

    end = DummyOperator(
            task_id = "end"
    )

    (
        start 
        >> from_csv_101_f 
        >> end
    )
    