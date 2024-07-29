from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas
from datetime import datetime


def insert_data(csv_name, t_table_name):
    df = pandas.read_csv(f'/opt/airflow/files/{csv_name}.csv', delimiter=';')
    postgres_hook = PostgresHook("dwh")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(t_table_name, engine, schema="ds", if_exists="replace", index=False)

def save_data(csv_name):
    postgres_hook = PostgresHook("dwh")
    engine = postgres_hook.get_sqlalchemy_engine()
    df = pandas.read_sql_query("""select from_date, 
                                         to_date,
                                         chapter,
                                         ledger_account,
                                         characteristic,
                                         balance_in_rub::text,
                                         r_balance_in_rub, 
                                         balance_in_val::text,
                                         r_balance_in_val,
                                         balance_in_total::text,
                                         r_balance_in_total,
                                         turn_deb_rub::text,
                                         r_turn_deb_rub,
                                         turn_deb_val::text,
                                         r_turn_deb_val,
                                         turn_deb_total::text,
                                         r_turn_deb_total,
                                         turn_cre_rub::text,
                                         r_turn_cre_rub,
                                         turn_cre_val::text,
                                         r_turn_cre_val,
                                         turn_cre_total::text,
                                         r_turn_cre_total,
                                         balance_out_rub::text,
                                         r_balance_out_rub,
                                         balance_out_val::text,
                                         r_balance_out_val,
                                         balance_out_total::text,
                                         r_balance_out_total
                                  from dm.dm_f101_round_f
                                  order by ledger_account""", engine)
    df.to_csv(f'/opt/airflow/files/{csv_name}.csv', sep=';', header=True, encoding='UTF8', index=False)

default_args = {
         "owner": "dwh_user", 
         "start_date": datetime(2024, 6, 15),
         "retries":2
}

with DAG('data_engineer_project', 
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
            op_kwargs = {"csv_name": "ft_balance_f", "t_table_name": "t_balance_f"}
    )

    ft_posting_f = PythonOperator(
            task_id = "ft_posting_f",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "ft_posting_f", "t_table_name": "t_posting_f"}
    )

    md_account_d = PythonOperator(
            task_id = "md_account_d",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "md_account_d", "t_table_name": "t_account_d"}
    )

    md_currency_d = PythonOperator(
            task_id = "md_currency_d",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "md_currency_d", "t_table_name": "t_currency_d"}
    )

    md_exchange_rate_d = PythonOperator(
            task_id = "md_exchange_rate_d",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "md_exchange_rate_d", "t_table_name": "t_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
            task_id = "md_ledger_account_s",
            python_callable = insert_data,
            op_kwargs = {"csv_name": "md_ledger_account_s", "t_table_name": "t_ledger_account_s"}
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
            op_kwargs = {"csv_name": "dm_f101_round_f"}
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
        >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s]
        >> fill_all_table
        >> fill_jan_2018
        >> fill_101_jan_2018
        >> save_to_csv
        >> log_end
        >> end
    )
   
    