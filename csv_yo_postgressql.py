dimport airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime

"""
Load CSV > Postgres in GCP Cloud SQL Instance
"""


#default arguments 

default_args = {
    'owner': 'fernanda.zamudio',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 1),
    'email': ['fernanda.zamudio@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

#name the DAG and configuration
dag = DAG('insert_data_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()
    # CSV loading to table
    with open(file_path("/Downloads/user_purchase.csv"), "r") as f:
        next(f)
        curr.copy_from(f, 'user_purchase', sep=",")
        get_postgres_conn.commit()

#Task 
task1 = PostgresOperator(task_id = 'create_table',
                        sql="""
                      CREATE TABLE IF NOT EXISTS user_purchase (
                          invoice_number VARCHAR(10),
                          stock_code VARCHAR(20),
                          detail VARCHAR(1000),
                          quantity INTEGER,
                          invoice_date timestamp,
                          unit_price INTEGER,
                          customer_id VARCHAR(20),
                          country VARCHAR(20));
                            """,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task2 = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)


task1 >> task2
