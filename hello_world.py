from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from custom_modules.operator_s3_to_postgres import S3ToPostgresTransfer

def print_hello():
    welcome = 'Hello world from first Airflow DAG!'
    return welcome

dag = DAG('hello_world', 
          description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator
