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

s3_to_postgres_operator = S3ToPostgresTransfer(
                           task_id = 'dag_s3_to_postgres',
                            schema =  'dbname', #'public'
                            table= 'user_purchase',
                           s3_bucket = 's3-data-bootcampfz',
                           s3_key =  'user_purchase.csv',
                           aws_conn_postgres_id = 'postgres_default',
                            aws_conn_id = 'aws_default',   
                           dag = dag
)

s3_to_postgres_operator
