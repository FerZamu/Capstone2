# The DAG object; we'll need this to instantiate a DAG
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import os.path
import pandas as pd
import io


# dummies
init = DummyOperator(task_id='init', on_success_callback=get_init_success_datetime)
end = DummyOperator(task_id='end', on_success_callback=send_success_notification)

## DAG NAME ##          
dag = DAG('dag_insert_data', 
          description='Inser Data from CSV To Postgres',
          schedule_interval='@once',        
          start_date=datetime(2021, 10, 1),
          catchup=False)

init >> end
