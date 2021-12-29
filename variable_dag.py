from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

def display_variable():
    my_var = Variable.get("my_var")
    print('variable' + my_var)
    return my_var

dag = DAG(dag_id="variable_dag", start_date=datetime(2021, 1, 1),
    schedule_interval='@daily', catchup=False)

task = PythonOperator(task_id='display_variable', python_callable=display_variable, dag=dag)

task 