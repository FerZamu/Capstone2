from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
#from airflow.providers.postgres.hooks.postgres.PostgresHook import PostgresHook
#from airflow.providers.amazon.aws.hooks.s3.S3Hook import S3Hook  
from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import os.path
import pandas as pd
import io

# Operators; we need this to operate!
#from custom_modules.operator_s3_to_postgres import S3ToPostgresTransfer

def print_welcome():
    return 'Welcome from custom operator - Airflow DAG!'

class S3ToPostgresTransfer(BaseOperator):
   
    template_fields = ()

    template_ext = ()

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            schema,
            table,
            s3_bucket,
            s3_key,
            aws_conn_postgres_id ='postgres_default',
            aws_conn_id='aws_default',
            verify=None,
            wildcard_match=False,
            copy_options=tuple(),
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(S3ToPostgresTransfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_postgres_id  = aws_conn_postgres_id 
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.wildcard_match = wildcard_match
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters
  
    def execute(self, context):

        self.log.info('Into the custom operator S3ToPostgresTransfer')
        
        #Create an instances to connect S3 and Postgres DB.
        self.log.info(self.aws_conn_postgres_id)   
        
        self.pg_hook = PostgresHook(postgre_conn_id = self.aws_conn_postgres_id)
        self.log.info("Init PostgresHook..")
        self.s3 = S3Hook(aws_conn_id = self.aws_conn_id, verify = self.verify)

        self.log.info("Downloading S3 file")
        self.log.info(self.s3_key + ', ' + self.s3_bucket)

        # Validate if the file source exist or not in the bucket.
        if self.wildcard_match:
            if not self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket):
                raise AirflowException("No key matches {0}".format(self.s3_key))
            s3_key_object = self.s3.get_wildcard_key(self.s3_key, self.s3_bucket)
        else:
            if not self.s3.check_for_key(self.s3_key, self.s3_bucket):
                raise AirflowException(
                    "The key {0} does not exists".format(self.s3_key))
                  
            s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)

        # Read and decode the file into a list of strings.  
        list_srt_content = s3_key_object.get()['Body'].read().decode(encoding = "utf-8", errors = "ignore")
        
        # schema definition for data types of the source.
        schema = {
                    'invoice_number': 'string',
                    'stock_code': 'string',
                    'detail': 'string',
                    'quantity': 'float',
                    'unit_price': 'float',                                
                    'customer_id': 'float',
                    'country': 'string'
                    
                 }  
        #date_cols = ['fechaRegistro']    
        custom_date_parser = lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M")

        # read a csv file with the properties required.
        df_products = pd.read_csv(io.StringIO(list_srt_content), 
                         header=0, 
                         delimiter=",",
                         quotechar='"',
                         low_memory=False,
                         parse_dates=["InvoiceDate"],
                         date_parser=custom_date_parser,                                           
                         dtype=schema                         
                         )
        self.log.info(df_products)
        self.log.info(df_products.info())

        # formatting and converting the dataframe object in list to prepare the income of the next steps.
        df_products = df_products.replace(r"[\"]", r"'")
        list_df_products = df_products.values.tolist()
        list_df_products = [tuple(x) for x in list_df_products]
        self.log.info(list_df_products)   
       
        # Read the file with the DDL SQL to create the table products in postgres DB.
        nombre_de_archivo = "bootcampdb.user_purchase.sql"
        
        #ruta_archivo = os.path.sep + nombre_de_archivo
        #self.log.info(ruta_archivo)
        #proposito_del_archivo = "r" #r es de Lectura
        #codificación = "UTF-8" #Tabla de Caracteres,
                               #ISO-8859-1 codificación preferidad por
                               #Microsoft, en Linux es UTF-8
        
        #with open(ruta_archivo, proposito_del_archivo, encoding=codificación) as manipulador_de_archivo:
       
            #Read dile with the DDL CREATE TABLE
         #   SQL_COMMAND_CREATE_TBL = manipulador_de_archivo.read()
         #   manipulador_de_archivo.close()

            #Display the content 
        
        if self.wildcard_match:
            if not self.s3.check_for_wildcard_key(nombre_de_archivo, self.s3_bucket):
                raise AirflowException("No key matches {0}".format(nombre_de_archivo))
            s3_key_object = self.s3.get_wildcard_key(nombre_de_archivo, self.s3_bucket)
        else:
            if not self.s3.check_for_key(nombre_de_archivo, self.s3_bucket):
                raise AirflowException(
                    "The key {0} does not exists".format(nombre_de_archivo))
                  
            s3_sql_key = self.s3.get_key(nombre_de_archivo, self.s3_bucket)
            self.log.info(s3_sql_key)
        SQL_COMMAND_CREATE_TBL = s3_sql_key.get()["Body"].read().decode(encoding = "utf-8", errors = "ignore")    
        self.log.info(io.StringIO(SQL_COMMAND_CREATE_TBL))  
        

        ############# 
            
           

dag = DAG('dag_insert_data', 
          description='Inser Data from CSV To Postgres',
          schedule_interval='@once',        
          start_date=datetime(2021, 10, 1),
          catchup=False)

welcome_operator = PythonOperator(task_id='welcome_task', 
                                  python_callable=print_welcome, 
                                  dag=dag)

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

#welcome_operator.set_downstream(s3_to_postgres_operator)
