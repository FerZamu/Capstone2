# The DAG object; we'll need this to instantiate a DAG
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import os.path
import io

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
        
        # schema definition for data types of the source. # Modificar
        if (self.table =="log_reviews"):
                schema = {
                                'log': 'string'
                         }
        if (self.table =="movie_reviews"): 
                schema = {
                                'cid': 'float',
                                'review_str': 'string'
                         }
        if (self.table =="user_purchase"): 
                schema = {
                                'InvoiceNo': 'float',
                                'StockCode': 'float',
                                'Description': 'string',
                                'Quantity': 'float',
                                'InvoiceDate': 'string',
                                'UnitPrice': 'float',
                                'CustomerID': 'float',
                                'Country': 'string',
                        }  
        return schema
            
        custom_date_parser = lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M")
        
        # read a csv file with the properties required.
        df_columns = pd.read_csv(io.StringIO(list_srt_content), 
                         header=0, 
                         delimiter=",",
                         quotechar='"',
                         low_memory=False,
                         parse_dates=["InvoiceDate"],
                         date_parser=custom_date_parser,                                              
                         dtype=schema                         
                         )
        self.log.info(df_columns)
        self.log.info(df_columns.info())

        # formatting and converting the dataframe object in list to prepare the income of the next steps.
        df_columns = df_columns.replace(r"[\"]", r"'")
        list_df_columns = df_columns.values.tolist()
        list_df_columns = [tuple(x) for x in list_df_columns]
        self.log.info(list_df_columns)   
       
        # Read the file with the DDL SQL to create the table products in postgres DB.
        #Ya esta definido
        
        # set the columns to insert, in this case we ignore the id, because is autogenerate.
        
        if (schema.tables == "log_reviews"):
            list_target_fields = ['log']
        if (schema.tables == "movie_reviews"):
            list_target_fields = ['cid',
                                  'review_str'
                                 ]
        if (schema.tables == "user_purchase"):
             list_target_fields = ['invoice_number', 
                               'stock_code',
                               'detail', 
                               'quantity', 
                               'invoice_date', 
                               'unit_price', 
                               'customer_id', 
                               'country']
        
        self.current_table = self.schema + '.' + self.table
        self.pg_hook.insert_rows(self.current_table,  
                                 list_df_columns, 
                                 target_fields = list_target_fields, 
                                 commit_every = 1000,
                                 replace = False)
        #Check the load
        self.request = 'SELECT * FROM ' + self.current_table
        self.log.info(self.request).limit(5)

 
## DAG NAME ##          
with DAG('Movie_reviews', 
          description='Start ETL process with movies',
          schedule_interval='@once',        
          start_date=datetime(2021, 10, 1),
          catchup=False
) as dag:

## Dummies ##
    init = DummyOperator(task_id='init')
    end = DummyOperator(task_id='end')

#Create tables
    create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    #sql in same directory
    sql= """ 
    CREATE SCHEMA IF NOT EXISTS bootcampdb;
    CREATE TABLE IF NOT EXISTS bootcampdb.user_purchases (
    invoice_number VARCHAR(10),
    stock_code VARCHAR(20),
    detail VARCHAR(1000),
    quantity BIGINT,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(8,3),
    customer_id VARCHAR(20),
    country VARCHAR(20) 
    );

    CREATE TABLE IF NOT EXISTS bootcampdb.log_reviews (
    log VARCHAR(1000)
    );

    CREATE TABLE IF NOT EXISTS bootcampdb.movie_reviews (
    cid VARCHAR(100),
    review_str VARCHAR(1000)    
    );""",
    postgres_conn_id='postgres_default'
)

#LOAD data tables

load_log_reviews = S3ToPostgresTransfer(
                            task_id = 'load_log_reviews',
                            schema =  'bootcampdb',
                            table= 'log_reviews',
                            s3_bucket = 'raw-movie-data',
                            s3_key = 'log_reviews.csv',
                            aws_conn_postgres_id = 'postgres_default',
                            aws_conn_id = 'aws_default'
)                           

load_movie_reviews = S3ToPostgresTransfer(
                            task_id = 'load_movie_reviews',
                            schema =  'bootcampdb',
                            table= 'movie_reviews',
                            s3_bucket = 'raw-movie-data',
                            s3_key = 'movie_review.csv',
                            aws_conn_postgres_id = 'postgres_default',
                            aws_conn_id = 'aws_default' 
                           
)
load_user_purchase = S3ToPostgresTransfer(
                            task_id = 'load_user_purchase',
                            schema =  'bootcampdb',
                            table= 'user_purchase',
                            s3_bucket = 'raw-movie-data',
                            s3_key = 'user_purchase.csv',
                            aws_conn_postgres_id = 'postgres_default',
                            aws_conn_id = 'aws_default' 
     
)   
init >> create_tables_task >> [load_log_reviews,load_movie_reviews,load_user_purchase] >> end
