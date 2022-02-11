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
            s3_key1,
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
        self.s3_key1 = s3_key1
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
        self.log.info(self.s3_key1 + ', ' + self.s3_bucket)

        # Validate if the file source exist or not in the bucket.
         
        if self.wildcard_match:
            if not self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket) or not self.s3.check_for_wildcard_key(self.s3_key1, self.s3_bucket): 
                raise AirflowException("No key matches {0}".format(self.s3_key,self.s3_key1)) #,self.s3_key1
            s3_key_object = self.s3.get_wildcard_key(self.s3_key, self.s3_bucket)
            s3_key_object1 = self.s3.get_wildcard_key(self.s3_key1, self.s3_bucket)
        else:
            if not self.s3.check_for_key(self.s3_key, self.s3_bucket)or not self.s3.check_for_wildcard_key(self.s3_key1, self.s3_bucket): 
                raise AirflowException(
                    "The key {0} does not exists".format(self.s3_key,self.s3_key1)) #,self.s3_key1
                  
            s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)
            s3_key_object1 = self.s3.get_key(self.s3_key1, self.s3_bucket)

        # Read and decode the file into a list of strings.  
        list_srt_content = s3_key_object.get()['Body'].read().decode(encoding = "utf-8", errors = "ignore")
        list_srt_content = s3_key_object1.get()['Body'].read().decode(encoding = "utf-8", errors = "ignore")
        
        # schema definition for data types of the source.
        schema = {
                    'log_id': 'float',
                    'log': 'string'  
                 }  
        schema1 = {
                    'cid': 'float',
                    'review_str': 'string',
                    'id_review': 'float'  
                 }  
        
        # read a csv logs file with the properties required.
        df_logs = pd.read_csv(io.StringIO(list_srt_content), 
                         header=0, 
                         delimiter=",",
                         quotechar='"',
                         low_memory=False,                                         
                         dtype=schema                         
                         )
        self.log.info(df_logs)
        self.log.info(df_logs.info())

        # read a csv movies file with the properties required.
        df_movie = pd.read_csv(io.StringIO(list_srt_content), 
                         header=0, 
                         delimiter=",",
                         quotechar='"',
                         low_memory=False,                                         
                         dtype=schema1                         
                         )
        self.log.info(df_movie)
        self.log.info(df_movie.info())

        # formatting and converting the dataframe object in list to prepare the income of the next steps.
        df_movie = df_movie.replace(r"[\"]", r"'")
        list_df_movie = df_movie.values.tolist()
        list_df_movie = [tuple(x) for x in list_df_movie]
        self.log.info(df_movie.info())   
       
        # formatting and converting the dataframe object in list to prepare the income of the next steps.
        df_logs = df_logs.replace(r"[\"]", r"'")
        list_df_logs = df_logs.values.tolist()
        list_df_logs = [tuple(x) for x in list_df_logs]
        self.log.info(list_df_logs)   

        # Read the file with the DDL SQL to create the table products in postgres DB.
        #file_name_log = "bootcampdb.log_reviews.sql"
        #file_name_movie = "bootcampdb.movie_reviews.sql"
        
            #Display the content 
        SQL_COMMAND_CREATE_TBL = """
       CREATE SCHEMA IF NOT EXISTS bootcampdb;
       CREATE TABLE IF NOT EXISTS bootcampdb.log_review (
                id_review NUMERIC(100),
                log VARCHAR(1000);       
        CREATE TABLE IF NOT EXISTS bootcampdb.movie_review (
                cid NUMERIC(100),
                review_str VARCHAR(100),
                id_review NUMERIC(100); """
           
        
 # execute command to create table in postgres.  
        self.pg_hook.run(SQL_COMMAND_CREATE_TBL)  
        
        # set the columns to insert, in this case we ignore the id, because is autogenerate.
        list_target_fields = ['log']
        
        self.current_table = self.schema + '.' + self.table
        self.pg_hook.insert_rows(self.current_table,  
                                 list_df_logs, 
                                 target_fields = list_target_fields, 
                                 commit_every = 1000,
                                 replace = False)

        list_target_fields = ['review_str','id_review']
        
        self.current_table = self.schema + '.' + self.table
        self.pg_hook.insert_rows(self.current_table,  
                                 list_df_logs, 
                                 target_fields = list_target_fields, 
                                 commit_every = 1000,
                                 replace = False)
 
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

## Load the data to Postgres#
load_movie_files = S3ToPostgresTransfer(
                            task_id = 'dag_s3_to_postgres_review',
                            schema =  'bootcampdb', #'public'
                            table= 'log_reviews',
                            s3_bucket = 's3-data-bootcampfz',
                            s3_key = 'log_reviews.csv',
                            s3_key1 = 'movie_review.csv',
                            aws_conn_postgres_id = 'postgres_default',
                            aws_conn_id = 'aws_default', 
                            dag=dag
)

init >> load_movie_files >> end
