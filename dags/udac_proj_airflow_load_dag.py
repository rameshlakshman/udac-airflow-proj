from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from helpers import SqlQueries
from airflow.models import Variable
import ast

def chk_param(s):
    """used for safely evaluating strings containing Python values from 
    untrusted sources without the need to parse the values oneself
    """
    return ast.literal_eval(s.capitalize())

"""S3 source bucket & key information below
"""
bucket = 'udacity-dend'
song_key = 'song_data/'
event_key = 'log_data/'
log_jsonpath='log_json_path.json'

#default_args below
default_args = {
    'owner': 'Ramesh-L',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False # Run only the last DAG
}

#dag declared here
dag = DAG('Project-4-S3-TO-REDSHIFT',
          default_args=default_args,
          description='ELT in Redshift with Airflow',
          schedule_interval='@hourly',
        )

def begin_execution():
    """ Print out all data from the buckets
    """
    logging.info("Begin Execution")
    hook = S3Hook(aws_conn_id='aws_credentials')
    logging.info(f"Listing Keys from {bucket}/{song_key}")
    keys = hook.list_keys(bucket, prefix=song_key)
    read = False
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")
        if read == False and key[-4:].upper() == 'JSON':
            content = hook.read_key(key, bucket)
            read = True

    logging.info(f"Listing Keys from {bucket}/{event_key}")
    keys = hook.list_keys(bucket, prefix=event_key)
    read = False
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")
        if read == False and key[-4:].upper() == 'JSON':
            content = hook.read_key(key, bucket)
            read = True

start_operator = PythonOperator(
    task_id='Begin_execution',
    dag=dag,
    python_callable=begin_execution)

#Below task will load the S3 log_data into the staging_events table
load_events_to_redshift_stg = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket=bucket,
    s3_key_path=event_key,
    s3_file_type='json',
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift',
    redshift_iam_role='myRdshftRole',
    table='staging_events',
    create_query=SqlQueries.staging_events_table_create,
    copy_query=SqlQueries.staging_events_copy,
    jsonpath='s3://udacity-dend/log_json_path.json',
    active=chk_param(Variable.get('run_staging'))
)

#Below task will load the S3 data into the staging_songs table
load_songs_to_redshift_stg = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket=bucket,
    s3_key_path=song_key,
    s3_file_type='json',
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift',
    redshift_iam_role='myRdshftRole',
    table='staging_songs',
    create_query=SqlQueries.staging_songs_table_create,
    copy_query=SqlQueries.staging_songs_copy,
    jsonpath='auto',
    active=chk_param(Variable.get('run_staging'))
)

#Below task will load the songplays FACT table from the staging data
load_fct_songplays_data = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='myRdshftRole',
    table='songplays',
    create_query=SqlQueries.songplay_table_create,
    insert_query=SqlQueries.songplay_table_insert
)

#Below task will load the dimesnion table USERS
load_dim_user_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='myRdshftRole',
    table='users',
    create_query=SqlQueries.user_table_create,
    insert_query=SqlQueries.user_table_insert,
    #append_only=chk_param(Variable.get('append_only'))
)

#Below task will load the dimesnion table SONGS
load_dim_song_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='myRdshftRole',
    table='songs',
    create_query=SqlQueries.song_table_create,
    insert_query=SqlQueries.song_table_insert,
    #append_only=chk_param(Variable.get('append_only'))
)

#Below task will load the dimesnion table ARTISTS
load_dim_artists_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='myRdshftRole',
    table='artists',
    create_query=SqlQueries.artist_table_create,
    insert_query=SqlQueries.artist_table_insert,
    #append_only=chk_param(Variable.get('append_only'))
)

#Below task will load the dimesnion table TIME
load_dim_time_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    redshift_iam_role='myRdshftRole',
    table='time',
    create_query=SqlQueries.time_table_create,
    insert_query=SqlQueries.time_table_insert,
    #append_only=chk_param(Variable.get('append_only'))
)

#Below task will do data-quality check to see if table has rows
data_quality_rowcount_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    redshift_iam_role='myRdshftRole',   
    #tables=Variable.get('tables_list', deserialize_json=True),
    tables='songplays',
    dag=dag
)

#Below task denotes the end of the process
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Below lines control the dependencies of various tasks associated in this process
#begin, then load staging tables, then load dimension table, then load all facts table in parallel,
#and then data-quality checks, and stop-execution.
start_operator >> load_events_to_redshift_stg
start_operator >> load_songs_to_redshift_stg

load_events_to_redshift_stg >> load_fct_songplays_data
load_songs_to_redshift_stg >> load_fct_songplays_data

load_fct_songplays_data >> load_dim_song_table
load_fct_songplays_data >> load_dim_user_table
load_fct_songplays_data >> load_dim_artists_table
load_fct_songplays_data >> load_dim_time_table

load_dim_song_table >> data_quality_rowcount_checks
load_dim_user_table >> data_quality_rowcount_checks
load_dim_artists_table >> data_quality_rowcount_checks
load_dim_time_table >> data_quality_rowcount_checks

data_quality_rowcount_checks >> end_operator
