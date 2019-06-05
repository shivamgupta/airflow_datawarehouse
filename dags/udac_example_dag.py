from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator, PythonOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'shivam_gupta',
    'depends_on_past': False,
    'email': ['shivamvmc@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

main_dag = DAG('Project_5',
                description='Load and transform data in Redshift with Airflow',
                start_date=datetime(2018, 11, 1, 0, 0, 0, 0),
                end_date=datetime(2018, 11, 30, 0, 0, 0, 0),
                schedule_interval="@monthly",
                default_args=default_args,
                #catchup=False
           )

start_operator = DummyOperator(
                    task_id='Begin_Execution',
                    dag=main_dag
)

create_events_in_s3_task = PostgresOperator(
    task_id="Create_Events",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_events_table
)

copy_events_to_s3_task = StageToRedshiftOperator(
    task_id="Stage_Events",
    dag=main_dag,
    table_name="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/",
    s3_format="json",
    json_path="s3://udacity-dend/log_json_path.json"
)

create_songs_in_s3_task = PostgresOperator(
    task_id="Create_Songs",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_songs_table
)

copy_songs_to_s3_task = StageToRedshiftOperator(
    task_id="Stage_Songs",
    dag=main_dag,
    table_name="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    s3_format="json"
)

create_songplays_in_s3_task = PostgresOperator(
    task_id="Create_Songplays",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_songplays_table
)

load_songplays_in_s3_task = LoadFactOperator(
    task_id="Load_Songplays_Fact_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="songplays",
    sql=SqlQueries.songplay_table_insert,
    load_mode="append",
    filter_key=("{execution_date}", "{next_execution_date}")
)

create_song_in_s3_task = PostgresOperator(
    task_id="Create_Song_Dim_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_songs_table
)

create_user_in_s3_task = PostgresOperator(
    task_id="Create_User_Dim_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_users_table
)

create_artist_in_s3_task = PostgresOperator(
    task_id="Create_Artist_Dim_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_artist_table
)

create_time_in_s3_task = PostgresOperator(
    task_id="Create_Time_Dim_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_time_table
)

load_song_in_s3_task = LoadDimensionOperator(
    task_id="Load_Song_Dim_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="songs",
    sql=SqlQueries.song_table_insert,
    load_mode="clean"
)

load_user_in_s3_task = LoadDimensionOperator(
    task_id="Load_User_Dim_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="users",
    sql=SqlQueries.user_table_insert,
    load_mode="clean"
)

load_artist_in_s3_task = LoadDimensionOperator(
    task_id="Load_Artist_Dim_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="artists",
    sql=SqlQueries.artist_table_insert,
    load_mode="clean"
)

load_time_in_s3_task = LoadDimensionOperator(
    task_id="Load_Time_Dim_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="time",
    sql=SqlQueries.time_table_insert,
    load_mode="clean"
)

dq_check_task = DataQualityOperator(
    task_id="Run_Data_Quality_Checks",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_info_dict=[{"table_name": "users", "not_null": "userid"},     \
              {"table_name": "songs", "not_null": "songid"},            \
              {"table_name": "artists", "not_null": "artistid"},        \
              {"table_name": "time", "not_null": "start_time"},         \
              {"table_name": "songplays", "not_null": "playid"}         \
             ]
)

end_operator = DummyOperator(
                    task_id='End_Execution',
                    dag=main_dag
)

start_operator >> create_events_in_s3_task >> copy_events_to_s3_task >> create_songplays_in_s3_task 
start_operator >> create_songs_in_s3_task  >> copy_songs_to_s3_task >> create_songplays_in_s3_task 

create_songplays_in_s3_task >> load_songplays_in_s3_task

load_songplays_in_s3_task >> create_song_in_s3_task >> load_song_in_s3_task >> dq_check_task
load_songplays_in_s3_task >> create_user_in_s3_task >> load_user_in_s3_task >> dq_check_task
load_songplays_in_s3_task >> create_artist_in_s3_task >> load_artist_in_s3_task >> dq_check_task
load_songplays_in_s3_task >> create_time_in_s3_task >> load_time_in_s3_task >> dq_check_task

dq_check_task >> end_operator
