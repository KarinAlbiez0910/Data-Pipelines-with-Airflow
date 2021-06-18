from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import logging

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = PostgresOperator(
  task_id="start_execution",
  dag=dag,
  sql="create_tables.sql",
  postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
                            task_id='Stage_events',
                            dag=dag, 
                            redshift_conn_id='redshift',
                            aws_credentials_id='aws_credentials',
                            table='public.staging_events',
                            s3_bucket='s3://udacity-dend/log_data',
                            aws_iam_role='arn:aws:iam::133319634930:role/dwhRole',
                            json_path='s3://udacity-dend/log_json_path.json',
                            region='us-west-2'
                           )




stage_songs_to_redshift = StageToRedshiftOperator(
                                                    task_id='Stage_songs',
                                                    dag=dag,  
                                                    redshift_conn_id='redshift',
                                                    aws_credentials_id='aws_credentials',
                                                    table='public.staging_songs',
                                                    s3_bucket='s3://udacity-dend/song_data',
                                                    aws_iam_role='arn:aws:iam::133319634930:role/dwhRole',
                                                    json_path='auto',
                                                    region='us-west-2'
                                                  )

load_songplays_table = LoadFactOperator(
                                         task_id='Load_songplays_fact_table',
                                         dag=dag, 
                                         redshift_conn_id="redshift",
                                         fact_table="songplays",
                                         insert_columns="start_time, userid, level, songid, artistid, sessionid, location, user_agent",
                                         select_columns="timestamp 'epoch' + events.ts/1000 * interval '1 second' as start_time,\
                                                         events.userId,\
                                                         events.level,\
                                                         songs.song_id,\
                                                         songs.artist_id,\
                                                         events.sessionId,\
                                                         events.location,\
                                                         events.userAgent",
                                         staging_tables="staging_events AS events\
                                                         JOIN staging_songs AS songs\
                                                         ON (events.artist = songs.artist_name)\
                                                         AND (events.song = songs.title)\
                                                         AND (events.length = songs.duration)",
                                         condition="events.page = 'NextSong'",
                                         truncate=True,
)

load_user_dimension_table = LoadDimensionOperator(
                                                    task_id='Load_user_dim_table',
                                                    dag=dag, 
                                                    redshift_conn_id="redshift",
                                                    table="users",
                                                    insert_columns="userid, first_name, last_name, gender, level",
                                                    select_columns="userId AS userid,\
                                                                    firstName AS first_name,\
                                                                    lastName AS last_name,\
                                                                    gender,\
                                                                    level",
                                                   staging_table="staging_events WHERE page='NextSong'",
                                                   truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
                                                      task_id='Load_song_dim_table',
                                                      dag=dag, 
                                                      redshift_conn_id="redshift",
                                                      table="songs",
                                                      insert_columns="songid, title, artistid, year, duration",
                                                      select_columns="song_id, title, artist_id as artistid, year, duration",
                                                      staging_table="staging_songs",
                                                      truncate=True,
                                                 )

load_artist_dimension_table = LoadDimensionOperator(
                                                    task_id='Load_artist_dim_table',
                                                    dag=dag, 
                                                    redshift_conn_id="redshift",
                                                    table="artists",
                                                    insert_columns="artistid, name, location, lattitude, longitude",
                                                    select_columns="artist_id AS artistid, artist_name AS name,\
                                                                    artist_location AS location,\
                                                                    artist_latitude AS lattitude,\
                                                                    artist_longitude AS artist_longitude",
                                                   staging_table="staging_songs",
                                                   truncate=True
                                                   )

load_time_dimension_table = LoadDimensionOperator(
                                                    task_id='Load_time_dim_table',
                                                    dag=dag, 
                                                    redshift_conn_id="redshift",
                                                    table="time",
                                                    insert_columns="start_time, hour, day, week, month, year, weekday",
                                                    select_columns="a.start_time,\
                                                                    EXTRACT (HOUR FROM a.start_time),\
                                                                    EXTRACT (DAY FROM a.start_time),\
                                                                    EXTRACT (WEEK FROM a.start_time), \
                                                                    EXTRACT (MONTH FROM a.start_time),\
                                                                    EXTRACT (YEAR FROM a.start_time), \
                                                                    EXTRACT (WEEKDAY FROM a.start_time)",
                                                   staging_table="(SELECT TIMESTAMP 'epoch' + staging_events.ts/1000 *INTERVAL '1 second' as\
                                                                   start_time FROM staging_events) a",
                                                   truncate=True
                                                   )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["staging_events", "staging_songs", "artists", "songs", "users", "time", "songplays"]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
