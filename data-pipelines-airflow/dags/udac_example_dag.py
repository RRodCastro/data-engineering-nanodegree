from datetime import datetime, timedelta
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                DataQualityOperator, LoadDimensionOperator)
from airflow.operators import PostgresOperator

from helpers import SqlQueries
from airflow.operators.subdag_operator import SubDagOperator

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

start_date = datetime.utcnow()

default_args = {
    'owner': 'rod',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

test = '/home/workspace/airflow/create_tables.sql'
fd = open(test)
sql_file = fd.read()
fd.close()

create_redshift_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_file
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    copy_json_option='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    copy_json_option='auto',
    region='us-west-2',
    dag=dag
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert,
    table='songplays'
)

load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        table="users",
        redshift_conn_id='redshift',
        aws_credentials_id="aws_credentials",
        insert_query=SqlQueries.user_table_insert
    )

load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        table="songs",
        redshift_conn_id='redshift',
        aws_credentials_id="aws_credentials",
        insert_query=SqlQueries.song_table_insert
    )

load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        table="artists",
        redshift_conn_id='redshift',
        aws_credentials_id="aws_credentials",
        insert_query=SqlQueries.artist_table_insert,
        truncate=True,
        primary_key="artistid"
    )

load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        table="time",
        redshift_conn_id='redshift',
        aws_credentials_id="aws_credentials",
        insert_query=SqlQueries.time_table_insert,
    )


songplays_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_songplays",
    redshift_conn_id="redshift",
    table='songplays',
    dag=dag
)

artists_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_artists",
    redshift_conn_id="redshift",
    table='artists',
    dag=dag
)

users_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_users",
    redshift_conn_id="redshift",
    table='users',
    dag=dag
)

songs_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_songs",
    redshift_conn_id="redshift",
    table='songs',
    dag=dag
)

time_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_time",
    redshift_conn_id="redshift",
    table='time',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



# Depdencies


start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                           load_time_dimension_table]

load_songplays_table >> songplays_data_quality
load_artist_dimension_table >> artists_data_quality
load_user_dimension_table >> users_data_quality
load_song_dimension_table >> songs_data_quality
load_time_dimension_table >> time_data_quality

[songplays_data_quality, artists_data_quality, users_data_quality, songs_data_quality,    time_data_quality] >> end_operator
