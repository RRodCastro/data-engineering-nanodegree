from datetime import datetime, timedelta
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ( DataQualityOperator, LoadToRedshiftOperator)
from airflow.operators import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
start_date = datetime.utcnow()

default_args = {
    'owner': 'rod',
    'start_date': start_date,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('etlInmigration',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
SQL = '/home/workspace/airflow/create_tables.sql'
fd = open(SQL)
sql_file = fd.read()
fd.close()


create_redshift_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_file
)

stage_visa_codes = LoadToRedshiftOperator (
    task_id='Visa_codes',
    table='visa_codes',
    file="visa_codes.csv",
    dag=dag
)

visa_codes_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_visa_codes",
    redshift_conn_id="redshift",
    table='visa_codes',
    dag=dag
)

stage_cities_demographics = LoadToRedshiftOperator (
    task_id='us_cities_demographics',
    table='us_cities_demographics',
    file="us_cities_demographics.csv",
    dag=dag,
    delimiter=";"
)

cities_demographics_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_cities_demographics",
    redshift_conn_id="redshift",
    table='us_cities_demographics',
    dag=dag
)

stage_state_codes = LoadToRedshiftOperator (
    task_id='states_codes',
    table='states_codes',
    file="states_codes.csv",
    dag=dag
)

state_codes_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_stage_state_codes",
    redshift_conn_id="redshift",
    table='states_codes',
    dag=dag
)

stage_country_codes = LoadToRedshiftOperator (
    task_id='country_codes',
    table='country_codes',
    file="country_codes.csv",
    dag=dag,
    delimiter=";"
)

country_codes_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_country_codes",
    redshift_conn_id="redshift",
    table='country_codes',
    dag=dag
)

stage_airport_codes = LoadToRedshiftOperator (
    task_id='airport_codes',
    table='airport_codes',
    file="airport_codes.csv",
    dag=dag
)

airport_codes_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_airport_codes",
    redshift_conn_id="redshift",
    table='airport_codes',
    dag=dag
)


stage_inmigrants_data = LoadToRedshiftOperator (
    task_id='inmigrants_data',
    table='immigration',
    file="sas_data/",
    dag=dag,
    is_parquet_file=True
)

immigration_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_immigration",
    redshift_conn_id="redshift",
    table='immigration',
    dag=dag
)


start_operator >> create_redshift_tables 
create_redshift_tables >> stage_visa_codes >>  visa_codes_data_quality
create_redshift_tables >> stage_cities_demographics >> cities_demographics_data_quality
create_redshift_tables >> stage_state_codes >> state_codes_data_quality
create_redshift_tables >> stage_country_codes >> country_codes_data_quality
create_redshift_tables >> stage_airport_codes >> airport_codes_data_quality
create_redshift_tables >> stage_inmigrants_data >> immigration_data_quality


