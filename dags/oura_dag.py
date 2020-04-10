from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import S3KeySensor
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import os


def read_csv():
    '''
    Read Oura Trend Data (sleep, activity, and readiness)
    '''
    df = pd.read_csv('/Users/jthompson/dev/airflow_home/data/oura_2019-09-17_2020-04-01_trends.csv')

def transform(): 
    '''
    Transform data
    '''
    df['Bedtime Start'] = pd.to_datetime(df['Bedtime Start'], utc=True)
    df['Bedtime End'] = pd.to_datetime(df['Bedtime End'], utc=True)

def update_db():
    '''
    Loads data into postgre DB
    '''
    engine = create_engine('postgresql+psycopg2://jthompson:@localhost:5432/jthompson')
    df.to_sql(name='oura_trends', con=engine, schema='oura_data', if_exists='replace')


default_args = {
    'owner': 'JLAT',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['jlat0128@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

"""
dag.doc_md =
#### Update DB
Update DB takes in the csv downloaded from oura and loads it into postgres database
as soon as dag is triggered by file landing in s3 bucket
"""

dag = DAG(
    dag_id='oura_pipeline',
    default_args=default_args,
    description='ETL',
    schedule_interval= timedelta(days=1)
)

sensor = S3KeySensor(
    task_id='s3_file_check',
    bucket_key='oura_*',
    wildcard_match=True,
    bucket_name='ouraringbackupdata',
    s3_conn_id='my_conn_S3',
    timeout=18*60*60,
    poke_interval=120,
    dag=dag
)

t1 = PythonOperator(
        task_id='read_csv',
        provide_context=False,
        python_callable=read_csv,
        dag=dag
)

t2 = PythonOperator(
        task_id='transform_data',
        provide_context=False,
        python_callable=transform,
        dag=dag
)

t3 = PythonOperator(
        task_id='Load_to_postgre',
        provide_context=False,
        python_callable=update_db,
        dag=dag
)

sensor >> t1 >> t2 >> t3
