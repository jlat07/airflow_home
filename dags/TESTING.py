"""
S3 Sensor Connection Test
"""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import S3KeySensor
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import timedelta


default_args = {
    'owner': 'JLAT',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='s3_senso',
    default_args=default_args,
    description='Sensor checks if daily file has been loaded to S3',
    schedule_interval= timedelta(days=1)
)

t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "hello, it should work" > s3_conn_test.txt',
    dag=dag
)

sensor = S3KeySensor(
    task_id='s3_file_check',
    bucket_key='file-to-watch-*',
    wildcard_match=True,
    bucket_name='ouraringbackupdata',
    s3_conn_id='my_conn_S3',
    timeout=18*60*60,
    poke_interval=120,
    dag=dag
)

t1.set_upstream(sensor)
