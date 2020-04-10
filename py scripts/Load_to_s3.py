from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import logging
from botocore.exceptions import ClientError


def upload_file(file_name, bucket, object_name=None):
    """
    upload file to an S3 bucket
    :param file_name: file to be upkoaded
    :param bucket:
    :param object_name:
    :return:
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def insert_data():
    upload_file("/Users/jthompson/dev/airflow_home/data/oura_*", 'ouraringbackupdata', 'Oura_Data')


default_args = {
    'owner': 'JLAT',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['jlat0128@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG(
    dag_id='load_to_S3',
    default_args=default_args,
    description='Extract data from API, then load to s3',
    schedule_interval=timedelta(days=1)
)

t2 = PythonOperator(
        task_id='load_to_S3',
        provide_context=False,
        python_callable=insert_data,
        dag=dag
)
# t1 >> t2