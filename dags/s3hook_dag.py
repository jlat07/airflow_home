from airflow.operators import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta


schedule = timedelta(minutes=5)
default_args={
    'owner': 'JLAT',
    'start_date': datetime.now(),
    'retries': 5,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='s3_key_sensor_Oura_Bucket',
    schedule_interval=schedule, 
    default_args=default_args
)

def new_file_detection(**kwargs):
    print("A new file has arrived in s3 bucket")

print_message = PythonOperator(task_id='print_message',
    provide_context=True,
    python_callable=new_file_detection,
    dag=dag
)

#My data file name has the following format "datatfile_YYYY_MM_DD.json"; this file arrives in S3 every day.
file_suffix = "{{ execution_date.strftime('%Y-%m-%d') }}"
bucket_key_template = 's3://[bucket_name]/datatfile_{}.json'.format(file_suffix)
    
file_sensor = S3KeySensor(
    task_id='s3_key_sensor_task',
    poke_interval=60 * 60, # (seconds); checking file every hour
    timeout=60 * 60 * 12, # timeout in 12 hours
    bucket_key=bucket_key_template,
    bucket_name=None,
    wildcard_match=False,
    dag=dag
)



file_sensor >> print_message

