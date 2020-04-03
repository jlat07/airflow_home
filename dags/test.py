from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from datetime import datetime


args = {
    'owner:':'JT',
    'start_date':datetime.now(),
    'retries':3,
    'retry_delay':timedelta(minutes=1)
    }

dag = DAG(
    dag_id='test_dag',
    default_args=args,
    description= 'Testing Dag',
    )

t1 = BashOperator(
    task_id='t1',
    depends_on_past=False,
    bash_command='date',
    dag=dag,
    )

t3 = BashOperator(
    task_id='t2',
    depends_on_past=False,
    bash_command='echo "GoodBYe"',
    dag=dag,
    )

t2 = BashOperator(
    task_id='t3',
    depends_on_past=False,
    bash_command='echo "something else"',
    dag=dag,
    )

t1 >> t2 >> t3
