from datetime import timedelta, datetime
import urllib.request
import json


today = datetime.today()
today = today.isoformat()
prior = datetime.today() - timedelta(days=1)
prior = prior.isoformat()


default_args={
    'owner': 'JLAT',
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='get_oura_data',
    default_args = default_args,
    description = 'Get Oura Data',
    )

def get_sleep_data():

    url = 'https://api.ouraring.com/v1/sleep' + f'?start={prior}&end={today}'
    headers = {
      'Authorization': 'Bearer NGGKZHFPETIHLJ7QTZ6263BBZE2VWOXG',
      'Cookie': '_ga=GA1.2.1358351741.1586059502; _gid=GA1.2.1601331497.1586059502'
    }
    req = urllib.request.Request(url, headers=headers)

    file_name = '/Users/jthompson/dev/airflow_home/data/sleep_data.json'
    
    with urllib.request.urlopen(req) as f:
        data = json.load(f)
        
        with open(file_name, 'w') as handler:
            json.dump(data, handler)


t1 = PythonOperator(
    task_id = 'get_sleep_data',
    python_callable = get_sleep_data,
    dag = dag,
    )


def get_activity_data():

    url = 'https://api.ouraring.com/v1/activity' + f'?start={prior}&end={today}'
    headers = {
      'Authorization': 'Bearer NGGKZHFPETIHLJ7QTZ6263BBZE2VWOXG',
      'Cookie': '_ga=GA1.2.1358351741.1586059502; _gid=GA1.2.1601331497.1586059502'
    }
    req = urllib.request.Request(url, headers=headers)

    file_name = '/Users/jthompson/dev/airflow_home/data/activity_data.json'
    
    with urllib.request.urlopen(req) as f:
        data = json.load(f)
        
        with open(file_name, 'w') as handler:
            json.dump(data, handler)


t2 = PythonOperator(
    task_id = 'get_activity_data',
    python_callable = get_activity_data,
    dag = dag,
    )


def get_readiness_data():

    url = 'https://api.ouraring.com/v1/readiness' + f'?start={prior}&end={today}'
    headers = {
      'Authorization': 'Bearer NGGKZHFPETIHLJ7QTZ6263BBZE2VWOXG',
      'Cookie': '_ga=GA1.2.1358351741.1586059502; _gid=GA1.2.1601331497.1586059502'
    }
    req = urllib.request.Request(url, headers=headers)

    file_name = '/Users/jthompson/dev/airflow_home/data/readiness_data.json'
    
    with urllib.request.urlopen(req) as f:
        data = json.load(f)
        
        with open(file_name, 'w') as handler:
            json.dump(data, handler)

            
t3 = PythonOperator(
    task_id = 'get_readiness_data',
    python_callable = get_readiness_data,
    dag = dag,
    )


t1 >> t2 >> t3 