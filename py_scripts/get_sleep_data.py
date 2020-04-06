from datetime import timedelta, datetime
import urllib.request
import json


today = datetime.today()
today = today.isoformat()
prior = datetime.today() - timedelta(days=1)
prior = prior.isoformat()

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