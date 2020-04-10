from datetime import timedelta, datetime
from sqlalchemy import create_engine
import urllib.request
import pandas as pd
import json


today = datetime.today()
today = today.isoformat()
prior = datetime.today() - timedelta(days=1)
prior = prior.isoformat()

def get_activity_data():

    url = 'https://api.ouraring.com/v1/activity' + f'?start={prior}'
    headers = {
        'Authorization': 'Bearer M7IEAGX653H6UGYXDIDPVOCXAATK6AAD',
        'Cookie': '_ga=GA1.2.1358351741.1586059502; _gid=GA1.2.886189931.1586288695'
    }
    req = urllib.request.Request(url, headers=headers)

    file_name = '/Users/jthompson/dev/airflow_home/data/activity_data.json'
    
    with urllib.request.urlopen(req) as f:
        data = json.load(f)
        
        with open(file_name, 'w') as handler:
            json.dump(data, handler)
            # While file is ope. Store Data into DB
            engine = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/Oura_Data')
            df = pd.read_json(f'{file_name}', delimiter=',')
            df.to_sql(name='oura_activity', con=engine, schema='Oura_Data', if_exists='replace')




if __name__ == "__main__":
    get_activity_data()