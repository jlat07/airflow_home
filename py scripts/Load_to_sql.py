from sqlalchemy import create_engine
import pandas as pd
import json


def update_db():

    '''
    Sleep data
    '''
    
    engine = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/Oura_Data')
    df = pd.read_json('Users/jthompson/dev/airflow_home/data/sleep_data.json', delimiter=',')
    df.to_sql(name='oura_sleep', con=engine, schema='Oura_Data', if_exists='replace')

    '''
    Activity Data
    '''
  
    engine = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/Oura_Data')
    df = pd.read_json('Users/jthompson/dev/airflow_home/data/activity_data.json', delimiter=',')
    df.to_sql(name='oura_readiness', con=engine, schema='Oura_Data', if_exists='replace')
    
    '''
    Readiness Data
    '''
    
    engine = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/Oura_Data')
    df = pd.read_json('Users/jthompson/dev/airflow_home/data/readiness_data_response.json', delimiter=',')
    df.to_sql(name='oura_readiness', con=engine, schema='Oura_Data', if_exists='replace')
    

if __name__ == "__main__":   
    update_db()