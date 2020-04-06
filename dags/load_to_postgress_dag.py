from airflow.operators.postgres_operator import PostgresOperatorimport uuid
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
from datetime import datetime


dag_params = {
    'dag_id': 'PostgresOperator_dag',
    'start_date': datetime.now(),
    'schedule_interval': None
}


with DAG(**dag_params) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='''CREATE TABLE new_table(
            custom_id integer NOT NULL, timestamp TIMESTAMP NOT NULL, user_id VARCHAR (50) NOT NULL
            );''',
    )

    insert_row = PostgresOperator(
        task_id='insert_row',
        sql='INSERT INTO new_table VALUES(%s, %s, %s)',
        trigger_rule=TriggerRule.ALL_DONE,
        parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10])
    )

    create_table >> insert_row

default_args = {
    'owner': 'JLAT',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['jlat0128@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG(
    dag_id='get_all_data_daily',
    default_args=default_args,
    description='create and insert',
    schedule_interval=timedelta(days=1),
)


def connect_pst():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")

def create_sleep_table():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE oura_sleep (
    
    /*Sleep events are uniquely identified by period ID per summary date*/
    
    CONSTRAINT sleep_id     PRIMARY KEY (summary_date, period_id),

    awake               INTEGER,
    bedtime_end         TIMESTAMPTZ,
    bedtime_end_delta       INTEGER,
    bedtime_start           TIMESTAMPTZ,
    bedtime_start_delta     INTEGER,
    breath_average          FLOAT,
    deep                INTEGER,
    duration            INTEGER,
    efficiency          SMALLINT,
    hr_5min             SMALLINT[],
    hr_average          FLOAT,
    hr_lowest           SMALLINT,
    hypnogram_5min          CHAR[],
    is_longest          BOOLEAN,
    light               INTEGER,
    midpoint_at_delta       INTEGER,
    midpoint_time           INTEGER,
    onset_latency           INTEGER,
    period_id           SMALLINT,
    rem             INTEGER,
    restless            SMALLINT,
    rmssd               SMALLINT,
    rmssd_5min          SMALLINT[],
    score               SMALLINT,
    score_alignment         SMALLINT,
    score_deep          SMALLINT,
    score_disturbances      SMALLINT,
    score_efficiency        SMALLINT,
    score_latency           SMALLINT,
    score_rem           SMALLINT,
    score_total         SMALLINT,
    summary_date            DATE,
    temperature_delta       FLOAT,
    temperature_deviation       FLOAT,
    temperature_trend_deviation FLOAT,
    timezone            SMALLINT,
    total               INTEGER
);
    )
    """)

    conn.commit()

def create_activity_table():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""

    CREATE TABLE oura_activity (
    summary_date DATE PRIMARY KEY,
    
    cal_active          SMALLINT,
    cal_total           SMALLINT,
    daily_movement          SMALLINT,
    day_end             TIMESTAMPTZ,
    day_start           TIMESTAMPTZ,
    high                SMALLINT,
    inactive            SMALLINT,
    inactivity_alerts       SMALLINT,
    low             SMALLINT,
    medium              SMALLINT,
    met_min_inactive        SMALLINT,
    non_wear            SMALLINT,
    rest                SMALLINT,
    score               SMALLINT,
    steps               INTEGER,
    timezone            SMALLINT,
    average_met         FLOAT,
    class_5min          SMALLINT[],
    met_1min            FLOAT[],
    met_min_high            SMALLINT,
    met_min_low         SMALLINT,
    met_min_medium          SMALLINT,
    met_min_medium_plus     SMALLINT,
    score_meet_daily_targets    SMALLINT,
    score_move_every_hour       SMALLINT,
    score_recovery_time     SMALLINT,
    score_stay_active       SMALLINT,
    score_training_frequency    SMALLINT,
    score_training_volume       SMALLINT,
    target_calories         SMALLINT,
    target_km           SMALLINT,
    target_miles            SMALLINT,
    to_target_km            FLOAT,
    to_target_miles         FLOAT,
    total               SMALLINT
);
    )
    """)

    conn.commit()


def create_readiness_table():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""

    CREATE TABLE oura_readiness (
    
    /*Readiness has only one row per sleep event*/
    
    CONSTRAINT readiness_id PRIMARY KEY (summary_date, period_id),

    period_id       SMALLINT,
    score_activity_balance  SMALLINT,
    score_previous_day  SMALLINT,
    score_previous_night    SMALLINT,
    score_recovery_index    SMALLINT,
    score_resting_hr    SMALLINT,
    score_sleep_balance SMALLINT,
    score_temperature   SMALLINT,
    summary_date        DATE,
    score           SMALLINT
);
    )
    """)

    conn.commit()


def insert_csv():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    with open("/Users/jthompson/dev/airflow_home/Oura_____.csv", 'r') as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, 'sec', sep=',')
        conn.commit()

t1 = PythonOperator(
    task_id='connect_pst',
    provide_context=False,
    python_callable=connect_pst,
    dag=dag,
)

t3 = PythonOperator(
    task_id='create_sleep_table',
    provide_context=False,
    python_callable=create_sleep_table,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_activity_table',
    provide_context=False,
    python_callable=create_activity_table,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_readiness_table',
    provide_context=False,
    python_callable=create_readiness_table(),
    dag=dag,
)
t3 = PythonOperator(
    task_id='insert_csv',
    provide_context=False,
    python_callable=insert_csv,
    dag=dag,
)


t1

t2 >> t3
