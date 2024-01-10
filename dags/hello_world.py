from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    print("Hello World!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('hello_world',
          default_args=default_args,
          description='Hello World DAG',
          schedule_interval='0 12 * * *',  # Run once a day at noon
          catchup=False)

hello_world_task = PythonOperator(task_id='hello_world_task',
                                  python_callable=hello_world,
                                  dag=dag)
