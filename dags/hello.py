# DAG.py

from airflow import DAG
from airflow.operators.python import PythonOperator

# Definindo a DAG
dag = DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 9, 23, 51, 0),
    schedule_interval="@daily",
)

# Definindo a tarefa
task = PythonOperator(
    task_id="hello_world",
    python_callable=lambda: print("Hello World!"),
)

# Adicionando a tarefa à DAG
dag.add_task(task)
