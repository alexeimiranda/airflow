from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import json
import mysql.connector

# Função para conectar ao MySQL e extrair dados
def extract_data_from_mysql():
    # Configurações da conexão
    config = {
        'user': 'admin',
        'password': 'password',
        'host': '10.211.55.3',
        'database': 'agro',
        'raise_on_warnings': True
    }

    # Conectar ao banco de dados
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # Executar consulta SQL
    query = "SELECT * FROM clientes;"
    cursor.execute(query)

    # Converter os dados para um DataFrame do Pandas
    df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

    # Fechar conexão
    cursor.close()
    conn.close()

    # Gerar arquivo JSON
    df.to_json('agro_clientes.json', orient='records')

# Definindo argumentos padrão para a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definindo a DAG
dag = DAG(
    'mysql_to_json1',
    default_args=default_args,
    description='Uma DAG simples para extrair dados do MySQL e salvar em JSON',
    schedule_interval=timedelta(days=1),
)

# Definindo a tarefa
extract_task = PythonOperator(
    task_id='extract_from_mysql1',
    python_callable=extract_data_from_mysql,
    dag=dag,
)

# Configurando a ordem das tarefas na DAG
extract_task
