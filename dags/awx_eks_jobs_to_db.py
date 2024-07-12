# ---------------------------------------------------------------------------------------------------
# @author: Alexei Miranda <alexei.miranda@inter.co>
# @copyright: (c) Inter&Co - CyberSecurity - DevSecOps
# @name: dags/automation/healthchecking/data_collector/awx_eks_jobs_to_db.py
# @description: DAG to Get all AWX Jobs (templates eks) and send them to the dwsecurity/healthchecking
# @version: 1.0.0
# ---------------------------------------------------------------------------------------------------
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import datetime
import urllib3
from urllib.parse import urljoin

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Template AWX EKS
template_id = 12

def get_token(awx_security):
    hook = HttpHook(http_conn_id=awx_security)

    token_endpoint = "/api/v2/tokens/"
    auth = (hook.get_connection().login, hook.get_connection().password)

    response = hook.run(endpoint=token_endpoint, auth=auth, json={}, method="POST")
    response.raise_for_status()
    return response.json()["token"]

def get_job_status(awx_host, token, id):
    url = f"{awx_host}/api/v2/job_templates/{id}/jobs/"
    headers = {"Authorization": f"Bearer {token}"}
    results = []

    next_url = url
    while next_url:
        response = requests.get(next_url, headers=headers, verify=False)
        response.raise_for_status()
        data = response.json()
        results.extend(data.get('results', []))
        next_url = data.get('next')

        if next_url:
            next_url = urljoin(awx_host, next_url)

    return results

def save_job_status_to_db(job_status, dwsecurity_healthchecking):
    hook = PostgresHook(postgres_conn_id=dwsecurity_healthchecking)
    conn = hook.get_conn()
    cur = conn.cursor()

    count = 0

    print("\n")
    print(f"AWX Get Templates with Job_Id (save DB)")
    print("\n")

    print(f"template_id;job_id;job_created;job_template_name;failed")
    print(f"--------------------------------------------------------------------------------------------")

    for result in job_status:
        job_id = result.get('id')
        job_created = result.get('created')
        job_template = result.get('summary_fields', {}).get('job_template', {})
        job_template_name = job_template.get('name')
        failed = result.get('failed')

        dt = datetime.datetime.strptime(job_created, "%Y-%m-%dT%H:%M:%S.%fZ")
        formatted_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")

        query_check = 'SELECT EXISTS (SELECT 1 FROM "hc_jobs" WHERE job_id = %s::int)'
        cur.execute(query_check, (str(job_id),))
        exists = cur.fetchone()[0]

        if not exists:
            query_insert = 'INSERT INTO "hc_jobs" (template_id, job_id, job_created, job_template_name, failed) VALUES (%s, %s, %s, %s, %s)'
            values = (template_id, job_id, formatted_timestamp, job_template_name, failed)
            cur.execute(query_insert, values)
            count += 1

            print(f"{template_id};{job_id};{formatted_timestamp};{job_template_name};{failed}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"--------------------------------------------------------------------------------------------")

    return count

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG('healthchecking_data_collector_jobs_eks', default_args=default_args, schedule_interval='@daily') as dag:
    def fetch_and_save_job_status():
        awx_conn_id = "awx_security"
        postgres_conn_id = "dwsecurity_healthchecking"

        token = get_token(awx_conn_id)
        job_status = get_job_status(awx_host, token, 12)
        count = save_job_status_to_db(job_status, postgres_conn_id)
        print(f"Count: {count}")

    execute_task = PythonOperator(
        task_id='execute_task',
        python_callable=fetch_and_save_job_status,
    )

    execute_task
