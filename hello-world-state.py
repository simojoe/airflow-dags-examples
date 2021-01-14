from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from ContextedHttpOperator import ExtendedHttpOperator

import json

def get_job_status_update_callable(**context):
    dag_run_conf = context["dag_run"].conf
    job_id = dag_run_conf.get("job_id")

    return json.dumps({
        "query": """
            mutation jobUpdateStatus($jobId: ID!, $status: String!) {
            jobUpdateStatus(input: { jobId: $jobId, status: $status }) {
                job {
                id
                status
                }
            }
            }
        """,
        "variables": {
            "jobId": job_id,
            "status": "RUNNING"
        }
    })


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow(),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG(
    "kubernetes_hello_world_state", 
    default_args=default_args, 
    schedule_interval=timedelta(minutes=10)
)


start_callback = ExtendedHttpOperator(
    http_conn_id="apar_graphql",
    endpoint="graphql/",
    method="POST",
    headers={"Content-Type": "application/json"},
    data_fn=get_job_status_update_callable,
    dag=dag
)


start = DummyOperator(task_id="start", dag=dag)

passing = KubernetesPodOperator(
    namespace="airflow",
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="passing-test",
    task_id="passing-task",
    get_logs=True,
    dag=dag
)


end = DummyOperator(task_id="end", dag=dag)

start.set_upstream(start_callback)
passing.set_upstream(start)
passing.set_downstream(end)
