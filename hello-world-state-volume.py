from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from ContextedHttpOperator import ExtendedHttpOperator

from functools import partial
import json

from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount


def get_job_status_update_callable(status, **context):
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
            "status": status
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
    schedule_interval=None
)


start_callback = ExtendedHttpOperator(
    http_conn_id="apar_graphql",
    endpoint="graphql/",
    method="POST",
    headers={"Content-Type": "application/json"},
    data_fn=partial(get_job_status_update_callable, "RUNNING"),
    task_id="start_callback",
    dag=dag
)

completed_callback = ExtendedHttpOperator(
    http_conn_id="apar_graphql",
    endpoint="graphql/",
    method="POST",
    headers={"Content-Type": "application/json"},
    data_fn=partial(get_job_status_update_callable, "COMPLETED"),
    task_id="completed_callback",
    dag=dag
)

failed_callback = ExtendedHttpOperator(
    http_conn_id="apar_graphql",
    endpoint="graphql/",
    method="POST",
    headers={"Content-Type": "application/json"},
    data_fn=partial(get_job_status_update_callable, "FAILED"),
    task_id="failed_callback",
    dag=dag,
    trigger_rule="all_failed",
)




passing = KubernetesPodOperator(
    namespace="airflow",
    image="python:3.6",
    cmds=["ls","-a"],
    # arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="passing-test",
    task_id="passing-task",
    get_logs=True,
    dag=dag,
    # volume_mounts=[],
    # volumes=[]
)


with dag:
    start_callback >> passing >> [completed_callback, failed_callback]
