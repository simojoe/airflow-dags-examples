from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.http import SimpleHttpOperator
from airflow.operators.dummy_operator import DummyOperator

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



variables = {
    "jobId": "12121212",
    "status": "RUNNING"
}

mutation = """
    mutation jobUpdateStatus($jobId: ID!, $status: String!) {
    jobUpdateStatus(input: { jobId: $jobId, status: $status }) {
        job {
        id
        status
        }
    }
    }
"""


start_callback = SimpleHttpOperator(
    endpoint="http://127.0.0.1:8000/graphql/",
    method="POST",
    data={
        "query": mutation,
        "variables": variables
    }
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
