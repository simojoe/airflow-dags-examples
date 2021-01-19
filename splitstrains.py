from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from datetime import datetime, timedelta
from functools import partial
import json

from ContextedHttpOperator import ExtendedHttpOperator


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
    "splitStrains",
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

volume_config = {
    "persistentVolumeClaim": {
        "claimName": "pvc-data-name"
    }
}

test_volume = Volume(name="pv-data-name", configs=volume_config)

test_volume_mount = VolumeMount(
    "pv-data-name", mount_path="/data", sub_path=None, read_only=False)


trimmomatic = KubernetesPodOperator(
    namespace="airflow",
    image="biocontainers/trimmomatic:v0.38dfsg-1-deb_cv1",
    arguments=[
        "trimmomatic", "PE",
        "-threads", "1",
        "data/fastq/SRR6982497_pass_1.fastq.gz",
        "data/fastq/SRR6982497_pass_2.fastq.gz",
        "data/fastq/SRR6982497_trimmed1.fastq.gz",
        "data/fastq/SRR6982497_trimmed2.fastq.gz",
        "SLIDINGWINDOW:4:15"
    ],
    name="trimmomatic",
    task_id="trimmomatic",
    get_logs=True,
    dag=dag,
    volumes=[test_volume],
    volume_mounts=[test_volume_mount],
)

with dag:
    start_callback >> trimmomatic >> [completed_callback, failed_callback]