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


fastq_dump = KubernetesPodOperator(
    namespace="airflow",
    image="quay.io/biocontainers/sra-tools:2.10.0--pl526he1b5a44_0",
    cmds=[
        "fastq-dump",
        "--outdir", "data/fastq",
        "--gzip", "--skip-technical",  "--readids",
        "--read-filter", "pass", "--dumpbase",
        "--split-3", "--clip",
        "SRR6982497"
    ],
    name="fastq_dump",
    task_id="fastq_dump",
    get_logs=True,
    dag=dag,
    volumes=[test_volume],
    volume_mounts=[test_volume_mount],
)


trimmomatic = KubernetesPodOperator(
    namespace="airflow",
    image="quay.io/biocontainers/trimmomatic:0.39--0",
    cmds=[
        "trimmomatic", "PE",
        "-threads", "1",
        "data/fastq/SRR6982497_pass_1.fastq.gz",
        "data/fastq/SRR6982497_pass_2.fastq.gz",
        "data/fastq/SRR6982497_trimmed1_paired.fastq.gz",
        "data/fastq/SRR6982497_trimmed1_unpaired.fastq.gz",
        "data/fastq/SRR6982497_trimmed2_paired.fastq.gz",
        "data/fastq/SRR6982497_trimmed2_unpaired.fastq.gz",
        "SLIDINGWINDOW:4:15"
    ],
    name="trimmomatic",
    task_id="trimmomatic",
    get_logs=True,
    dag=dag,
    volumes=[test_volume],
    volume_mounts=[test_volume_mount],
)

bwa_R1 = KubernetesPodOperator(
    namespace="airflow",
    image="quay.io/biocontainers/bwa:0.7.17--hed695b0_7",
    cmds=[
        "bwa", "aln",
        "-q", "15",
        "-t", "6",
        "-R", "'@RG\tID:mixed\tSM:mixed\tLB:None\tPL:Illumina'"
        "data/ref/tuberculosis.fna",
        "data/fastq/SRR6982497_trimmed1_paired.fastq.gz",
        ">", "data/fastq/SRR6982497_aligned1.sai",
    ],    
    name="bwa_aln_R1",
    task_id="bwa_aln_R1",
    get_logs=True,
    dag=dag,
    volumes=[test_volume],
    volume_mounts=[test_volume_mount],
)

bwa_R2 = KubernetesPodOperator(
    namespace="airflow",
    image="quay.io/biocontainers/bwa:0.7.17--hed695b0_7",
    cmds=[
        "bwa", "aln",
        "-q", "15",
        "-t", "6",
        "-R", "'@RG\tID:mixed\tSM:mixed\tLB:None\tPL:Illumina'"
        "data/ref/tuberculosis.fna",
        "data/fastq/SRR6982497_trimmed2_paired.fastq.gz",
        ">", "data/fastq/SRR6982497_aligned2.sai",
    ],    
    name="bwa_aln_R2",
    task_id="bwa_aln_R2",
    get_logs=True,
    dag=dag,
    volumes=[test_volume],
    volume_mounts=[test_volume_mount],
)

bwa_sampe = KubernetesPodOperator(
    namespace="airflow",
    image="quay.io/biocontainers/bwa:0.7.17--hed695b0_7",
    cmds=[
        "bwa", "sampe",
        "data/ref/tuberculosis.fna",
        "data/fastq/SRR6982497_aligned1.sai",
        "data/fastq/SRR6982497_aligned2.sai",
        "data/fastq/SRR6982497_trimmed1_paired.fastq.gz",
        "data/fastq/SRR6982497_trimmed2_paired.fastq.gz",
    ],    
    name="bwa_sampe",
    task_id="bwa_sampe",
    get_logs=True,
    dag=dag,
    volumes=[test_volume],
    volume_mounts=[test_volume_mount],
)

# with dag:
#     start_callback >> fastq_dump >> trimmomatic >> [
#         completed_callback, failed_callback]
with dag:
    start_callback >> fastq_dump >> trimmomatic >> [bwa_R2, bwa_R1] >> bwa_sampe >> [completed_callback, failed_callback]
