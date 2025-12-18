from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

S3_BUCKET = "gsingh-pyspark-poc"
S3_KEY = "airflow/hello.py"

with DAG(
    dag_id="s3_script_smoketest",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoketest", "s3", "kubernetes"],
) as dag:

    run_script_from_s3 = KubernetesPodOperator(
        task_id="run_script_from_s3",
        name="run-script-from-s3",
        namespace="airflow",
        in_cluster=True,
        image="public.ecr.aws/docker/library/python:3.11-slim",
        cmds=["bash", "-lc"],
        arguments=[
            f"""
            set -e
            echo "Installing awscli..."
            pip install -q awscli

            echo "Downloading script from S3..."
            aws s3 cp s3://{S3_BUCKET}/{S3_KEY} /tmp/hello.py

            echo "Running script..."
            python /tmp/hello.py
            """
        ],
        get_logs=True,
        is_delete_operator_pod=False,
    )
