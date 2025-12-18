from __future__ import annotations

import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

AWS_REGION = "us-east-1"
EMR_APP_ID = "00g1epr3lmt18s09"
EMR_EXEC_ROLE_ARN = "arn:aws:iam::451393504235:role/emr-serverless-execution-role-poc"
ENTRYPOINT_S3 = "s3://gsingh-pyspark-poc/emr/jobs/hello_spark.py"


def _client():
    import boto3
    return boto3.client("emr-serverless", region_name=AWS_REGION)


def start_app():
    c = _client()
    try:
        c.start_application(applicationId=EMR_APP_ID)
    except c.exceptions.ConflictException:
        # already starting/started
        pass

    # wait until STARTED
    for _ in range(60):
        state = c.get_application(applicationId=EMR_APP_ID)["application"]["state"]
        print(f"state={state}")
        if state == "STARTED":
            return
        time.sleep(10)

    raise RuntimeError("Timed out waiting for EMR app to reach STARTED")


def start_job(ti):
    c = _client()
    resp = c.start_job_run(
        applicationId=EMR_APP_ID,
        executionRoleArn=EMR_EXEC_ROLE_ARN,
        jobDriver={"sparkSubmit": {"entryPoint": ENTRYPOINT_S3}},
        # no monitoringConfiguration (S3 logs removed as requested)
    )
    job_run_id = resp["jobRunId"]
    print(f"JOB_RUN_ID={job_run_id}")
    ti.xcom_push(key="job_run_id", value=job_run_id)


def wait_job(ti):
    c = _client()
    job_run_id = ti.xcom_pull(task_ids="start_job", key="job_run_id")
    if not job_run_id:
        raise ValueError("Missing job_run_id from XCom")

    while True:
        jr = c.get_job_run(applicationId=EMR_APP_ID, jobRunId=job_run_id)["jobRun"]
        state = jr["state"]
        print(f"STATE={state}")

        if state == "SUCCESS":
            print(f"✅ SUCCESS: {job_run_id}")
            return

        if state in ("FAILED", "CANCELLED"):
            print(f"❌ {state}: {job_run_id}")
            print(jr)
            raise RuntimeError(f"EMR Serverless job ended in {state}")

        time.sleep(15)


def stop_app():
    c = _client()
    try:
        c.stop_application(applicationId=EMR_APP_ID)
    except c.exceptions.ConflictException:
        # already stopping/stopped
        pass
    print("Stop requested.")


with DAG(
    dag_id="emr_serverless_trigger",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["emr-serverless"],
) as dag:

    t1 = PythonOperator(task_id="start_app", python_callable=start_app)
    t2 = PythonOperator(task_id="start_job", python_callable=start_job)
    t3 = PythonOperator(task_id="wait_job", python_callable=wait_job)

    # stop even if job fails
    t4 = PythonOperator(
        task_id="stop_app",
        python_callable=stop_app,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t1 >> t2 >> t3 >> t4
