from __future__ import annotations

import time
from datetime import datetime

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from utils.stgwe_logger import (
    create_job_log,
    close_job_log,
    task_on_execute,
    task_on_success,
    task_on_failure,
)


REGION = "us-east-2"
EMR_APP_ID = "00g2peqpfb58ar0d"
EXECUTION_ROLE_ARN = "arn:aws:iam::356070494385:role/emr-serverless-execution-role"
S3_BUCKET = "stgwe-spark"

SPARK_PARAMS = (
    "--conf spark.dynamicAllocation.enabled=false "
    "--conf spark.executor.instances=2 "
    "--conf spark.executor.cores=2 "
    "--conf spark.executor.memory=6g "
    "--conf spark.executor.memoryOverhead=2g "
    "--conf spark.driver.memory=4g "
    "--conf spark.driver.memoryOverhead=2g "
    "--jars s3://stgwe-spark/jar/postgresql-42.7.3.jar"
)

START_TASK_ID = "__start_log"
XCOM_KEY_LOG_ID = "stgwe_log_id"


def _emr():
    return boto3.client("emr-serverless", region_name=REGION)


def submit_and_wait(**context):
    """
    Submits EMR job for the current task_id (script name), waits for terminal state.
    Logs for Airflow step/job are handled by stgwe_logger callbacks.
    """
    ti = context["ti"]
    action_name = ti.task_id  # e.g. two_step_step1
    log_id = ti.xcom_pull(task_ids=START_TASK_ID, key=XCOM_KEY_LOG_ID)

    entry_point = f"s3://{S3_BUCKET}/code/jobs/{action_name}.py"

    spark_params = SPARK_PARAMS
    if log_id is not None:
        spark_params += f" --conf spark.job.log_id={int(log_id)}"

    resp = _emr().start_job_run(
        applicationId=EMR_APP_ID,
        executionRoleArn=EXECUTION_ROLE_ARN,
        name=f"two_step__{action_name}",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_point,
                "sparkSubmitParameters": spark_params,
            }
        },
        tags={
            "pipeline": "two_step",
            "step_name": action_name,
            "log_id": str(log_id) if log_id is not None else "",
        },
    )

    job_run_id = resp["jobRunId"]
    print(f"[EMR] started action={action_name} jobRunId={job_run_id}")

    # wait
    while True:
        jr = _emr().get_job_run(applicationId=EMR_APP_ID, jobRunId=job_run_id)["jobRun"]
        state = jr["state"]
        print(f"[EMR] action={action_name} jobRunId={job_run_id} state={state}")
        if state in ("SUCCESS", "FAILED", "CANCELLED"):
            if state != "SUCCESS":
                raise RuntimeError(f"EMR step failed: action={action_name}, state={state}, jobRunId={job_run_id}")
            return
        time.sleep(15)


with DAG(
    dag_id="two_step",  # MUST match stgwe.stg_job.job_name
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    default_args={
        "on_execute_callback": task_on_execute,
        "on_success_callback": task_on_success,
        "on_failure_callback": task_on_failure,
    },
) as dag:

    start_log = PythonOperator(
        task_id="__start_log",
        python_callable=create_job_log,
    )

    # task_ids MUST match stg_action.action_name
    two_step_step1 = PythonOperator(
        task_id="two_step_step1",
        python_callable=submit_and_wait,
    )

    two_step_step2 = PythonOperator(
        task_id="two_step_step2",
        python_callable=submit_and_wait,
    )

    end_ok = PythonOperator(
        task_id="__end_ok",
        python_callable=lambda **ctx: close_job_log("SUCCESS", **ctx),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end_fail = PythonOperator(
        task_id="__end_fail",
        python_callable=lambda **ctx: close_job_log("FAILED", **ctx),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    start_log >> two_step_step1 >> two_step_step2
    two_step_step2 >> [end_ok, end_fail]
