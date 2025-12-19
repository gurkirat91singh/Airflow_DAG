from __future__ import annotations

from datetime import datetime
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

def _hello():
    print("hello world!")

def _bye():
    print("bye!")

with DAG(
    dag_id="Airflow_Hello_World",  # MUST match stg_job.job_name
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

    print_hello = PythonOperator(
        task_id="print_hello",  # MUST match stg_action.action_name
        python_callable=_hello,
    )

    print_bye = PythonOperator(
        task_id="print_bye",    # MUST match stg_action.action_name
        python_callable=_bye,
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

    start_log >> print_hello >> print_bye
    print_bye >> [end_ok, end_fail]
