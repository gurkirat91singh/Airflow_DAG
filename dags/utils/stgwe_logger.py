from __future__ import annotations

from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "stgwe_postgres"
XCOM_KEY_LOG_ID = "stgwe_log_id"
START_TASK_ID = "__start_log"


# -------------------------
# helpers
# -------------------------

def _pg():
    return PostgresHook(postgres_conn_id=PG_CONN_ID)


def _utcnow():
    return datetime.utcnow()


def _get_job_id(dag_id: str) -> int:
    rec = _pg().get_first(
        """
        SELECT job_id
          FROM stgwe.stg_job
         WHERE job_name = %s
           AND is_active = TRUE
        """,
        parameters=(dag_id,),
    )
    if not rec:
        raise ValueError(f"stgwe.stg_job missing active job_name='{dag_id}'")
    return int(rec[0])


def _get_action_id(job_id: int, task_id: str) -> int:
    rec = _pg().get_first(
        """
        SELECT id
          FROM stgwe.stg_action
         WHERE job_id = %s
           AND action_name = %s
        """,
        parameters=(job_id, task_id),
    )
    if not rec:
        raise ValueError(
            f"stgwe.stg_action missing for job_id={job_id}, action_name='{task_id}'"
        )
    return int(rec[0])


def _is_internal_task(task_id: str) -> bool:
    return task_id.startswith("__")


# -------------------------
# job-level logging
# -------------------------

def create_job_log(**context) -> int:
    """
    Creates a row in stg_job_log and stores log_id in XCom.
    MUST be run as the first task in the DAG with task_id=__start_log
    """
    ti = context["ti"]
    dag_id = ti.dag_id
    job_id = _get_job_id(dag_id)

    log_id = _pg().get_first(
        """
        INSERT INTO stgwe.stg_job_log (job_id, start_time, status)
        VALUES (%s, %s, %s)
        RETURNING log_id
        """,
        parameters=(job_id, _utcnow(), "RUNNING"),
    )[0]

    ti.xcom_push(key=XCOM_KEY_LOG_ID, value=int(log_id))
    return int(log_id)


def close_job_log(status: str, **context):
    """
    Updates stg_job_log using log_id from __start_log XCom.
    Called from __end_ok / __end_fail tasks.
    """
    ti = context["ti"]
    log_id = ti.xcom_pull(task_ids=START_TASK_ID, key=XCOM_KEY_LOG_ID)

    if not log_id:
        raise ValueError("log_id not found in XCom from __start_log")

    _pg().run(
        """
        UPDATE stgwe.stg_job_log
           SET end_time = %s,
               status   = %s
         WHERE log_id   = %s
        """,
        parameters=(_utcnow(), status, int(log_id)),
    )


# -------------------------
# step-level callbacks
# -------------------------

def task_on_execute(context):
    ti = context["ti"]

    # Skip internal bookkeeping tasks
    if _is_internal_task(ti.task_id):
        return

    job_id = _get_job_id(ti.dag_id)
    action_id = _get_action_id(job_id, ti.task_id)

    log_id = ti.xcom_pull(task_ids=START_TASK_ID, key=XCOM_KEY_LOG_ID)
    if not log_id:
        raise ValueError("log_id not found in XCom from __start_log")

    _pg().run(
        """
        INSERT INTO stgwe.stg_job_step_log (log_id, action_id, start_time, status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (log_id, action_id) DO UPDATE
           SET start_time = EXCLUDED.start_time,
               status     = EXCLUDED.status
        """,
        parameters=(int(log_id), int(action_id), _utcnow(), "RUNNING"),
    )


def task_on_success(context):
    ti = context["ti"]

    if _is_internal_task(ti.task_id):
        return

    job_id = _get_job_id(ti.dag_id)
    action_id = _get_action_id(job_id, ti.task_id)
    log_id = ti.xcom_pull(task_ids=START_TASK_ID, key=XCOM_KEY_LOG_ID)

    if not log_id:
        raise ValueError("log_id not found in XCom from __start_log")

    _pg().run(
        """
        UPDATE stgwe.stg_job_step_log
           SET end_time = %s,
               status   = %s
         WHERE log_id   = %s
           AND action_id= %s
        """,
        parameters=(_utcnow(), "SUCCESS", int(log_id), int(action_id)),
    )


def task_on_failure(context):
    ti = context["ti"]

    if _is_internal_task(ti.task_id):
        return

    job_id = _get_job_id(ti.dag_id)
    action_id = _get_action_id(job_id, ti.task_id)
    log_id = ti.xcom_pull(task_ids=START_TASK_ID, key=XCOM_KEY_LOG_ID)

    if not log_id:
        raise ValueError("log_id not found in XCom from __start_log")

    _pg().run(
        """
        UPDATE stgwe.stg_job_step_log
           SET end_time = %s,
               status   = %s
         WHERE log_id   = %s
           AND action_id= %s
        """,
        parameters=(_utcnow(), "FAILED", int(log_id), int(action_id)),
    )
