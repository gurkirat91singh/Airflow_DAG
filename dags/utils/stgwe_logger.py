from __future__ import annotations

from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "stgwe_postgres"
XCOM_KEY_LOG_ID = "stgwe_log_id"


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
        raise ValueError(f"stgwe.stg_action missing for job_id={job_id}, action_name='{task_id}'")
    return int(rec[0])


def create_job_log(**context) -> int:
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
    ti = context["ti"]
    log_id = ti.xcom_pull(key=XCOM_KEY_LOG_ID)
    if not log_id:
        raise ValueError("log_id not found in XCom (start_log may not have run)")

    _pg().run(
        """
        UPDATE stgwe.stg_job_log
           SET end_time = %s,
               status   = %s
         WHERE log_id   = %s
        """,
        parameters=(_utcnow(), status, int(log_id)),
    )


def task_on_execute(context):
    ti = context["ti"]
    job_id = _get_job_id(ti.dag_id)
    action_id = _get_action_id(job_id, ti.task_id)

    log_id = ti.xcom_pull(key=XCOM_KEY_LOG_ID)
    if not log_id:
        raise ValueError("log_id not found in XCom (ensure __start_log is upstream)")

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
    job_id = _get_job_id(ti.dag_id)
    action_id = _get_action_id(job_id, ti.task_id)
    log_id = int(ti.xcom_pull(key=XCOM_KEY_LOG_ID))

    _pg().run(
        """
        UPDATE stgwe.stg_job_step_log
           SET end_time = %s,
               status   = %s
         WHERE log_id   = %s
           AND action_id= %s
        """,
        parameters=(_utcnow(), "SUCCESS", log_id, action_id),
    )


def task_on_failure(context):
    ti = context["ti"]
    job_id = _get_job_id(ti.dag_id)
    action_id = _get_action_id(job_id, ti.task_id)
    log_id = int(ti.xcom_pull(key=XCOM_KEY_LOG_ID))

    _pg().run(
        """
        UPDATE stgwe.stg_job_step_log
           SET end_time = %s,
               status   = %s
         WHERE log_id   = %s
           AND action_id= %s
        """,
        parameters=(_utcnow(), "FAILED", log_id, action_id),
    )
