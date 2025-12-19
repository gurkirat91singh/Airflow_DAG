from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def test():
    h = PostgresHook(postgres_conn_id="stgwe_postgres")
    print(h.get_first("select 1;"))

with DAG(
    dag_id="test_rds_conn",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="ping_rds", python_callable=test)
