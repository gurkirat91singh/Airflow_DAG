from datetime import datetime
from airflow import DAG

EMR_APP_ID = "00g1epr3lmt18s09"
EMR_EXEC_ROLE_ARN = "arn:aws:iam::451393504235:role/emr-serverless-execution-role-poc"

# Spark entrypoint script in S3 (upload this file)
ENTRYPOINT_S3 = "s3://gsingh-pyspark-poc/emr/jobs/hello_spark.py"

# Where EMR Serverless should write logs
LOG_URI = "s3://gsingh-pyspark-poc/emr-serverless-logs/"

def kpo(task_id: str, script: str):
    # lazy import to avoid DAG import timeouts
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace="airflow",
        in_cluster=True,
        service_account_name="airflow-worker",
        image="public.ecr.aws/aws-cli/aws-cli:2.15.30",
        cmds=["sh", "-lc"],
        arguments=[script],
        get_logs=True,
        is_delete_operator_pod=True,
    )

with DAG(
    dag_id="emr_serverless_trigger",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["emr-serverless"],
) as dag:

    start_app = kpo(
        "start_emr_app",
        f"""
set -euo pipefail
echo "Starting EMR Serverless application {EMR_APP_ID} (if needed)..."
aws emr-serverless start-application --application-id "{EMR_APP_ID}" || true

echo "Waiting for STARTED..."
for i in $(seq 1 60); do
  s=$(aws emr-serverless get-application --application-id "{EMR_APP_ID}" --query "application.state" --output text)
  echo "state=$s"
  [ "$s" = "STARTED" ] && exit 0
  sleep 10
done

echo "Timed out waiting for STARTED"
exit 1
""",
    )

    run_job = kpo(
        "run_spark_job",
        f"""
set -euo pipefail

echo "Submitting job..."
JOB_RUN_ID=$(aws emr-serverless start-job-run \
  --application-id "{EMR_APP_ID}" \
  --execution-role-arn "{EMR_EXEC_ROLE_ARN}" \
  --job-driver '{{"sparkSubmit": {{"entryPoint": "{ENTRYPOINT_S3}"}}}}' \
  --configuration-overrides '{{"monitoringConfiguration": {{"s3MonitoringConfiguration": {{"logUri": "{LOG_URI}"}}}}}}' \
  --query "jobRunId" --output text)

echo "JOB_RUN_ID=$JOB_RUN_ID"

echo "Polling until terminal state..."
while true; do
  STATE=$(aws emr-serverless get-job-run \
    --application-id "{EMR_APP_ID}" \
    --job-run-id "$JOB_RUN_ID" \
    --query "jobRun.state" --output text)
  echo "STATE=$STATE"

  if [ "$STATE" = "SUCCESS" ]; then
    echo "✅ SUCCESS: $JOB_RUN_ID"
    exit 0
  fi

  if [ "$STATE" = "FAILED" ] || [ "$STATE" = "CANCELLED" ]; then
    echo "❌ FAILED/CANCELLED: $JOB_RUN_ID"
    aws emr-serverless get-job-run --application-id "{EMR_APP_ID}" --job-run-id "$JOB_RUN_ID" --output json || true
    exit 1
  fi

  sleep 15
done
""",
    )

    stop_app = kpo(
        "stop_emr_app",
        f"""
set -euo pipefail
echo "Stopping EMR Serverless application {EMR_APP_ID}..."
aws emr-serverless stop-application --application-id "{EMR_APP_ID}" || true
""",
    )

    start_app >> run_job >> stop_app
