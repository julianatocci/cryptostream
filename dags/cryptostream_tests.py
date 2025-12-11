from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ID = "wagon-bootcamp-466414"
REGION = "europe-west1"

default_args = {
    "owner": "cryptostream",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="cryptostream_tests",
    default_args=default_args,
    description="Roda os testes de dbt via Cloud Run Job uma vez por minuto",
    schedule_interval="* * * * *",   # todo minuto
    start_date=datetime(2025, 12, 10),
    catchup=False,
    max_active_runs=1,
    tags=["cryptostream", "dbt", "tests", "cloud-run"],
) as dag:

    run_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            "gcloud run jobs execute cryptostream-test-pipeline "
            f"--region={REGION} "
            f"--project={PROJECT_ID} "
            "--wait"
        ),
    )

    run_tests
