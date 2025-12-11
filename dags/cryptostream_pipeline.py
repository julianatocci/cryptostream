from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Se quiser, pode trocar pra Airflow Variables depois:
PROJECT_ID = "wagon-bootcamp-466414"
REGION = "europe-west1"

default_args = {
    "owner": "cryptostream",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def create_job_loop_dag(dag_id: str, job_name: str, description: str) -> DAG:
    """
    Cria uma DAG que roda APENAS um job do Cloud Run em loop infinito e independente.
    Fluxo:
        start -> run_job -> cooldown -> trigger_next_iteration (na mesma DAG)
    """
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=None,              # sem cron; só se auto-dispara
        start_date=datetime(2025, 12, 10),
        catchup=False,
        max_active_runs=1,                  # garante um loop por vez para CADA DAG
        tags=["cryptostream", "dbt", "cloud-run"],
    ) as dag:

        start = EmptyOperator(task_id="start")

        run_job = BashOperator(
            task_id="run_cloud_run_job",
            bash_command=(
                f"gcloud run jobs execute {job_name} "
                f"--region={REGION} "
                f"--project={PROJECT_ID} "
                "--wait"
            ),
        )

        cooldown = BashOperator(
            task_id="cooldown_2s",
            bash_command="sleep 2",
        )

        trigger_next = TriggerDagRunOperator(
            task_id="trigger_next_iteration",
            trigger_dag_id=dag_id,          # dispara ESTA MESMA DAG
            wait_for_completion=False,
            reset_dag_run=False,
        )

        start >> run_job >> cooldown >> trigger_next

        return dag


# DAG 1: trades
trades_dag = create_job_loop_dag(
    dag_id="cryptostream_trades_pipeline",
    job_name="cryptostream-trades-pipeline",
    description="Loop contínuo da pipeline de trades via Cloud Run Jobs",
)

# DAG 2: candles
candles_dag = create_job_loop_dag(
    dag_id="cryptostream_candles_pipeline",
    job_name="cryptostream-candles-pipeline",
    description="Loop contínuo da pipeline de candles via Cloud Run Jobs",
)

# DAG 3: tickers
tickers_dag = create_job_loop_dag(
    dag_id="cryptostream_tickers_pipeline",
    job_name="cryptostream-tickers-pipeline",
    description="Loop contínuo da pipeline de tickers via Cloud Run Jobs",
)

# DAG 4: orderbook
orderbook_dag = create_job_loop_dag(
    dag_id="cryptostream_orderbook_pipeline",
    job_name="cryptostream-orderbook-pi",
    description="Loop contínuo da pipeline de orderbook via Cloud Run Jobs",
)
