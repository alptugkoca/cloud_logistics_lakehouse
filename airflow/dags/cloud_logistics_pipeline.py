from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
PYTHON_BIN = "python"

COMMON_ENV = (
    f"cd {PROJECT_DIR} && "
    f"unset JAVA_HOME && "
    f"export APP_ENV=aws && "
    f"export AWS_PROFILE=default && "
)

default_args = {
    "owner": "alptug",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="cloud_logistics_pipeline",
    default_args=default_args,
    description="Cloud Logistics Lakehouse Pipeline (Batch)",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["lakehouse", "batch", "pipeline"],
) as dag:

    # BRONZE
    bronze_batch = BashOperator(
        task_id="bronze_batch_ingest",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.bronze.batch_ingest",
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    bronze_events = BashOperator(
        task_id="bronze_events_ingest",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.bronze.events_ingest",
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # SILVER
    silver_batch = BashOperator(
        task_id="silver_batch_transform",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.silver.transform_batch",
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    silver_events = BashOperator(
        task_id="silver_events_transform",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.silver.transform_events",
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # QUALITY
    quality_check = BashOperator(
        task_id="quality_check_logistics_data",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.quality.check_logistics_data",
    )

    # ML PREDICTION
    predict_cost = BashOperator(
        task_id="gold_predict_cost",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.gold.predict_cost",
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # GOLD
    gold_logistics = BashOperator(
        task_id="gold_logistics_kpi",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.gold.logistics_kpi",
    )

    gold_route = BashOperator(
        task_id="gold_route_performance",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.gold.route_performance",
    )

    gold_anomaly = BashOperator(
        task_id="gold_cost_anomaly",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.gold.cost_anomaly",
    )

    gold_funnel = BashOperator(
        task_id="gold_funnel_kpi",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.gold.funnel_kpi",
    )

    gold_incidents = BashOperator(
        task_id="gold_incidents_kpi",
        bash_command=COMMON_ENV + f"{PYTHON_BIN} -m src.gold.incidents_kpi",
    )

    # DEPENDENCIES

    # Batch pipeline
    bronze_batch >> silver_batch
    silver_batch >> quality_check >> predict_cost

    # ML feeds KPIs
    predict_cost >> gold_logistics >> gold_route >> gold_anomaly
    
    # Events branch
    bronze_events >> silver_events
    silver_events >> gold_funnel
    silver_events >> gold_incidents