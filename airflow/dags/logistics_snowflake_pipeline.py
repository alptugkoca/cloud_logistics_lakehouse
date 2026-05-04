from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

BASE = "/opt/airflow/project"

with DAG(
    dag_id="logistics_snowflake_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["snowflake", "logistics", "lakehouse"],
) as dag:

    load_cost_anomalies = BashOperator(
        task_id="load_cost_anomalies",
        bash_command=f"cd {BASE} && python scripts/run_sql_file.py snowflake/loads/load_cost_anomalies.sql",
    )

    load_logistics_kpi = BashOperator(
        task_id="load_logistics_kpi",
        bash_command=f"cd {BASE} && python scripts/run_sql_file.py snowflake/loads/load_logistics_kpi.sql",
    )

    load_cost_predictions_staging = BashOperator(
        task_id="load_cost_predictions_staging",
        bash_command=f"cd {BASE} && python scripts/run_sql_file.py snowflake/loads/load_cost_predictions_staging.sql",
    )

    merge_cost_predictions = BashOperator(
        task_id="merge_cost_predictions",
        bash_command=f"cd {BASE} && python scripts/run_sql_file.py snowflake/loads/merge_cost_predictions.sql",
    )

    load_route_performance = BashOperator(
        task_id="load_route_performance",
        bash_command=f"cd {BASE} && python scripts/run_sql_file.py snowflake/loads/load_route_performance.sql",
    )

    load_cost_model_metrics = BashOperator(
        task_id="load_cost_model_metrics",
        bash_command=f"cd {BASE} && python scripts/run_sql_file.py snowflake/loads/load_cost_model_metrics.sql",
    )

    rebuild_dq = BashOperator(
        task_id="rebuild_data_quality_checks",
        bash_command=f"cd {BASE} && python scripts/run_sql_file.py snowflake/quality/rebuild_data_quality_checks.sql",
    )

    create_dq_view = BashOperator(
        task_id="create_data_quality_monitor_view",
        bash_command=f"cd {BASE} && python scripts/run_sql_file.py snowflake/quality/create_data_quality_monitor_view.sql",
    )
    
    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f"cd {BASE}/cloud_logistics_dbt && dbt run",
    )
    
    dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"cd {BASE}/cloud_logistics_dbt && dbt test",
    )

    create_alerts = BashOperator(
        task_id="create_alerts_view",
        bash_command=f"cd {BASE} && python scripts/run_sql_file.py snowflake/views/create_alerts_view.sql",
    )

    [load_cost_anomalies, load_logistics_kpi, load_route_performance, load_cost_model_metrics] >> load_cost_predictions_staging
    load_cost_predictions_staging >> merge_cost_predictions >> rebuild_dq >> create_dq_view >> dbt_run >> dbt_test >> create_alerts