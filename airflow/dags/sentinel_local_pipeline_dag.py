from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


REPO_ROOT = Path("/opt/airflow/Sentinel-Robot")
WAREHOUSE_DIR = REPO_ROOT / "warehouse"


default_args = {
    "owner": "sentinel",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="sentinel_local_pipeline",
    description="Local orchestration proof for Sentinel-Robot Snowflake/dbt pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sentinel", "snowflake", "dbt", "local"],
) as dag:

    generate_full_data = BashOperator(
        task_id="generate_full_data",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "python -m generator.cli --out data/raw"
        ),
    )

    load_raw_to_snowflake = BashOperator(
        task_id="load_raw_to_snowflake",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "python scripts/load_raw_to_snowflake.py"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {WAREHOUSE_DIR} && "
            "dbt build"
        ),
    )

    generate_full_data >> load_raw_to_snowflake >> dbt_build