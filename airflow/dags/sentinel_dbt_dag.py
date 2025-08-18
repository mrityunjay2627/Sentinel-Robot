from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define the paths to your dbt project and virtual environment
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/sentinel_dbt"
VENV_PYTHON_PATH = "/usr/local/airflow/dbt_venv/bin/python"

with DAG(
    dag_id="sentinel_daily_dbt_run",
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    schedule_interval="0 2 * * *",  # Run daily at 2 AM UTC
    catchup=False,
    tags=["sentinel", "dbt"],
) as dag:
    
    # Task to run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        # Command to navigate to the dbt project and run the models
        bash_command=f"cd {DBT_PROJECT_DIR} && {VENV_PYTHON_PATH} -m dbt run",
    )

    # Task to run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        # Command to navigate to the dbt project and run the tests
        bash_command=f"cd {DBT_PROJECT_DIR} && {VENV_PYTHON_PATH} -m dbt test",
    )

    # Define the task dependencies
    dbt_run >> dbt_test