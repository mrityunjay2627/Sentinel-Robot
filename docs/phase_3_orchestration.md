# Phase 3: Lightweight Orchestration

Phase 3 adds a small Airflow DAG artifact for the Sentinel-Robot pipeline.

## Pipeline

The local orchestration flow is:

```text
generate_full_data
    ↓
load_raw_to_snowflake
    ↓
dbt_build