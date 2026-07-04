# Sentinel-Robot Reliability Checks

This project includes lightweight reliability checks across the synthetic data generation, raw loading, and dbt warehouse layers.

## 1. Generator Quality Gate

The generator validates clean internal data before writing messy external outputs.

Latest full run:

| Check | Result |
|---|---:|
| Generator quality checks | 37 passed / 0 failed |
| Robots | 50 |
| Days | 90 |
| Sensor readings | 432,000 |
| Tasks | 24,927 |
| Maintenance tickets | 304 |

## 2. Snowflake RAW Load Reconciliation

The raw loader copies generated CSV files into the Snowflake `RAW` schema.

Latest full raw load:

| Raw table | Rows |
|---|---:|
| RAW_ROBOT_MODELS | 6 |
| RAW_ZONES | 10 |
| RAW_ROBOTS | 50 |
| RAW_ROBOT_ATTRIBUTE_CHANGES | 104 |
| RAW_SENSOR_READINGS | 432,000 |
| RAW_TASKS | 24,927 |
| RAW_MAINTENANCE_TICKETS | 304 |

## 3. dbt Build Reliability

The full dbt warehouse build completed successfully.

Latest result:

```text
PASS=86 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=86