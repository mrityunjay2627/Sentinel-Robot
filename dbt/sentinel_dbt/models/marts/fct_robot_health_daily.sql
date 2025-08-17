-- models/marts/fct_robot_health_daily.sql

SELECT
    robotid,
    CAST(event_timestamp AS DATE) AS summary_date,

    COUNT(1) AS event_count,
    AVG(motortemp) AS avg_motor_temp,
    MAX(motortemp) AS max_motor_temp,
    AVG(batteryvoltage) AS avg_battery_voltage,
    MIN(batteryvoltage) AS min_battery_voltage,
    MAX(vibration) AS max_vibration

FROM
    {{ ref('stg_telemetry') }}

GROUP BY
    1, 2