-- models/marts/ml_robot_training_dataset.sql

WITH daily_health AS (
    SELECT * FROM {{ ref('fct_robot_health_daily') }}
),

failures AS (
    SELECT * FROM {{ ref('simulated_failures') }}
)

SELECT
    dh.robotid,
    dh.summary_date,
    dh.avg_motor_temp,
    dh.max_motor_temp,
    dh.avg_battery_voltage,
    dh.min_battery_voltage,
    dh.max_vibration,

    -- This is our TARGET VARIABLE (the "label")
    -- It checks if a failure occurred for this robot within the next 7 days
    CASE
        WHEN f.failure_date IS NOT NULL THEN 1
        ELSE 0
    END AS will_fail_in_next_7_days

FROM
    daily_health dh
LEFT JOIN
    failures f ON dh.robotid = f.robot_id
    AND dh.summary_date >= DATE_ADD('day', -7, f.failure_date) -- The 7-day prediction window
    AND dh.summary_date < f.failure_date