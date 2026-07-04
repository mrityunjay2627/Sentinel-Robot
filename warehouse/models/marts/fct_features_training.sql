with daily as (

    select *
    from {{ ref('fct_robot_daily') }}

),

features as (

    select
        robot_id,
        report_date as as_of_date,
        report_date_key as as_of_date_key,

        avg(avg_motor_temp_c) over (
            partition by robot_id
            order by report_date
            rows between 6 preceding and current row
        ) as avg_motor_temp_7d,

        max(max_motor_temp_c) over (
            partition by robot_id
            order by report_date
            rows between 6 preceding and current row
        ) as max_motor_temp_7d,

        avg(avg_vibration_g) over (
            partition by robot_id
            order by report_date
            rows between 6 preceding and current row
        ) as avg_vibration_7d,

        max(max_vibration_g) over (
            partition by robot_id
            order by report_date
            rows between 6 preceding and current row
        ) as max_vibration_7d,

        avg(avg_battery_soc_pct) over (
            partition by robot_id
            order by report_date
            rows between 6 preceding and current row
        ) as avg_battery_soc_7d,

        sum(error_reading_count) over (
            partition by robot_id
            order by report_date
            rows between 6 preceding and current row
        ) as error_readings_7d,

        sum(suspicious_motor_temp_count) over (
            partition by robot_id
            order by report_date
            rows between 6 preceding and current row
        ) as suspicious_motor_temp_count_7d,

        reading_count,
        current_timestamp() as dbt_loaded_at

    from daily

)

select *
from features