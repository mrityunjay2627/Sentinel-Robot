with sensor_fact as (

    select *
    from {{ ref('fct_sensor_reading') }}
    where reading_ts is not null

),

sensor_daily as (

    select
        robot_id,
        to_date(reading_ts) as report_date,
        to_number(to_char(to_date(reading_ts), 'YYYYMMDD')) as report_date_key,

        count(*) as reading_count,

        avg(battery_voltage_v) as avg_battery_voltage_v,
        avg(battery_soc_pct) as avg_battery_soc_pct,
        min(battery_soc_pct) as min_battery_soc_pct,

        avg(battery_temp_c) as avg_battery_temp_c,
        max(battery_temp_c) as max_battery_temp_c,

        avg(motor_temp_c) as avg_motor_temp_c,
        max(motor_temp_c) as max_motor_temp_c,

        avg(motor_rpm) as avg_motor_rpm,
        avg(motor_load_pct) as avg_motor_load_pct,

        avg(vibration_g) as avg_vibration_g,
        max(vibration_g) as max_vibration_g,

        max(odometer_m) as max_odometer_m,

        sum(iff(operational_status = 'ACTIVE', 1, 0)) as active_reading_count,
        sum(iff(operational_status = 'ERROR', 1, 0)) as error_reading_count,
        sum(iff(operational_status = 'MAINTENANCE', 1, 0)) as maintenance_reading_count,

        sum(iff(is_reading_ts_parse_failed, 1, 0)) as timestamp_parse_failure_count,
        sum(iff(is_battery_soc_invalid, 1, 0)) as invalid_soc_count,
        sum(iff(is_motor_temp_suspicious, 1, 0)) as suspicious_motor_temp_count,
        sum(iff(is_odometer_invalid, 1, 0)) as invalid_odometer_count

    from sensor_fact
    group by 1, 2, 3

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['robot_id', 'report_date']) }} as robot_daily_sk,
        *
    from sensor_daily

)

select *
from final