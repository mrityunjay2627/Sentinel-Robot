with readings as (

    select *
    from {{ ref('stg_sensor_readings') }}
    where reading_id is not null
      and robot_id is not null

),

robot_dim as (

    select *
    from {{ ref('dim_robot') }}

),

zone_dim as (

    select *
    from {{ ref('dim_zone') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['r.reading_id']) }} as sensor_reading_sk,

        r.reading_id,
        r.robot_id,
        d.robot_sk,
        z.zone_sk,

        r.reading_ts,
        to_number(to_char(to_date(r.reading_ts), 'YYYYMMDD')) as reading_date_key,

        r.battery_voltage_v,
        r.battery_soc_pct,
        r.battery_temp_c_raw as battery_temp_c,
        r.motor_temp_c_raw as motor_temp_c,
        r.motor_rpm,
        r.motor_load_pct,
        r.vibration_g,
        r.odometer_m,

        r.operational_status,
        r.floor_condition,
        r.zone_id,

        r.is_reading_ts_parse_failed,
        r.is_battery_soc_invalid,
        r.is_battery_temp_suspicious,
        r.is_motor_temp_suspicious,
        r.is_odometer_invalid,

        r._loaded_at as raw_loaded_at,
        current_timestamp() as dbt_loaded_at

    from readings r

    left join robot_dim d
        on r.robot_id = d.robot_id
       and r.reading_ts >= d.valid_from
       and r.reading_ts < d.valid_to

    left join zone_dim z
        on r.zone_id = z.zone_id

)

select *
from final