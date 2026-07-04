with fact as (

    select *
    from {{ ref('fct_sensor_reading') }}

),

robot as (

    select *
    from {{ ref('dim_robot') }}

),

model as (

    select *
    from {{ ref('dim_robot_model') }}

),

zone as (

    select *
    from {{ ref('dim_zone') }}

),

final as (

    select
        f.sensor_reading_sk,
        f.reading_id,
        f.robot_id,
        f.reading_ts,
        f.reading_date_key,

        r.serial_number,
        r.model_id,
        m.model_name,
        m.manufacturer,
        m.max_payload_kg,
        m.battery_capacity_wh,
        m.weight_kg,
        m.max_speed_mps,
        m.wear_rate,

        r.zone_id as robot_scd_zone_id,
        z.zone_name,
        z.site,
        z.zone_type,
        z.has_charging,

        r.firmware_version,
        r.owner_team,

        f.battery_voltage_v,
        f.battery_soc_pct,
        f.battery_temp_c,
        f.motor_temp_c,
        f.motor_rpm,
        f.motor_load_pct,
        f.vibration_g,
        f.odometer_m,

        f.operational_status,
        f.floor_condition,

        f.is_reading_ts_parse_failed,
        f.is_battery_soc_invalid,
        f.is_battery_temp_suspicious,
        f.is_motor_temp_suspicious,
        f.is_odometer_invalid,

        current_timestamp() as dbt_loaded_at

    from fact f

    left join robot r
        on f.robot_sk = r.robot_sk

    left join model m
        on r.model_id = m.model_id

    left join zone z
        on r.zone_id = z.zone_id

)

select *
from final