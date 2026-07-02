with source as (

    select *
    from {{ source('raw', 'RAW_SENSOR_READINGS') }}

),

cleaned as (

    select
        nullif(trim(reading_id), '') as reading_id,
        nullif(trim(robot_id), '') as robot_id,

        reading_ts as reading_ts_raw,

        coalesce(
            try_to_timestamp_ntz(reading_ts),
            try_to_timestamp_ntz(reading_ts, 'MM/DD/YYYY HH24:MI:SS'),
            try_to_timestamp_ntz(reading_ts, 'MM/DD/YYYY HH12:MI:SS AM'),
            try_to_timestamp_ntz(reading_ts, 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz(reading_ts, 'YYYY-MM-DD"T"HH24:MI:SS')
        ) as reading_ts,

        case
            when reading_ts is not null
             and coalesce(
                try_to_timestamp_ntz(reading_ts),
                try_to_timestamp_ntz(reading_ts, 'MM/DD/YYYY HH24:MI:SS'),
                try_to_timestamp_ntz(reading_ts, 'MM/DD/YYYY HH12:MI:SS AM'),
                try_to_timestamp_ntz(reading_ts, 'YYYY-MM-DD HH24:MI:SS'),
                try_to_timestamp_ntz(reading_ts, 'YYYY-MM-DD"T"HH24:MI:SS')
             ) is null
            then true
            else false
        end as is_reading_ts_parse_failed,

        try_to_double(voltage_v) as battery_voltage_v,

        case
            when try_to_double(soc_pct) between 0 and 100
            then try_to_double(soc_pct)
            else null
        end as battery_soc_pct,

        case
            when try_to_double(soc_pct) is not null
             and not (try_to_double(soc_pct) between 0 and 100)
            then true
            else false
        end as is_battery_soc_invalid,

        try_to_double(battery_temp_c) as battery_temp_c_raw,

        case
            when try_to_double(battery_temp_c) > 60 then true
            else false
        end as is_battery_temp_suspicious,

        case
            when try_to_double(motor_temp_c) > 100 then true
            else false
        end as is_motor_temp_suspicious,

        try_to_double(motor_temp_c) as motor_temp_c_raw,
        try_to_double(rpm) as motor_rpm,
        try_to_double(load_pct) as motor_load_pct,
        try_to_double(vibration_g) as vibration_g,

        case
            when try_to_double(odometer_m) >= 0
            then try_to_double(odometer_m)
            else null
        end as odometer_m,

        case
            when try_to_double(odometer_m) is not null
             and try_to_double(odometer_m) < 0
            then true
            else false
        end as is_odometer_invalid,

        upper(trim(operational_status)) as operational_status,
        upper(trim(floor_condition)) as floor_condition,
        nullif(trim(zone_id), '') as zone_id,

        _loaded_at,
        _source_file

    from source

)

select *
from cleaned