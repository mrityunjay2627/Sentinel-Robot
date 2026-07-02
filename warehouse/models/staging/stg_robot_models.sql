with source as (

    select *
    from {{ source('raw', 'RAW_ROBOT_MODELS') }}

),

cleaned as (

    select
        nullif(trim(model_id), '') as model_id,
        nullif(trim(model_name), '') as model_name,
        nullif(trim(manufacturer), '') as manufacturer,

        try_to_double(max_payload_kg) as max_payload_kg,
        try_to_double(battery_capacity_wh) as battery_capacity_wh,
        try_to_double(weight_kg) as weight_kg,
        try_to_double(max_speed_mps) as max_speed_mps,
        try_to_double(wear_rate) as wear_rate,

        nullif(trim(sensor_suite), '') as sensor_suite,

        _loaded_at,
        _source_file

    from source

)

select *
from cleaned