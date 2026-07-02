with source as (

    select *
    from {{ source('raw', 'RAW_ROBOTS') }}

),

cleaned as (

    select
        nullif(trim(robot_id), '') as robot_id,
        nullif(trim(serial_number), '') as serial_number,
        nullif(trim(model_id), '') as model_id,

        try_to_date(commissioned_date) as commissioned_date,

        nullif(trim(home_zone_id), '') as home_zone_id,
        nullif(trim(initial_firmware), '') as initial_firmware,
        nullif(trim(owner_team), '') as owner_team,
        nullif(trim(ip_address), '') as ip_address,
        nullif(trim(mac_address), '') as mac_address,

        _loaded_at,
        _source_file

    from source

)

select *
from cleaned