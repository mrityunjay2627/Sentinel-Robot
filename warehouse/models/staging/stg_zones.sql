with source as (

    select *
    from {{ source('raw', 'RAW_ZONES') }}

),

cleaned as (

    select
        nullif(trim(zone_id), '') as zone_id,
        nullif(trim(zone_name), '') as zone_name,
        nullif(trim(site), '') as site,
        upper(trim(zone_type)) as zone_type,

        try_to_double(floor_area_sqm) as floor_area_sqm,
        try_to_number(max_robots) as max_robots,

        case
            when lower(trim(has_charging)) in ('true', '1', 'yes', 'y') then true
            when lower(trim(has_charging)) in ('false', '0', 'no', 'n') then false
            else null
        end as has_charging,

        _loaded_at,
        _source_file

    from source

)

select *
from cleaned