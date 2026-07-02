with source as (

    select *
    from {{ source('raw', 'RAW_ROBOT_ATTRIBUTE_CHANGES') }}

),

cleaned as (

    select
        nullif(trim(change_id), '') as change_id,
        nullif(trim(robot_id), '') as robot_id,

        coalesce(
            try_to_timestamp_ntz(changed_at),
            try_to_timestamp_ntz(changed_at, 'MM/DD/YYYY HH24:MI:SS'),
            try_to_timestamp_ntz(changed_at, 'MM/DD/YYYY HH12:MI:SS AM'),
            try_to_timestamp_ntz(changed_at, 'YYYY-MM-DD HH24:MI:SS')
        ) as changed_at,

        case
            when upper(trim(attribute)) in ('FIRMWARE', 'INITIAL_FIRMWARE', 'FIRMWARE_VERSION') then 'FIRMWARE'
            when upper(trim(attribute)) in ('ZONE', 'HOME_ZONE_ID', 'HOME_ZONE','ZONE_ID') then 'ZONE'
            when upper(trim(attribute)) in ('TEAM', 'OWNER_TEAM') then 'TEAM'
        else upper(trim(attribute))
        end as attribute,
        nullif(trim(old_value), '') as old_value,
        nullif(trim(new_value), '') as new_value,

        case
            when changed_at is not null
             and coalesce(
                try_to_timestamp_ntz(changed_at),
                try_to_timestamp_ntz(changed_at, 'MM/DD/YYYY HH24:MI:SS'),
                try_to_timestamp_ntz(changed_at, 'MM/DD/YYYY HH12:MI:SS AM'),
                try_to_timestamp_ntz(changed_at, 'YYYY-MM-DD HH24:MI:SS')
             ) is null
            then true
            else false
        end as is_changed_at_parse_failed,

        _loaded_at,
        _source_file

    from source

)

select *
from cleaned