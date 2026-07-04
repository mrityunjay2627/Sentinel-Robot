with source as (

    select *
    from {{ source('raw', 'RAW_TASKS') }}

),

cleaned as (

    select
        nullif(trim(task_id), '') as task_id,
        nullif(trim(robot_id), '') as robot_id,

        upper(trim(task_type)) as task_type,
        upper(trim(status)) as status,

        try_to_number(priority) as priority,

        coalesce(
            try_to_timestamp_ntz(assigned_ts),
            try_to_timestamp_ntz(assigned_ts, 'MM/DD/YYYY HH24:MI:SS'),
            try_to_timestamp_ntz(assigned_ts, 'MM/DD/YYYY HH12:MI:SS AM'),
            try_to_timestamp_ntz(assigned_ts, 'YYYY-MM-DD HH24:MI:SS')
        ) as assigned_ts,

        coalesce(
            try_to_timestamp_ntz(started_ts),
            try_to_timestamp_ntz(started_ts, 'MM/DD/YYYY HH24:MI:SS'),
            try_to_timestamp_ntz(started_ts, 'MM/DD/YYYY HH12:MI:SS AM'),
            try_to_timestamp_ntz(started_ts, 'YYYY-MM-DD HH24:MI:SS')
        ) as started_ts,

        coalesce(
            try_to_timestamp_ntz(completed_ts),
            try_to_timestamp_ntz(completed_ts, 'MM/DD/YYYY HH24:MI:SS'),
            try_to_timestamp_ntz(completed_ts, 'MM/DD/YYYY HH12:MI:SS AM'),
            try_to_timestamp_ntz(completed_ts, 'YYYY-MM-DD HH24:MI:SS')
        ) as completed_ts,

        try_to_double(distance_m) as distance_m,
        try_to_double(payload_kg) as payload_kg,

        nullif(trim(source_zone_id), '') as source_zone_id,
        nullif(trim(destination_zone_id), '') as destination_zone_id,

        try_to_number(error_count) as error_count,

        case
            when task_id is null or trim(task_id) = '' then true
            else false
        end as is_missing_task_id,

        case
            when robot_id is null or trim(robot_id) = '' then true
            else false
        end as is_missing_robot_id,

        case
            when upper(trim(task_type)) not in ('PICK', 'PUT', 'MOVE', 'INSPECT', 'CHARGE', 'IDLE') then true
            else false
        end as is_invalid_task_type,

        case
            when upper(trim(status)) not in ('ASSIGNED', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT') then true
            else false
        end as is_invalid_status,

        case
            when assigned_ts is not null
             and started_ts is not null
             and started_ts < assigned_ts
            then true
            else false
        end as is_task_lifecycle_invalid,

        _loaded_at,
        _source_file

    from source

)

select *
from cleaned