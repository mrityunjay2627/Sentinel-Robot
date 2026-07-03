with robots as (

    select *
    from {{ ref('stg_robots') }}

),

changes as (

    select *
    from {{ ref('stg_robot_attribute_changes') }}
    where changed_at is not null

),

robot_start_state as (

    select
        robot_id,
        serial_number,
        model_id,
        commissioned_date,
        home_zone_id as zone_id,
        initial_firmware as firmware_version,
        owner_team,
        commissioned_date::timestamp_ntz as valid_from
    from robots

),

change_events as (

    select
        c.robot_id,
        r.serial_number,
        r.model_id,
        r.commissioned_date,

        max(case when c.attribute = 'ZONE' then c.new_value end)
            over (
                partition by c.robot_id
                order by c.changed_at
                rows between unbounded preceding and current row
            ) as changed_zone_id,

        max(case when c.attribute = 'FIRMWARE' then c.new_value end)
            over (
                partition by c.robot_id
                order by c.changed_at
                rows between unbounded preceding and current row
            ) as changed_firmware_version,

        max(case when c.attribute = 'TEAM' then c.new_value end)
            over (
                partition by c.robot_id
                order by c.changed_at
                rows between unbounded preceding and current row
            ) as changed_owner_team,

        c.changed_at as valid_from

    from changes c
    inner join robots r
        on c.robot_id = r.robot_id

),

combined_versions as (

    select
        robot_id,
        serial_number,
        model_id,
        commissioned_date,
        zone_id,
        firmware_version,
        owner_team,
        valid_from
    from robot_start_state

    union all

    select
        robot_id,
        serial_number,
        model_id,
        commissioned_date,
        changed_zone_id as zone_id,
        changed_firmware_version as firmware_version,
        changed_owner_team as owner_team,
        valid_from
    from change_events

),

forward_filled as (

    select
        robot_id,
        serial_number,
        model_id,
        commissioned_date,

        last_value(zone_id ignore nulls) over (
            partition by robot_id
            order by valid_from
            rows between unbounded preceding and current row
        ) as zone_id,

        last_value(firmware_version ignore nulls) over (
            partition by robot_id
            order by valid_from
            rows between unbounded preceding and current row
        ) as firmware_version,

        last_value(owner_team ignore nulls) over (
            partition by robot_id
            order by valid_from
            rows between unbounded preceding and current row
        ) as owner_team,

        valid_from

    from combined_versions

),

versioned as (

    select
        *,
        lead(valid_from) over (
            partition by robot_id
            order by valid_from
        ) as next_valid_from
    from forward_filled

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['robot_id', 'valid_from']) }} as robot_sk,

        robot_id,
        serial_number,
        model_id,
        zone_id,
        firmware_version,
        owner_team,
        commissioned_date,

        valid_from,
        coalesce(next_valid_from, to_timestamp_ntz('9999-12-31')) as valid_to,

        case
            when next_valid_from is null then true
            else false
        end as is_current,

        current_timestamp() as dbt_loaded_at

    from versioned

)

select *
from final