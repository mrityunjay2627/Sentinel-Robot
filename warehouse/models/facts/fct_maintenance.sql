select
    cast(null as varchar) as maintenance_sk,
    cast(null as varchar) as ticket_id,
    cast(null as varchar) as robot_id,
    cast(null as varchar) as failure_type,
    cast(null as varchar) as severity,

    cast(null as timestamp_ntz) as reported_ts,
    cast(null as timestamp_ntz) as diagnosed_ts,
    cast(null as timestamp_ntz) as parts_ordered_ts,
    cast(null as timestamp_ntz) as repaired_ts,
    cast(null as timestamp_ntz) as closed_ts,

    cast(null as number) as reported_date_key,
    cast(null as number) as diagnosed_date_key,
    cast(null as number) as repaired_date_key,
    cast(null as number) as closed_date_key,

    cast(null as varchar) as root_cause,
    cast(null as double) as cost_usd,
    cast(null as double) as downtime_hours,

    current_timestamp() as dbt_loaded_at

where 1 = 0