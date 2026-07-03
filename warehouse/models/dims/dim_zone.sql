with source as (

    select *
    from {{ ref('stg_zones') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['zone_id']) }} as zone_sk,

        zone_id,
        zone_name,
        site,
        zone_type,
        floor_area_sqm,
        max_robots,
        has_charging,

        current_timestamp() as dbt_loaded_at

    from source

)

select *
from final