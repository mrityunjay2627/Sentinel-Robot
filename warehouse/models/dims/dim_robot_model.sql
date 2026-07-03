with source as (

    select *
    from {{ ref('stg_robot_models') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['model_id']) }} as robot_model_sk,

        model_id,
        model_name,
        manufacturer,
        max_payload_kg,
        battery_capacity_wh,
        weight_kg,
        max_speed_mps,
        wear_rate,
        sensor_suite,

        current_timestamp() as dbt_loaded_at

    from source

)

select *
from final