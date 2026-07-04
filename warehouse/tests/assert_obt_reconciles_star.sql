with fact_count as (

    select count(*) as row_count
    from {{ ref('fct_sensor_reading') }}

),

obt_count as (

    select count(*) as row_count
    from {{ ref('obt_sensor_readings') }}

)

select
    fact_count.row_count as fact_row_count,
    obt_count.row_count as obt_row_count
from fact_count
cross join obt_count
where fact_count.row_count <> obt_count.row_count