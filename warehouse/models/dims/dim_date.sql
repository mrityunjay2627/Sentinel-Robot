with date_spine as (

    select
        dateadd(day, seq4(), to_date('2024-01-01')) as date_day
    from table(generator(rowcount => 2000))

),

final as (

    select
        to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
        date_day,

        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,
        monthname(date_day) as month_name,
        day(date_day) as day_of_month,
        dayofweek(date_day) as day_of_week,
        dayname(date_day) as day_name,
        weekofyear(date_day) as week_of_year,

        case
            when dayofweek(date_day) in (0, 6) then true
            else false
        end as is_weekend

    from date_spine

)

select *
from final