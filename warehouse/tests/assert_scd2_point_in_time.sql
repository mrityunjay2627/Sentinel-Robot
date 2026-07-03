with robot_versions as (

    select
        robot_id,
        valid_from,
        valid_to
    from {{ ref('dim_robot') }}

),

overlaps as (

    select
        a.robot_id,
        a.valid_from as a_valid_from,
        a.valid_to as a_valid_to,
        b.valid_from as b_valid_from,
        b.valid_to as b_valid_to
    from robot_versions a
    inner join robot_versions b
        on a.robot_id = b.robot_id
       and a.valid_from < b.valid_to
       and b.valid_from < a.valid_to
       and a.valid_from <> b.valid_from

)

select *
from overlaps