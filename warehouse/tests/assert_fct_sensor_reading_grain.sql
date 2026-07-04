select
    reading_id,
    count(*) as row_count
from {{ ref('fct_sensor_reading') }}
group by reading_id
having count(*) > 1