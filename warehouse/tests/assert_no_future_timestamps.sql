select *
from {{ ref('fct_sensor_reading') }}
where reading_ts > current_timestamp()