select *
from {{ ref('stg_sensor_readings') }}
where upper(trim(robot_id)) in ('NULL', 'N/A', 'NA', 'NAN', 'NONE', '-', '#N/A')
   or upper(trim(reading_id)) in ('NULL', 'N/A', 'NA', 'NAN', 'NONE', '-', '#N/A')