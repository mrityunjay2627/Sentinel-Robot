select *
from {{ ref('stg_tasks') }}
where is_missing_task_id is null
   or is_missing_robot_id is null
   or is_invalid_task_type is null
   or is_invalid_status is null
   or is_task_lifecycle_invalid is null