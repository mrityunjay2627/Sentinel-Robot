-- A singular test to check for positive motor temperatures.
-- If this query returns any rows, the test will fail.

SELECT
    event_timestamp,
    motortemp
FROM
    {{ ref('stg_telemetry') }}
WHERE
    motortemp < 0