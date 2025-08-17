SELECT
    robotid,
    -- The timestamp from Kinesis is in Unix seconds, convert it to a proper timestamp
    from_unixtime(timestamp) AS event_timestamp,
    motortemp,
    batteryvoltage,
    vibration,
    -- These are the partition columns created by the crawler
    year,
    month,
    day,
    hour
FROM
    {{ source('raw_data', 'telemetry') }}