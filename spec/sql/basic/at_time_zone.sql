-- AT TIME ZONE support tests

-- Basic AT TIME ZONE with timestamp cast
SELECT CAST('2024-01-01 12:00:00' AS timestamp) AT TIME ZONE 'Asia/Tokyo' AS tokyo_time;

-- Multiple AT TIME ZONE expressions
SELECT
  current_timestamp AT TIME ZONE 'UTC' AS utc_time,
  current_timestamp AT TIME ZONE 'America/New_York' AS ny_time,
  current_timestamp AT TIME ZONE 'Europe/London' AS london_time;

-- AT TIME ZONE in WHERE clause using VALUES
SELECT *
FROM (VALUES ('2024-01-01 12:00:00')) AS t(timestamp_col)
WHERE CAST(timestamp_col AS timestamp) AT TIME ZONE 'Asia/Tokyo' >= '2024-01-01';

SELECT
  '2025-01-01'::TIMESTAMP AT TIME ZONE 'Asia/Tokyo';
