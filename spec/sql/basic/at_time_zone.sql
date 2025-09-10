-- AT TIME ZONE support tests

-- Basic AT TIME ZONE with timestamp cast
SELECT CAST('2024-01-01 12:00:00' AS timestamp) AT TIME ZONE 'Asia/Tokyo' AS tokyo_time;

-- AT TIME ZONE with format_datetime function
SELECT 
  format_datetime(CAST('2024-01-01 12:00:00' AS timestamp) AT TIME ZONE 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss') AS formatted_time;

-- Multiple AT TIME ZONE expressions
SELECT
  current_timestamp AT TIME ZONE 'UTC' AS utc_time,
  current_timestamp AT TIME ZONE 'America/New_York' AS ny_time,
  current_timestamp AT TIME ZONE 'Europe/London' AS london_time;

-- AT TIME ZONE in WHERE clause using VALUES
SELECT *
FROM (VALUES ('2024-01-01 12:00:00')) AS t(timestamp_col)
WHERE CAST(timestamp_col AS timestamp) AT TIME ZONE 'Asia/Tokyo' >= '2024-01-01';

-- AT TIME ZONE with from_unixtime
SELECT 
  from_unixtime(1704067200) AT TIME ZONE 'Asia/Tokyo' AS tokyo_time;

-- Nested AT TIME ZONE expressions
SELECT
  format_datetime(
    from_unixtime((1704067200 - ((9 * 60) * 60))) AT TIME ZONE 'Asia/Tokyo',
    'yyyy-MM-dd HH:mm:ss'
  ) AS adjusted_time;

-- AT TIME ZONE with date_trunc
SELECT *
FROM (VALUES (1704067200)) AS t(unix_ts)
WHERE from_unixtime(unix_ts) AT TIME ZONE 'Asia/Tokyo' >= 
      (date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo') - INTERVAL '5' DAY);

-- Complex expression from the original error case
SELECT
  format_datetime(CAST(f_d2c04 AS timestamp) AT TIME ZONE 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss') AS f_c63c7,
  format_datetime(current_timestamp AT TIME ZONE 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss') AS f_41dab,
  f_3a5c8,
  f_26d28
FROM (VALUES 
  ('2024-01-01 12:00:00', '4qnD4DalaL0FspB2aArK', 'value1', 1704067200)
) AS t(f_d2c04, f_3a5c8, f_26d28, f_9d304)
WHERE ((f_3a5c8 = '4qnD4DalaL0FspB2aArK') AND 
       (from_unixtime((f_9d304 - ((9 * 60) * 60))) AT TIME ZONE 'Asia/Tokyo' >= 
        (date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo') - INTERVAL '5' DAY)))
ORDER BY f_c63c7 DESC;