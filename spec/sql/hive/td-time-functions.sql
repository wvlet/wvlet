-- Test Treasure Data time functions
-- TD_TIME_RANGE function with nested TD_TIME_ADD
SELECT
    client_id,
    count(*) as access_count
FROM access
WHERE TD_TIME_RANGE(time, TD_TIME_ADD(1465938349, '-1h'), 1465938349)
    AND is_uu = 1
GROUP BY client_id;

-- TD_TIME_FORMAT function
SELECT TD_TIME_FORMAT(TD_TIME_ADD(1465938349, '-1h'), 'yyyy-MM-dd HH:00:00', 'JST') AS target_ymdh;

-- TD_TIME_ADD function with negative offset
SELECT TD_TIME_ADD(1465938349, '-1h') as time_minus_hour;

-- TD_TIME_RANGE with string parameters
SELECT * FROM logs WHERE TD_TIME_RANGE(time, '2016-06-15 00:00:00', '2016-06-15 23:59:59');
