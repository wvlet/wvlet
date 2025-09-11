-- Test cases for non-reserved keywords that can be used as column/table names
-- Following Trino's approach to maximize identifier flexibility

-- Test ALL as identifier
SELECT all FROM (VALUES ('all_value1'), ('all_value2')) AS t(all);
SELECT all, count FROM (VALUES ('total', 5), ('partial', 3)) AS t(all, count);

-- Test ADD as identifier  
SELECT add FROM (VALUES ('add_operation'), ('insert_op')) AS t(add);
SELECT add + 1 AS result FROM (VALUES (10), (20)) AS t(add);

-- Test COMMENT as identifier
SELECT comment FROM (VALUES ('This is a comment'), ('Another comment')) AS t(comment);
SELECT comment, length(comment) FROM (VALUES ('short'), ('very long comment text')) AS t(comment);

-- Test DESCRIBE as identifier
SELECT describe FROM (VALUES ('description1'), ('description2')) AS t(describe);

-- Test EXPLAIN as identifier
SELECT explain FROM (VALUES ('explanation text'), ('detailed info')) AS t(explain);

-- Test FETCH as identifier
SELECT fetch FROM (VALUES ('fetch_data'), ('retrieve_info')) AS t(fetch);

-- Test INTERVAL as identifier (distinguished from literal INTERVAL '1' DAY by context)
SELECT interval FROM (VALUES ('daily'), ('weekly'), ('monthly')) AS t(interval);
SELECT interval, duration FROM (VALUES ('hourly', 60), ('daily', 1440)) AS t(interval, duration);

-- Test INTERVAL in arithmetic expressions (multiplication and division work)
SELECT interval * 2 FROM (VALUES (5), (10)) AS t(interval);
SELECT interval / 2 FROM (VALUES (10), (20)) AS t(interval);

-- Note: INTERVAL + literal and INTERVAL - literal are reserved for interval literals
-- These would be parsed as interval literals and cause errors if not followed by time unit:
-- SELECT interval + 1 FROM (VALUES (5)) AS t(interval); -- Would error: Expected time unit
-- SELECT interval - 1 FROM (VALUES (5)) AS t(interval); -- Would error: Expected time unit

-- Test LIMIT as identifier
SELECT limit FROM (VALUES (100), (200), (500)) AS t(limit);
SELECT limit * 2 AS double_limit FROM (VALUES (50), (75)) AS t(limit);

-- Test MERGE as identifier
SELECT merge FROM (VALUES ('merge_strategy'), ('combine_method')) AS t(merge);

-- Test OFFSET as identifier
SELECT offset FROM (VALUES (10), (20), (30)) AS t(offset);
SELECT offset + 5 AS adjusted_offset FROM (VALUES (0), (10)) AS t(offset);

-- Test PARTITION as identifier
SELECT partition FROM (VALUES ('partition_1'), ('partition_2')) AS t(partition);
SELECT partition, partition_size FROM (VALUES ('p1', 1024), ('p2', 2048)) AS t(partition, partition_size);

-- Test PLAN as identifier
SELECT plan FROM (VALUES ('plan_a'), ('plan_b'), ('plan_premium')) AS t(plan);
SELECT plan, price FROM (VALUES ('basic', 10), ('premium', 25)) AS t(plan, price);

-- Test RESET as identifier
SELECT reset FROM (VALUES ('reset_password'), ('factory_reset')) AS t(reset);

-- Test SESSION as identifier
SELECT session FROM (VALUES ('session_123'), ('session_456')) AS t(session);
SELECT session, user_id FROM (VALUES ('sess_1', 100), ('sess_2', 200)) AS t(session, user_id);

-- Test SET as identifier
SELECT set FROM (VALUES ('set_a'), ('set_b'), ('empty_set')) AS t(set);
SELECT set, size FROM (VALUES ('numbers', 10), ('letters', 26)) AS t(set, size);

-- Test SHOW as identifier
SELECT show FROM (VALUES ('show_name'), ('episode_title')) AS t(show);

-- Test UPDATE as identifier
SELECT update FROM (VALUES ('update_v1'), ('update_v2')) AS t(update);
SELECT update, version FROM (VALUES ('patch', '1.1'), ('major', '2.0')) AS t(update, version);

-- Test USE as identifier
SELECT use FROM (VALUES ('primary_use'), ('secondary_use')) AS t(use);

-- Test IMPLEMENTATION as identifier
SELECT implementation FROM (VALUES ('java_impl'), ('scala_impl')) AS t(implementation);

-- Test multiple non-reserved keywords together
SELECT 
  all AS all_items,
  plan AS subscription_plan,
  limit AS max_items,
  offset AS start_position,
  session AS user_session
FROM (VALUES 
  ('everything', 'premium', 1000, 0, 'sess_abc'),
  ('filtered', 'basic', 100, 50, 'sess_def')
) AS t(all, plan, limit, offset, session);

-- Test in WHERE clauses
SELECT * FROM (VALUES ('plan1', 100), ('plan2', 200)) AS t(plan, limit)
WHERE plan = 'plan1' AND limit > 50;

-- Test in GROUP BY
SELECT plan, count(*) AS cnt
FROM (VALUES ('basic'), ('basic'), ('premium'), ('premium'), ('premium')) AS t(plan)
GROUP BY plan;

-- Test with table aliases using non-reserved keywords
SELECT p.plan, l.limit
FROM (VALUES ('basic'), ('premium')) AS p(plan)
JOIN (VALUES ('basic', 100), ('premium', 1000)) AS l(plan, limit) ON p.plan = l.plan;

-- Test nested expressions with non-reserved keywords
SELECT 
  concat(plan, '_', CAST(limit AS VARCHAR)) AS plan_description,
  CASE 
    WHEN limit > 500 THEN 'high'
    WHEN limit > 100 THEN 'medium' 
    ELSE 'low'
  END AS limit_category
FROM (VALUES ('basic', 100), ('premium', 1000), ('starter', 50)) AS t(plan, limit);

-- Test function calls with non-reserved keyword column names
SELECT 
  upper(comment) AS upper_comment,
  length(describe) AS describe_length,
  substring(explain, 1, 10) AS explain_preview
FROM (VALUES 
  ('this is a comment', 'brief description', 'detailed explanation text'),
  ('another comment', 'longer description here', 'more explanation details')
) AS t(comment, describe, explain);