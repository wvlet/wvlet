-- Hive-specific function examples

-- Array aggregation
SELECT collect_list(user_id) as user_ids,
       collect_set(user_id) as unique_user_ids
FROM events
GROUP BY session_id;

-- Regular expression matching
SELECT *
FROM logs
WHERE regexp(message, 'ERROR.*timeout');

-- Array operations with LATERAL VIEW
SELECT session_id, user_action
FROM events
LATERAL VIEW explode(actions) actions_table AS user_action;

-- Map creation
SELECT user_id,
       MAP(ARRAY['name', 'email'], ARRAY[name, email]) as user_info
FROM users;

-- Struct/Map field access
SELECT user_id,
       user_info['name'] as name,
       user_info['email'] as email
FROM user_profiles;

-- Multiple LATERAL VIEWs
SELECT order_id, item_id, tag
FROM orders
LATERAL VIEW explode(items) items_table AS item_id
LATERAL VIEW explode(tags) tags_table AS tag;

-- Window functions
SELECT user_id,
       event_time,
       collect_list(event_type) OVER (PARTITION BY user_id ORDER BY event_time) as event_history
FROM user_events;