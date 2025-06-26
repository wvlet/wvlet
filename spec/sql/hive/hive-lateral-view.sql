-- Hive LATERAL VIEW examples

-- Basic LATERAL VIEW with explode
SELECT order_id, item
FROM orders
LATERAL VIEW explode(items) items_table AS item;

-- LATERAL VIEW with posexplode (position + value)
SELECT order_id, pos, item
FROM orders
LATERAL VIEW posexplode(items) items_table AS pos, item;

-- Multiple LATERAL VIEWs
SELECT user_id, device, action
FROM user_sessions
LATERAL VIEW explode(devices) devices_table AS device
LATERAL VIEW explode(actions) actions_table AS action;

-- LATERAL VIEW with complex expressions
SELECT 
    user_id,
    day,
    hour,
    event_count
FROM user_activity
LATERAL VIEW explode(
    MAP(
        'monday', monday_events,
        'tuesday', tuesday_events,
        'wednesday', wednesday_events
    )
) day_table AS day, day_events
LATERAL VIEW posexplode(day_events) hour_table AS hour, event_count;

-- LATERAL VIEW OUTER (includes nulls)
SELECT user_id, tag
FROM users
LATERAL VIEW OUTER explode(tags) tags_table AS tag;

-- LATERAL VIEW with inline (for array of structs)
SELECT order_id, item_id, item_name, item_price
FROM orders
LATERAL VIEW inline(
    ARRAY(
        named_struct('id', 1, 'name', 'Item A', 'price', 10.99),
        named_struct('id', 2, 'name', 'Item B', 'price', 20.99)
    )
) items_table AS item_id, item_name, item_price;