-- Test VALUES clause syntax (Hive doesn't require parentheses)

-- Simple INSERT with VALUES
INSERT INTO test_table 
VALUES (1, 'first'), (2, 'second'), (3, 'third');

-- INSERT with column list
INSERT INTO users (id, name, email)
VALUES 
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com');

-- INSERT from SELECT
INSERT INTO user_summary
SELECT 
    user_id,
    COUNT(*) as event_count
FROM user_events
GROUP BY user_id;