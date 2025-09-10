-- Test array<T> type syntax with angle brackets
SELECT CAST(array() AS array<bigint>);
SELECT CAST(array(1, 2, 3) AS array<bigint>);
SELECT CAST(array(1, 2, 3) AS array<integer>);
SELECT CAST(array('a', 'b') AS array<varchar>);

-- Nested array types
SELECT CAST(array(array(1, 2)) AS array<array<bigint>>);

-- Map type with angle brackets
SELECT CAST(map('a', 1) AS map<varchar, bigint>);