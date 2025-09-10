-- Test Hive-style array type syntax with angle brackets
-- Basic array<T> type in CAST expression
SELECT CAST(array() AS array<bigint>);

-- Array with elements
SELECT CAST(array(1, 2, 3) AS array<bigint>);

-- Nested array types
SELECT CAST(array(array(1, 2), array(3, 4)) AS array<array<bigint>>);

-- Map type with angle brackets
SELECT CAST(map('a', 1, 'b', 2) AS map<varchar, bigint>);

-- Complex nested types
SELECT CAST(array(map('x', 1)) AS array<map<varchar, bigint>>);

-- Array of various numeric types
SELECT CAST(array(1) AS array<int>);
SELECT CAST(array(1) AS array<integer>);
SELECT CAST(array(1) AS array<smallint>);
SELECT CAST(array(1) AS array<tinyint>);
SELECT CAST(array(1.5) AS array<double>);
SELECT CAST(array(1.5) AS array<float>);
SELECT CAST(array(1.5) AS array<real>);
SELECT CAST(array(1.5) AS array<decimal>);

-- Array of string types
SELECT CAST(array('hello') AS array<varchar>);
SELECT CAST(array('hello') AS array<string>);
SELECT CAST(array('hello') AS array<char>);

-- Array of temporal types
SELECT CAST(array(DATE '2024-01-01') AS array<date>);
SELECT CAST(array(TIMESTAMP '2024-01-01 12:00:00') AS array<timestamp>);

-- Array of boolean
SELECT CAST(array(true, false) AS array<boolean>);

-- Struct/Row types with angle brackets (if supported)
-- SELECT CAST(array(ROW(1, 'a')) AS array<struct<id:bigint, name:varchar>>);