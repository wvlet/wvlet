-- Test Hive-style array and map types with angle bracket syntax
-- This syntax is commonly used in Hive SQL for defining complex types

-- Basic array<T> type in CAST expression
SELECT CAST(array() AS array<bigint>);
SELECT CAST(array() AS array<int>);
SELECT CAST(array() AS array<string>);
SELECT CAST(array() AS array<varchar>);
SELECT CAST(array() AS array<double>);
SELECT CAST(array() AS array<boolean>);
SELECT CAST(array() AS array<date>);
SELECT CAST(array() AS array<timestamp>);

-- Array with elements cast to specific types
SELECT CAST(array(1, 2, 3) AS array<bigint>);
SELECT CAST(array('a', 'b', 'c') AS array<string>);
SELECT CAST(array(true, false) AS array<boolean>);
SELECT CAST(array(1.5, 2.5, 3.5) AS array<double>);

-- Map types with angle brackets
SELECT CAST(map('a', 1, 'b', 2) AS map<string, int>);
SELECT CAST(map('key1', 'value1') AS map<varchar, varchar>);
SELECT CAST(map(1, 'one', 2, 'two') AS map<int, string>);

-- Nested array types
SELECT CAST(array(array(1, 2), array(3, 4)) AS array<array<int>>);
SELECT CAST(array(array('a'), array('b')) AS array<array<string>>);

-- Complex nested types
SELECT CAST(array(map('x', 1)) AS array<map<string, int>>);
SELECT CAST(map('key', array(1, 2, 3)) AS map<string, array<int>>);

-- Mixed complex types
SELECT CAST(
  map('numbers', array(1, 2, 3), 'doubles', array(1.1, 2.2)) 
  AS map<string, array<double>>
);

-- Array of arrays of maps
SELECT CAST(
  array(
    array(map('a', 1), map('b', 2)),
    array(map('c', 3))
  ) AS array<array<map<string, int>>>
);

-- Deeply nested types
SELECT CAST(
  map('data', array(map('nested', array(1, 2, 3))))
  AS map<string, array<map<string, array<int>>>>
);