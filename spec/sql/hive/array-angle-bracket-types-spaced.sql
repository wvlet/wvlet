-- Test Hive-style array and map types with angle bracket syntax
-- Using spaces between >> to avoid tokenization issues

-- Basic array<T> type in CAST expression
SELECT CAST(array() AS array<bigint>);
SELECT CAST(array() AS array<int>);
SELECT CAST(array() AS array<string>);
SELECT CAST(array() AS array<varchar>);
SELECT CAST(array() AS array<double>);
SELECT CAST(array() AS array<boolean>);

-- Array with elements cast to specific types
SELECT CAST(array(1, 2, 3) AS array<bigint>);
SELECT CAST(array('a', 'b', 'c') AS array<string>);
SELECT CAST(array(true, false) AS array<boolean>);

-- Map types with angle brackets
SELECT CAST(map('a', 1, 'b', 2) AS map<string, int>);
SELECT CAST(map('key1', 'value1') AS map<varchar, varchar>);

-- Nested array types (with spaces to avoid >> tokenization issue)
SELECT CAST(array(array(1, 2), array(3, 4)) AS array<array<int> >);
SELECT CAST(array(array('a'), array('b')) AS array<array<string> >);

-- Complex nested types
SELECT CAST(array(map('x', 1)) AS array<map<string, int> >);
SELECT CAST(map('key', array(1, 2, 3)) AS map<string, array<int> >);

-- Mixed complex types
SELECT CAST(
  map('numbers', array(1, 2, 3), 'doubles', array(1.1, 2.2)) 
  AS map<string, array<double> >
);