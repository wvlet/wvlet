-- Test Hive-style array constructor with parentheses
-- Simple array with string literals
SELECT array('bias');

-- Array with multiple string literals
SELECT array('car_color', 'car_engine');

-- Array with mixed types
SELECT array('a', 'b', 'c');

-- Array with numbers
SELECT array(1, 2, 3);

-- Nested array function call
SELECT array_concat(
    array('bias'),
    array('car_color', 'car_engine', 'car_grade')
);

-- Traditional bracket syntax should still work
SELECT array['x', 'y', 'z'];

-- Mixed usage in complex query
SELECT array_concat(
    array('prefix'),
    array['middle1', 'middle2'],
    array('suffix')
);