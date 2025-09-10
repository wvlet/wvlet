-- Test map type with angle brackets
SELECT CAST(map('a', 1) AS map<varchar, bigint>);