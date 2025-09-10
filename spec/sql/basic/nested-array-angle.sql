-- Test nested array type
SELECT CAST(array(array(1, 2)) AS array<array<bigint>>);