-- Test nested array type
SELECT CAST(array(array(1)) AS array<array<int>>);