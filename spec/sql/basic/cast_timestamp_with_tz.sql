-- Test CAST expressions with timestamp with time zone
SELECT CAST('2024-01-01' AS timestamp with time zone);
SELECT CAST('2024-01-01 12:00:00' AS timestamp without time zone);
SELECT CAST('2024-01-01' AS timestamp(3) with time zone);
SELECT CAST('2024-01-01' AS timestamp(6) without time zone);
SELECT CAST('2024-01-01' AS timestamp); -- Should work as before