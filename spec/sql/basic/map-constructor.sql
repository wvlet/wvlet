-- Test MAP constructor syntax support

-- MAP(key_array, value_array) syntax (this was the failing pattern)
SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2]);

-- MAP constructor with array access (from original failing pattern)  
SELECT MAP(ARRAY['2517', '2564'], ARRAY[999, 888])['2517'];

-- MAP constructor in WHERE clause (similar to original error)
SELECT * FROM (VALUES (1)) t(x) WHERE 1 <= MAP(ARRAY['key'], ARRAY[999])['key'];