-- Test MAP constructor syntax support

-- Traditional MAP {key: value} syntax
SELECT MAP {'a': 1, 'b': 2};

-- New MAP(key_array, value_array) syntax
SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2]);

-- MAP constructor with array access (from original failing pattern)
SELECT MAP(ARRAY['2517', '2564'], ARRAY[999, 888])['2517'];