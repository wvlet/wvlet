-- Test case for TABLESAMPLE syntax
-- This addresses parsing errors with TABLESAMPLE BERNOULLI statements

-- Basic TABLESAMPLE with BERNOULLI method (integer percentage)
SELECT * FROM information_schema.tables TABLESAMPLE BERNOULLI (10);

-- TABLESAMPLE with parenthesized table (reproduces the original error)  
SELECT * FROM (information_schema.tables t1) TABLESAMPLE BERNOULLI (5);

-- TABLESAMPLE with alias after sampling
SELECT * FROM information_schema.tables TABLESAMPLE BERNOULLI (20) as sampled_tables;

-- TABLESAMPLE with SYSTEM method (integer percentage)
SELECT * FROM information_schema.tables TABLESAMPLE SYSTEM (15);