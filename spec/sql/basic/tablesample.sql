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

-- TABLESAMPLE with DECIMAL percentage
SELECT * FROM information_schema.tables TABLESAMPLE BERNOULLI (DECIMAL '12');

-- DuckDB TABLESAMPLE with percentage symbol
SELECT * FROM information_schema.tables TABLESAMPLE BERNOULLI (10%);

-- DuckDB USING SAMPLE with row count
SELECT * FROM information_schema.tables USING SAMPLE 5;

-- DuckDB USING SAMPLE with explicit rows keyword
SELECT * FROM information_schema.tables USING SAMPLE 5 rows;

-- DuckDB USING SAMPLE with percentage
SELECT * FROM information_schema.tables USING SAMPLE 10%;

-- DuckDB USING SAMPLE with explicit percent keyword  
SELECT * FROM information_schema.tables USING SAMPLE 10 percent;

-- DuckDB USING SAMPLE with reservoir method
SELECT * FROM information_schema.tables USING SAMPLE reservoir(10%);