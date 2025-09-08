-- Test escaped quotes in string literals

-- Simple escaped quote
SELECT 'Don''t worry' as message1;

-- Multiple escaped quotes
SELECT 'It''s a ''test'' string' as message2;

-- Escaped quotes in function arguments
SELECT regexp_replace('test', '(''quoted'')', '$1') as result1;

-- Complex regex pattern with escaped quotes (similar to the original error)
SELECT regexp_replace('test string', '(?ims)(.*)(INSERT INTO)([\\s\\n]*(''?\\S+''?))?(.*)', '$2$3 $1$4') as result2;

-- Standard CASE expression with escaped quotes
SELECT 
  CASE 
    WHEN col1 = 'O''Reilly' THEN 'Author''s name contains apostrophe'
    WHEN col1 LIKE '%''%' THEN 'Contains quote character'  
    ELSE 'No quotes'
  END as quote_check
FROM VALUES ('O''Reilly'), ('Smith'), ('D''Angelo') AS t(col1);

-- Standard CONCAT with escaped quotes
SELECT CONCAT('Hello, ', 'it''s me') as greeting;