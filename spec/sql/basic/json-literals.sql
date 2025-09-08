-- JSON literal tests for Trino compatibility

-- Test basic JSON null
SELECT json 'null';

-- Test JSON number  
SELECT json '42';

-- Test JSON boolean
SELECT json 'true';
SELECT json 'false';

-- Test JSON array with numbers
SELECT json '[1, 2, 3]';

-- Test JSON with COALESCE function (original error case)
SELECT concat('{', 'customer_id:', json_format(COALESCE(TRY_CAST(customer_id AS json), json 'null')), '}');

-- Test JSON in CASE expressions
SELECT
  CASE 
    WHEN 1 IS NOT NULL THEN json 'true'
    ELSE json 'false'
  END;