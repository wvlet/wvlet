-- JSON literal tests for Trino compatibility

-- Test basic JSON null
SELECT json 'null';

-- Test JSON string
SELECT json '"hello"';

-- Test JSON number
SELECT json '42';

-- Test JSON boolean
SELECT json 'true';
SELECT json 'false';

-- Test JSON object
SELECT json '{"name": "Alice", "age": 30}';

-- Test JSON array
SELECT json '[1, 2, 3]';

-- Test complex JSON object
SELECT json '{"customer_id": 123, "orders": [{"id": 1, "amount": 100.50}, {"id": 2, "amount": 75.25}]}';

-- Test JSON with COALESCE function (from the original error case)
SELECT concat('{', '"customer_id":', json_format(COALESCE(TRY_CAST("customer_id" AS json), json 'null')), '}');

-- Test JSON in expressions
SELECT
  CASE 
    WHEN 1 IS NOT NULL THEN json '{"status": "active"}'
    ELSE json '{"status": "inactive"}'
  END;