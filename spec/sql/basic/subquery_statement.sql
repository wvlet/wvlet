-- Test subquery as standalone statement with VALUES
(
   SELECT array_join(array_agg(concat(concat('MAP(ARRAY[''tag_discriminator''], ARRAY[''', col1), '''])')), ',') f_32cde
   FROM
     (VALUES ('value1', 'purchase_shop_todaysdish'), ('value2', 'purchase_shop_todaysdish')) AS t(col1, col2)
   WHERE (col2 IN ('purchase_shop_todaysdish'))
);

-- Simple subquery statement
(SELECT 1);

-- Subquery with VALUES
(VALUES (1, 'a'), (2, 'b'));