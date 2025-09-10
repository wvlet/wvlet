-- Test subquery as standalone statement
(
   SELECT array_join(array_agg(concat(concat('MAP(ARRAY[''tag_discriminator''], ARRAY[''', f_77393), '''])')), ',') f_32cde
   FROM
     d_3145c.t_ab67a
   WHERE (f_6014e IN ('purchase_shop_todaysdish'))
);

-- Simple subquery statement
(SELECT 1);

-- Subquery with VALUES
(VALUES (1, 'a'), (2, 'b'));