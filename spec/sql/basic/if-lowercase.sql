-- Test if expression with lowercase
select
    if(1 > 0, 'true', 'false') as result1,
    if(0 > 1, 'true', 'false') as result2;