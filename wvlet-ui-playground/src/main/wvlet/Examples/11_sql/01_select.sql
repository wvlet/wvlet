-- SQL: select, where, order by
-- Open this example in SQL mode to see the equivalent Wvlet query.
select c_name, c_acctbal
from customer
where c_mktsegment = 'BUILDING' and c_acctbal > 9000
order by c_acctbal desc
limit 10
