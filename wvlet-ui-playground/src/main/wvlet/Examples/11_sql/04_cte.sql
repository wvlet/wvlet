-- SQL: common table expressions
with big_orders as (
  select o_custkey, o_totalprice
  from orders
  where o_totalprice > 300000
)
select o_custkey, count(*) as cnt
from big_orders
group by o_custkey
order by cnt desc, o_custkey
limit 10
