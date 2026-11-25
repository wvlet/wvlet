-- SQL: group by and having
select o_orderpriority, count(*) as cnt, sum(o_totalprice) as total
from orders
group by o_orderpriority
having count(*) > 100
order by o_orderpriority
