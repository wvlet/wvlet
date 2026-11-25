-- SQL: case and window functions
select
  o_orderkey,
  o_totalprice,
  case when o_totalprice > 200000 then 'large' else 'small' end as size,
  rank() over (partition by o_orderpriority order by o_totalprice desc) as rnk
from orders
where o_custkey = 1000
order by o_orderkey
