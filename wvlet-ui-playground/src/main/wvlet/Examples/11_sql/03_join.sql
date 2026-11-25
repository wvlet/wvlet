-- SQL: joins
select n.n_name, r.r_name
from nation n
join region r on n.n_regionkey = r.r_regionkey
where r.r_name = 'ASIA'
order by n.n_name
