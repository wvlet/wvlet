-- EXISTS and NOT EXISTS as select-item expressions
select
  a,
  exists (select 1 from (values (1), (3)) as r(c) where r.c = l.a) as e,
  not exists (select 1 from (values (1), (3)) as r(c) where r.c = l.a) as ne
from (values (1, 10), (2, null), (null, 30)) as l(a, b)
order by a nulls last;
