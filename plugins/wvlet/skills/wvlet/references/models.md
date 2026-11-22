# Models, definitions, and writes

## Models

A `model` is a named, reusable query. Reference it like a table.

```wvlet run
val orders(id, status, amount) = [[1, 'paid', 120], [2, 'paid', 40], [3, 'open', 300]]

---
Paid orders at or above a threshold.
---
model paid_orders(min_amount: int) = {
  from orders
  where status = 'paid'
  where amount >= min_amount
}

from paid_orders(100)
select id
test _.rows should be [[1]]
```

A model without parameters is written `model m = { ... }` and read with `from m`.

## Materialized models and `wvlet build`

A folder of `.wv` files works like a dbt project. `with { materialized: ... }` marks a model
for `wvlet build`, which builds models in dependency order inferred from references.

```wvlet
model stg_orders with { materialized: 'view' } = {
  from raw.orders
  where status != 'cancelled'
}

model customer_totals with { materialized: 'table', schema: 'marts' } = {
  from stg_orders
  group by customer_id
  agg amount.sum as total
}
```

- `materialized:` is `ephemeral` (default, inlined), `view`, or `table`. Other keys: `schema`,
  `alias`, `engine`, `retries`, `timeout`.
- `wvlet build` builds all; `-s m` one model, `-s m+` downstream, `-s +m` upstream;
  `--dry-run` prints the order and SQL.
- Outside `wvlet build` (in `wvlet run`, the REPL, tests) models are inlined, so a query never
  reads an unbuilt or stale table.
- A dependency cycle among materialized models is an error.

## Functions and query fragments

```wvlet run
def double(x: int): int = x * 2
def is_adult = where age >= 18

val people(name, age) = [['ann', 30], ['bob', 12]]

from people
is_adult
add double(age) as twice
select name, twice
test _.rows should be [['ann', 60]]
```

## Table declarations

A `table` declaration gives a stored table a static schema, so queries type-check without a
database connection. Generate them from a live database with `wvlet catalog import`.

```wvlet
table orders in mydb.sales = {
  order_id: bigint
  customer_id: bigint
  status: string
  amount: double
}

from sales.orders
where status = 'paid'
```

`type point = { x: long, y: long }` declares a structural type; `trait t extends string = { def ... }`
attaches methods to a type; `table b like a` copies a shape.

## Loops

A `for` loop repeats statements once per value. Use it for per-date or per-tenant tables, not
for transforming rows.

```wvlet
for d in ['2026_09_01', '2026_09_02'] {
  from s`events_${d}`
  group by user_id
  agg _.count as cnt
  add d as dt
  append to daily_summary
}

for t in (from tenants where active select tenant_id) {
  from s`orders_${t}`
  agg _.count as orders
  add t as tenant_id
  append to order_counts
}
```

## Imports

`import lib.models` brings models and definitions from another module or folder into scope.

## Tables and writes

Writes have a flow form (ending a query) and a block form (leading with the effect):

```wvlet
save to cleansed_orders {
  from orders
  where amount > 0
}

save to calendar if not exists {   -- seed once
  from date_dimension_source
}

append to metrics on date, metric_name {   -- insert or update by key (MERGE)
  from daily_metrics
}

from users
where active = true
save as view active_users

from users
where last_login < current_date() - '1 year':interval
update status = 'dormant', updated_at = now()
```

Table management:

```wvlet
create table users if not exists   -- columns come from a `table users = {...}` declaration
create or replace table users
drop table users if exists
truncate users
rename table users to customers
reshape users {
  add email: string
  rename name as full_name
  exclude age
  cast user_id as long
}
create schema staging if not exists
drop view active_users if exists
use 'archive.duckdb' as archive    -- attach an external database (DuckDB)
```

`save to t` creates or replaces `t`; `append to t` creates `t` if missing (with the declared
shape when a `table` declaration is in scope).
