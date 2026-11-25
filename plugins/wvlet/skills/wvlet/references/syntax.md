# Wvlet syntax reference

Keywords are lower case. Each operator reads the output of the line above it. Snippets marked
`wvlet run` are executed on DuckDB in CI and their `test` lines must pass.

## Reading data

```wvlet
from orders                       -- a table in the current schema
from sales.orders                 -- schema.table
from 'data/orders.parquet'        -- a file (Parquet, CSV, JSON) read by DuckDB
from 'logs/*.json'                -- a glob
from sql"select * from orders"    -- raw SQL as a relation
```

Inline rows, with or without a named table constant:

```wvlet run
val products(id, name, price) = [
  [1, 'Laptop', 999.99],
  [2, 'Mouse', 29.99],
  [3, 'Keyboard', 79.99],
]

from products
where price > 50
select name
order by name
test _.rows should be [['Keyboard'], ['Laptop']]
```

Column types can be declared: `val users(id: int, name: string, active: boolean) = [...]`.

Catalog discovery: `show tables`, `show tables in sales`, `show schemas`, `show catalogs`,
`show models`, `describe orders`, `explain from orders`.

## Filtering and selection

```wvlet run
val t(id, status, amount) = [[1, 'paid', 10], [2, 'open', 20], [3, 'paid', null]]

from t
where status = 'paid'
where amount is not null         -- repeated where clauses are combined with and
select id, amount
test _.rows should be [[1, 10]]
```

- `select distinct a, b` keeps unique rows of the selected columns; `dedup` keeps unique rows.
- `limit n` keeps the first n rows.
- `sample 1000`, `sample 10%`, `sample reservoir (1000)`, `sample system (10%)`,
  `sample bernoulli (10%)`.

## Column operations

```wvlet run
val t(id, given, family, secret) = [[1, 'Ada', 'Lovelace', 'x']]

from t
add given + ' ' + family as full_name  -- append to the right
add to left id * 100 as key            -- insert on the left
exclude secret
rename given as given_name
shift full_name                        -- move to the left
test _.columns should be ['full_name', 'key', 'id', 'given_name', 'family']
```

`shift to right c` moves columns to the right end.

## Aggregation

```wvlet run
val sales(region, item, qty, price) = [
  ['us', 'a', 2, 10],
  ['us', 'b', 1, 50],
  ['eu', 'a', 5, 10],
]

from sales
add qty * price as revenue
group by region
agg
  _.count as n,
  revenue.sum as total,
  item.count_distinct as items,
  item.max_by(revenue) as top_item
order by region
test _.rows should be [['eu', 1, 50, 1, 'a'], ['us', 2, 70, 2, 'b']]
```

- `group by` keys come first in the output; `agg` appends the aggregations.
- After `group by`, `_1`, `_2`, ... refer to the grouping keys.
- `where` after `agg` filters groups (SQL `HAVING`).
- Other aggregations: `x.avg`, `x.min`, `x.max`, `x.to_array`, `x.min_by(y)`; any SQL
  aggregate function call such as `approx_distinct(x)` also works.
- `from t count` counts all rows; `group by k` without `agg` returns the distinct keys.

## Joins

```wvlet run
val orders(order_id, customer_id, amount) = [[1, 10, 5], [2, 20, 7], [3, 30, 9]]
val customers(id, name) = [[10, 'ann'], [20, 'bob']]

from orders
left join customers on orders.customer_id = customers.id
select order_id, name
order by order_id
test _.rows should be [[1, 'ann'], [2, 'bob'], [3, null]]
```

- `join` (or `inner join`), `left join`, `right join`, `full join`, `cross join t`.
- `join t on a.k = t.k and a.d = t.d`; tables can be aliased: `from orders as o join customers as c on o.customer_id = c.id`.
- `asof join prices on t.symbol = prices.symbol and t.time >= prices.time` matches the latest
  earlier row.
- Expand an array column with `cross join unnest(scores) as t(score)`.

## Combining queries

```wvlet run
val a(id) = [[1], [2], [3]]
val b(id) = [[2], [3], [4]]

from a
concat { from b }         -- union all
dedup
order by id
test _.size should be 4
```

```wvlet run
val a(id) = [[1], [2], [3]]
val b(id) = [[2], [3], [4]]

from a
intersect { from b }      -- also: except { ... }; add `all` for bag semantics
order by id
test _.rows should be [[2], [3]]
```

## Subqueries and named queries

```wvlet run
val orders(id, status) = [[1, 'paid'], [2, 'open']]

with paid as {
  from orders
  where status = 'paid'
}
from paid
test _.size should be 1
```

A `{ ... }` block is a subquery wherever a relation is expected: `from { from t where x > 0 }`.

## Sorting

`order by total desc, name asc nulls last`. `order by 1` sorts by the first column.

## Pivot and unpivot

```wvlet run
val sales(region, month, amount) = [
  ['us', 'jan', 1], ['us', 'feb', 2], ['eu', 'jan', 3],
]

from sales
pivot on month in ('jan', 'feb')
group by region
agg amount.sum
order by region
test _.columns should be ['region', 'jan', 'feb']
```

```wvlet run
val wide(id, jan, feb) = [[1, 10, 20]]

from wide
unpivot amount for month in (jan, feb)
test _.size should be 2
```

## Window functions

```wvlet run
val orders(customer_id, day, amount) = [['c1', 1, 10], ['c1', 2, 20], ['c2', 1, 5]]

from orders
add amount.sum() over (partition by customer_id order by day rows [,0]) as running
add row_number() over (partition by customer_id order by day desc) as rn
where rn = 1
select customer_id, running
order by customer_id
test _.rows should be [['c1', 30], ['c2', 5]]
```

Frames: `rows [,0]` (unbounded preceding to current, the default), `rows [-1,1]`, `rows [0,]`.

## Expressions

| Expression | Meaning |
|------------|---------|
| `'text'`, `"text"`, `"""multi-line"""` | String literals |
| `true`, `false`, `null` | Boolean and null literals |
| `` `order id` `` | Quoted identifier |
| `=`, `!=`, `<`, `<=`, `>`, `>=` | Comparison (`is`, `is not` also compare) |
| `and`, `or`, `not`, `!` | Logic |
| `x is null`, `x is not null` | Null checks |
| `x in (1, 2)`, `x not in (...)`, `x in { from t select id }` | Membership |
| `x between 1 and 10` | Inclusive range |
| `x like 'a%'`, `x not like ...`, `x like 'a\_%' escape '\'` | Pattern match |
| `exists { from t where ... }`, `not exists { ... }` | Subquery test; also a boolean select item (`select exists { ... } as e`) |
| `x::int`, `'2026-01-01'::date` | Cast |
| `'7 days':interval` | Interval literal |
| `+`, `-`, `*`, `/`, `//`, `%` | Arithmetic; `//` is integer division |
| `'a' + x`, `s"id-${x}"` | String concatenation; NULL counts as `''` |
| `` s`events_${d}` `` | Interpolated identifier, e.g. a table name |
| `[1, 2, 3]`, `arr[1]` | Array, 1-origin index |
| `{k: 1, s: 'v'}`, `obj.k` | Struct |
| `map {'a': 1}` | Map |
| `xs.transform(x -> x + 1)`, `xs.filter(x -> x > 0)`, `xs.reduce((a, b) -> a + b)` | Lambda over array elements; the parameter has the element type |
| `_` | The input relation, e.g. `_.count`, `_.size` in tests |
| `sql"..."` | Raw SQL expression; `${...}` interpolates |

```wvlet run
val t(score) = [[95], [85], [70], [null]]

from t
add
  if score >= 90 then 'A' else if score >= 80 then 'B' else 'C' as by_if
add
  case
    when score >= 90 then 'A'
    when score >= 80 then 'B'
    else 'C'
  as by_case
add s"score: ${score}" as label
where score is not null
order by score desc
test _.rows should contain [95, 'A', 'A', 'score: 95']
```

## Comments

- `-- line comment`
- `---` opens and closes a Markdown doc comment block, usually placed before a `model`.

## Writes

```wvlet
from orders
where day = '2026-11-01'
save to daily_orders              -- create or replace a table

from orders
save to 'out/orders.parquet'      -- write a file

from new_orders
append to orders                  -- insert; creates the table if missing

from new_orders
append to orders on order_id      -- insert or update by key (MERGE)

from orders
where status = 'test'
delete                            -- delete matching rows of the source table
```

More write forms (`save as view`, `update`, `create table`, `reshape`, `drop table`) are in
[models.md](models.md#tables-and-writes).

## Reserved keywords

These words cannot be bare column or table names. Quote them with back quotes
(`` `order` ``) or pick another name. The list is generated from the parser.

<!-- BEGIN GENERATED: reserved-keywords -->
`add`, `agg`, `all`, `and`, `append`, `as`, `asc`, `asof`, `be`, `between`, `by`, `case`, `contain`, `create`, `cross`, `debug`, `dedup`, `def`, `delete`, `desc`, `describe`, `distinct`, `drop`, `else`, `escape`, `except`, `exclude`, `execute`, `exists`, `explain`, `export`, `extends`, `false`, `first`, `for`, `from`, `full`, `group`, `having`, `if`, `import`, `in`, `inline`, `inner`, `intersect`, `is`, `join`, `last`, `left`, `let`, `like`, `limit`, `map`, `model`, `native`, `not`, `null`, `nulls`, `of`, `on`, `or`, `order`, `over`, `package`, `partition`, `pivot`, `rename`, `right`, `rows`, `run`, `sample`, `save`, `select`, `shift`, `should`, `show`, `test`, `then`, `this`, `to`, `true`, `truncate`, `type`, `unpivot`, `update`, `use`, `val`, `when`, `where`, `with`
<!-- END GENERATED: reserved-keywords -->
