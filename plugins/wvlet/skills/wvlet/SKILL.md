---
name: wvlet
description: Write, convert, test, and run Wvlet queries (.wv files), a flow-style query language that compiles to SQL for DuckDB, Trino, Snowflake, and Hive. Use when a project contains .wv files or a profiles.json for wvlet, when the user mentions Wvlet, wv, or flow-style queries, when converting SQL to Wvlet, when exploring Parquet/CSV/JSON files with a query, or when building dbt-like model pipelines with `wvlet build`.
---

# Wvlet

Wvlet is a flow-style query language. A query starts with `from` and lists one operator per
line in the order the data is processed; each line takes the output of the line above. The
compiler turns a `.wv` file into SQL for DuckDB (default dialect), Trino, Snowflake, or Hive, and
`wvlet run` executes it on the built-in wvlet engine by default.

Every `wvlet` snippet in this skill is checked by the Wvlet compiler in CI, so the syntax shown
here matches the compiler version this skill ships with.

## Workflow

1. Write the query one operator per line, starting with `from`.
2. Check it without a database: `wvlet compile -f query.wv` (or `wvlet compile "<query>"`).
   Errors name the line and column. Add `--target trino` for another SQL dialect.
3. Run it: `wvlet run -f query.wv`, or `wvlet run "from 'data.parquet' limit 10"`.
   On the default wvlet engine, a function it does not bind fails at compile time with
   `FUNCTION_NOT_FOUND`: use the standard library function the message names, or `-t duckdb`.
4. Grow it by appending lines and re-running; never rewrite a working query to add a step.
5. Pin the result with `test` assertions after the query (see
   [references/testing.md](references/testing.md)).

To convert SQL, run `wvlet to_wvlet "<sql>"` and edit the output.

## Query shape

```wvlet run
val orders(id, customer_id, status, amount) = [
  [1, 'c1', 'paid', 120],
  [2, 'c1', 'paid', 30],
  [3, 'c2', 'cancelled', 300],
  [4, 'c2', 'paid', 90],
]

from orders
where status = 'paid'
group by customer_id
agg amount.sum as total, _.count as n
where total > 100        -- filter groups (SQL HAVING)
order by total desc
limit 10
test _.rows should be [['c1', 150, 2]]
```

The usual order is `from` → `where` → `add` → `group by` → `agg` → `where` → `exclude` /
`shift` → `select` → `order by` → `limit`. `select` is optional, and every operator may repeat.

## Rules that differ from SQL

| Do not write | Write |
|--------------|-------|
| `select a from t` | `from t select a` (`from` first; `select` last or omitted) |
| `FROM`, `WHERE` (upper case) | `from`, `where`; keywords are lower case |
| `==`, `<>` | `=`, `!=` |
| `having ...` | `where ...` after `agg` |
| `union all` / `union` | `concat { ... }` (add `dedup` to drop duplicates) |
| `case when c then a else b end` | `case when c then a else b` (no `end`), or `if c then a else b` |
| a keyword as a column name (`first`, `last`, `desc`, `order`, `type`) | `` `first` `` (back quotes), or another name |
| `"col"` for an identifier | `` `col` ``; `"..."` and `'...'` are both string literals |
| `// comment`, `/* ... */` | `-- comment`; `//` is integer division |
| `count(distinct c)` inside `_.` | `c.count_distinct` (SQL `count(distinct c)` also works) |
| `cast(x as int)` | `x::int` (SQL `cast` also works) |
| `a || b` | `a + b` or `s"${a}${b}"`; NULL counts as `''` |
| `select *, x * 2 as y` | `add x * 2 as y` |
| `select x y` (implicit alias) | `select x as y` or `select y = x`; a bare name after a column list is a partial query call |

## Operators

| Operator | Example |
|----------|---------|
| `from` table / file / values | `from orders`, `from 'data.parquet'`, `from [[1, 'a'], [2, 'b']]` |
| `where` | `where status = 'paid'` |
| `select`, `select distinct` | `select id, amount`, `select distinct status` |
| `add`, `add to left` | `add amount * 1.1 as with_tax`, `add to left amount * 2 as doubled` |
| `exclude`, `rename`, `shift` | `exclude password`, `rename amount as price`, `shift id`, `shift to right note` |
| `group by` + `agg` | `group by k agg _.count as n, v.sum as total` |
| `count` | `from orders count` |
| `order by`, `limit` | `order by total desc, id limit 10` |
| `dedup`, `sample` | `dedup`, `sample 1000`, `sample 10%` |
| `join` (`left`, `right`, `full`, `cross`, `asof`) | `join customers on orders.customer_id = customers.id` |
| `concat`, `intersect`, `except` | `concat { from archived_orders }` |
| `pivot`, `unpivot` | `pivot on month group by k agg v.sum` |
| `with` | `with paid as { from orders where status = 'paid' } from paid` |
| `describe`, `show`, `explain` | `describe orders`, `show tables`, `explain from orders` |
| `save to`, `append to`, `delete`, `update` | `from t save to 'out.parquet'`, `append to log` |
| `test`, `debug` | `test _.size should be 3` |

`exclude`, `rename`, and `shift` on a table with no known schema (e.g. `wvlet compile` without a catalog) compile to `* exclude (...)` / `* rename (... as ...)` on DuckDB and Snowflake (BigQuery: `* except`, no rename); other SQL targets such as Trino fail at compile time, so give the table a schema or `select` the columns.

Each operator, its options, and expressions are in [references/syntax.md](references/syntax.md).

## Expressions in brief

- Aggregations: `_.count`, `x.sum`, `x.avg`, `x.min`, `x.max`, `x.count_distinct`,
  `x.max_by(y)`, `x.to_array`; SQL calls such as `sum(x)` and `count(*)` work too.
- Conditions: `in (...)`, `not in`, `between a and b`, `like`, `is null`, `is not null`,
  `and`, `or`, `not`, `exists { ... }`.
- Values: `[1, 2]` (1-origin index `a[1]`), `{k: 1}`, `map {'a': 1}`, `'2026-01-01'::date`,
  `'7 days':interval`, `s"id-${id}"`, `x -> x + 1`.
- Window: `row_number() over (partition by k order by ts desc)`, frames `rows [,0]`.
- Raw SQL escape hatch: `sql"..."` as an expression, `from sql"select ..."` as a relation.

## Reusable code

```wvlet
-- A constant and a reusable, parameterized model
val min_amount = 100

model big_orders(threshold: int) = {
  from orders
  where amount >= threshold
}

from big_orders(min_amount)
select id, amount
```

`model`, `def`, `table` declarations, `for` loops, `import`, writes, and `wvlet build`
pipelines are in [references/models.md](references/models.md).

## Running

| Command | Purpose |
|---------|---------|
| `wvlet compile -f q.wv [--target trino]` | Print the generated SQL; fastest syntax and type check |
| `wvlet run -f q.wv` / `wvlet run "<query>"` | Run on the wvlet engine, or on `-t duckdb` / `-p <profile>`; `--format csv` |
| `wvlet to_wvlet "<sql>"` | Convert SQL to Wvlet |
| `wvlet build [-s m+] [--dry-run]` | Build materialized models in dependency order |
| `wvlet skill [syntax\|models\|testing\|cli]` | Print this skill as shipped with the installed CLI |
| `wvlet repl` | Interactive REPL (`wv` is an alias on the JVM CLI); `wvlet repl -c "<query>"` runs one query |

Engines, profiles, flows, and installation are in [references/cli.md](references/cli.md).
