---
title: Wvlet for AI Agents
sidebar_label: For AI Agents
sidebar_position: 1.5
description: How AI coding agents should write Wvlet queries. When to choose Wvlet, a flow-style syntax cheat sheet, a SQL-to-Wvlet mapping table, how to run queries on DuckDB and Trino, and common mistakes to avoid.
keywords: [wvlet, ai agents, llm, sql, flow-style query, cheat sheet, duckdb, trino]
---

# Wvlet for AI Agents

Wvlet is a flow-style query language that compiles to SQL for DuckDB, Trino, Snowflake, and Hive. A query starts with `from` and lists one operator per line in the order the data is processed. That makes Wvlet **the query language agents edit well**: adding a filter, a column, or an aggregation is a one-line append, not a rewrite of nested SQL.

This page is written for AI coding agents and the people who prompt them. The whole documentation is also available as plain text at [llms.txt](https://wvlet.org/wvlet/llms.txt) (index) and [llms-full.txt](https://wvlet.org/wvlet/llms-full.txt) (all pages). Every user-facing doc page has a Markdown copy at the same URL with a `.md` suffix.

## When should I choose Wvlet?

Choose Wvlet when:

- You build a query step by step, or keep changing one: each step is a line you can add, remove, or reorder without touching the rest.
- You need the same logic on more than one engine. Wvlet generates engine-specific SQL for DuckDB, Trino, Snowflake, and Hive.
- You want reusable, testable query modules: `model` definitions, `val` constants, and `test` assertions live next to the query in `.wv` files.
- You explore local files (Parquet, CSV, JSON) with DuckDB, which is built in.

Plain SQL is fine when the project already standardizes on SQL files, a tool only accepts SQL, or the query is a single short `SELECT`. Wvlet can still help there: `wvlet compile` turns a `.wv` file into SQL you can hand to any tool.

## Why do flow-style queries suit agents?

- **Line-local edits.** Each line is one operator (`where`, `add`, `group by`, `agg`, `order by`, ...). A change touches the lines it is about, which keeps diffs small and reviews easy.
- **Append-only growth.** The output of each line feeds the next one, so you can extend a working query by appending a line and check the result after every step.
- **Reading order is execution order.** `from` comes first and `select` comes last (or not at all), so there is no need to jump between `SELECT`, `FROM`, `GROUP BY`, and `HAVING` to understand a query.

```wvlet
from orders
where status = 'paid'
group by customer_id
agg amount.sum as total
where total > 100        -- filter groups (SQL HAVING)
order by total desc
limit 10
```

## Syntax cheat sheet

Keywords are lower-case. Each operator takes the output of the previous line as its input.

| Operator | What it does | Example |
|----------|--------------|---------|
| `from` | Read a table, a file, or inline values | `from orders`, `from 'data.parquet'` |
| `where` | Filter rows; repeat it as often as needed | `where status = 'paid'` |
| `select` | Choose output columns (optional) | `select id, amount` |
| `select distinct` | Unique rows of the selected columns | `select distinct status` |
| `add` | Append a computed column | `add amount * 1.1 as with_tax` |
| `exclude` | Drop columns | `exclude status, amount` |
| `rename` | Rename a column | `rename amount as price` |
| `shift` | Move columns to the left | `shift amount` |
| `group by` | Group rows by keys | `group by customer_id` |
| `agg` | Add aggregations to the group keys | `agg _.count as n, amount.sum as total` |
| `where` after `agg` | Filter groups | `where total > 100` |
| `order by` | Sort rows | `order by total desc` |
| `limit` | Keep the first n rows | `limit 10` |
| `count` | Count all rows | `from orders count` |
| `dedup` | Remove duplicate rows | `select status dedup` |
| `sample` | Random sample | `sample 1000` |
| `join` / `left join` | Join another relation | `join customers on orders.customer_id = customers.id` |
| `concat` | Union all rows of another query | `concat { from archived_orders }` |
| `with` | Name a subquery | `with paid as { from orders where status = 'paid' }` |
| `model` | Define a reusable query | `model paid_orders = { from orders where status = 'paid' }` |
| `val` | Define a constant or an inline table | `val t(id, name) = [[1, 'a'], [2, 'b']]` |
| `test` | Assert on the result | `test _.size should be 10` |

Expressions:

- Strings use single or double quotes: `'paid'` and `"paid"` are both string literals. Quote identifiers with back quotes: `` `order id` ``.
- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`, `is null`, `is not null`, `in (...)`, `between ... and ...`, `like`. Logic: `and`, `or`, `not`.
- Aggregations: `_.count` counts rows in a group; `column.sum`, `.avg`, `.min`, `.max`, `.count_distinct` aggregate a column. SQL functions such as `count(*)` and `sum(x)` also work.
- Casts: `amount::int`, `'2026-01-01'::date`.
- Conditionals: `if x > 0 then 'pos' else 'neg'`, or `case when x > 0 then 'pos' else 'neg'` (no `end`).
- String interpolation: `s"order-${id}"`.
- Window functions: `row_number() over (partition by customer_id order by amount desc)`.
- Comments: `-- line comment`. `---` starts a Markdown doc comment block.

See the [Query Syntax reference](./syntax/index.md) for every operator.

## SQL to Wvlet mapping

| SQL | Wvlet |
|-----|-------|
| `SELECT * FROM t` | `from t` |
| `SELECT a, b FROM t WHERE c > 0` | `from t where c > 0 select a, b` |
| `SELECT *, x * 2 AS y FROM t` | `from t add x * 2 as y` |
| `SELECT DISTINCT a FROM t` | `from t select distinct a` |
| `SELECT COUNT(*) FROM t` | `from t count` |
| `SELECT k, COUNT(*) AS n FROM t GROUP BY k` | `from t group by k agg _.count as n` |
| `... GROUP BY k HAVING SUM(v) > 10` | `group by k agg v.sum as s where s > 10` |
| `SELECT COUNT(DISTINCT u) FROM t` | `from t agg u.count_distinct as users` |
| `... ORDER BY n DESC LIMIT 10` | `order by n desc limit 10` |
| `FROM a JOIN b ON a.id = b.a_id` | `from a join b on a.id = b.a_id` |
| `FROM a LEFT JOIN b ON ...` | `from a left join b on ...` |
| `q1 UNION ALL q2` | `q1 concat { q2 }` |
| `WITH x AS (q) SELECT ... FROM x` | `with x as { q } from x ...` |
| `CAST(x AS INT)` | `x::int` |
| `CASE WHEN c THEN a ELSE b END` | `if c then a else b` |
| `VALUES (1, 'a'), (2, 'b')` | `val t(id, name) = [[1, 'a'], [2, 'b']]` then `from t` |

To convert an existing SQL query, run `wvlet to_wvlet "<sql>"`. For example, `wvlet to_wvlet "select status, count(*) as n from orders group by status having count(*) > 1"` prints:

```wvlet
from orders
group by status
where count(*) > 1
select status, count(*) as n
```

## How do I run Wvlet?

Install the CLI with Homebrew (`brew install wvlet/wvlet/wvlet`) or download a release package; see [Installation](./usage/install.md). The CLI has two commands:

- `wv`: an interactive shell (REPL). `wv -c "from 'data.csv' count"` runs one query and exits.
- `wvlet`: batch commands.
  - `wvlet run "from 'data.parquet' limit 10"` runs a query; `wvlet run -f query.wv` runs a file.
  - `wvlet compile -f query.wv` prints the generated SQL; add `--target trino` (or another engine) to choose the SQL dialect.
  - `wvlet to_wvlet "<sql>"` converts SQL to Wvlet.

Engines:

- **DuckDB** is the default and needs no setup. File paths in `from '...'` are read by DuckDB. See [DuckDB](./usage/duckdb.md).
- **Trino**, **Snowflake**, and others are configured as profiles in `~/.wvlet/profiles.json` and selected with `--profile <name>`. See [Trino](./usage/trino.md) and [Connectors](./usage/connectors.md).
- To check a query without a database, run `wvlet compile`: syntax errors are reported with the line and column.

## Common mistakes agents make

| Mistake | Fix |
|---------|-----|
| Starting with `select ... from t` | Start with `from t`; put `select` at the end, or omit it. |
| `==` or `<>` for comparison | Use `=` and `!=`. |
| `HAVING` after `GROUP BY` | Use `where` after `agg`. |
| `UNION` / `UNION ALL` | Use `concat { ... }`. Wvlet has no duplicate-eliminating `union`; add `dedup` if needed. |
| `CASE ... END` | `case` has no `end`: `case when c then a else b as col`. Or use `if c then a else b`. |
| `// comment` | `//` is integer division. Use `-- comment`. |
| Double quotes for column names, as in SQL | `"x"` is a string literal. Use back quotes: `` `x` ``. |
| `_.count_distinct(col)`, `_.max_by(a, b)` | Call the function on the column: `col.count_distinct`, `a.max_by(b)` (or SQL `count(distinct col)`, `max_by(a, b)`). |
| Upper-case keywords (`FROM`, `WHERE`) | Keywords are lower-case: `from`, `where`. |
| Rewriting the whole query to add a step | Append a line: another `where`, `add`, or `agg` after the existing ones. |

## How do I keep queries correct?

- Put `test` assertions after a query in a `.wv` file, for example `test _.size should be 10` or `test _.columns should contain 'user_id'`. See [Test Syntax](./syntax/test-syntax.md).
- Compile before running against a remote engine: `wvlet compile -f query.wv --target trino`.
- Build long queries incrementally: run the first lines, check the output, then append the next operator.
