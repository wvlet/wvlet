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

## How do I give my agent the Wvlet skill?

The Wvlet repository ships an [Agent Skill](https://github.com/wvlet/wvlet/tree/main/plugins/wvlet/skills/wvlet) with the syntax, SQL mapping, test assertions, models, and CLI usage. It is released with the compiler, and CI compiles every example in it (and runs the ones with `test` assertions on DuckDB), so the syntax it teaches matches the current compiler.

In Claude Code:

```text
/plugin marketplace add wvlet/wvlet
/plugin install wvlet@wvlet
```

Every CLI (npm, Homebrew, native) also embeds the skill of its own version, so agents without plugin support, or offline, read the guide that matches the installed compiler:

- `wvlet skill` prints `SKILL.md`; `wvlet skill syntax` (or `models`, `testing`, `cli`) prints a reference; `wvlet skill files` lists the files.
- `wvlet skill install` installs the skill for Claude Code, Codex, and Pi. Choose agents with `--agent claude,codex,pi`, and install for every project with `--global`. Re-run it after upgrading wvlet to update the skill.

| Agent | Project install | `--global` install |
|-------|-----------------|--------------------|
| Claude Code (`claude`) | `.claude/skills/wvlet` | `$CLAUDE_CONFIG_DIR/skills/wvlet`, or `~/.claude/skills/wvlet` |
| Codex (`codex`), Pi (`pi`) | `.agents/skills/wvlet` | `~/.agents/skills/wvlet` |
| Any other agent | `--dir <path>` writes `<path>/wvlet` | |

`install` records what it wrote in `.wvlet-skill.json`, so it never overwrites a folder you created or a skill you edited (`--force` replaces them). `wvlet skill list` shows each install and whether it is current, and `wvlet skill remove --yes` (or `--dry-run`) removes only what wvlet installed. Add `--json` for machine-readable results.

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
| `add` | Add a computed column on the right; `add to left` puts it first | `add amount * 1.1 as with_tax`, `add to left id * 100 as key` |
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
| `model ... with { materialized: ... }` | Persist a model as a `table` or `view` with `wvlet build` | `model daily with { materialized: 'table' } = { from orders ... }` |
| `val` | Define a constant or an inline table | `val t(id, name) = [[1, 'a'], [2, 'b']]` |
| `test` | Assert on the result | `test _.size should be 10` |

`exclude`, `rename`, and `shift` on a table with no known schema (e.g. `wvlet compile` without a catalog) compile to `* exclude (...)` / `* rename (... as ...)` on DuckDB and Snowflake (BigQuery: `* except`, no rename); other SQL targets such as Trino fail at compile time, so give the table a schema or `select` the columns.

Expressions:

- Strings use single or double quotes: `'paid'` and `"paid"` are both string literals. Quote identifiers with back quotes: `` `order id` ``.
- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`, `is null`, `is not null`, `in (...)`, `between ... and ...`, `like`. Logic: `and`, `or`, `not`.
- Aggregations: `_.count` counts rows in a group; `column.sum`, `.avg`, `.min`, `.max`, `.count_distinct` aggregate a column. SQL functions such as `count(*)` and `sum(x)` also work.
- Casts: `amount::int`, `'2026-01-01'::date`.
- Conditionals: `if x > 0 then 'pos' else 'neg'`, or `case when x > 0 then 'pos' else 'neg'` (no `end`).
- String interpolation: `s"order-${id}"`. `'a' + x` concatenates when either side is a string. In both, non-strings are cast and NULL counts as `''` (unlike SQL `||`).
- Window functions: `row_number() over (partition by customer_id order by amount desc)`.
- Arrays: `xs.transform(x -> x * 2)`, `xs.filter(x -> x > 0)`, `xs.reduce((a, b) -> a + b)`, `xs.reduce(init, (acc, x) -> ...)`, `xs.exists(x -> x > 0)`, `xs.forall(x -> x > 0)`, and `xs.zip_with(ys, (x, y) -> x * y)` take a lambda, and chain like Scala collections. The lambda parameter has the array's element type, so `names.filter(s -> s.length > 3)` calls the string `length`.
- Comments: `-- line comment`. `---` starts a Markdown doc comment block.

See the [Query Syntax reference](./syntax/index.md) for every operator.

## SQL to Wvlet mapping

| SQL | Wvlet |
|-----|-------|
| `SELECT * FROM t` | `from t` |
| `SELECT a, b FROM t WHERE c > 0` | `from t where c > 0 select a, b` |
| `SELECT x AS y FROM t`, `SELECT x y FROM t` | `from t select x as y` or `from t select y = x` (no implicit alias; `as` is required) |
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
| `SELECT EXISTS (SELECT 1 FROM r WHERE r.k = t.k) AS e FROM t` | `from t select exists { from r where r.k = t.k } as e` |
| `VALUES (1, 'a'), (2, 'b')` | `val t(id, name) = [[1, 'a'], [2, 'b']]` then `from t` |

To convert an existing SQL query, run `wvlet to_wvlet "<sql>"`. For example, `wvlet to_wvlet "select status, count(*) as n from orders group by status having count(*) > 1"` prints:

```wvlet
from orders
group by status
where count(*) > 1
select status, count(*) as n
```

## How do I run Wvlet?

Install the CLI with npm (`npm install -g @wvlet/cli`, Node.js 22+). It provides `wvlet version`, `compile`, `to_wvlet`, and `run` (DuckDB included, no JDK needed). The JVM CLI, which adds the `wv` REPL, `wvlet build`, `flow`, `catalog`, and `ui`, is installed with Homebrew (`brew install wvlet/wvlet/wvlet`) or a release package; DuckDB (`-t duckdb`) and Snowflake on the JVM CLI need the full package `wvlet-cli-full-<version>.tar.gz`; see [Installation](./usage/install.md). The commands:

- `wv` (JVM CLI only): an interactive shell (REPL). `wv -c "from 'data.csv' count"` runs one query and exits.
- `wvlet`: batch commands.
  - `wvlet run "from 'data.parquet' limit 10"` runs a query; `wvlet run -f query.wv` runs a file.
  - `wvlet compile -f query.wv` prints the generated SQL; add `--target trino` (or another engine) to choose the SQL dialect.
  - `wvlet to_wvlet "<sql>"` converts SQL to Wvlet.
  - `wvlet build` (JVM CLI only) builds materialized models in dependency order; `wvlet build -s m+` builds `m` and its downstream models, and `--dry-run` prints the SQL. A dependency cycle among materialized models (including a model reading itself) is an error. See [Model Pipelines](./syntax/model-pipelines.md).

Engines:

- **wvlet** is the default engine of `wvlet run` and `wv` (no `-t` or `--profile` needed). It is the in-process columnar engine: it executes Wvlet plans directly on Parquet, JSON, CSV, and Iceberg files with no DuckDB, on every CLI including the npm and native ones. Register tables by name with a `wvlet` connector's `dataDir` / `tables` properties; `prepareTPCH: true` (with `tpchScaleFactor`, default 0.01) registers generated TPC-H tables, and `prepareTPCDS: true` (with `tpcdsScaleFactor`, default 0.01, up to 10) TPC-DS tables (one of the two per connector); with no setting, `from tpch.sf1.lineitem` or `tpcds.tiny.store_sales` reads the same generated tables (schema `tiny` is scale factor 0.01, `sf<n>` is n, as in Trino); their values differ from `dbgen` / `dsdgen`'s. `save to`, `append to`, and `delete` work on in-memory tables, and `save to` / `append to` write Parquet, CSV, and JSON files and Iceberg tables (`save to 'dir' with format: 'iceberg'` creates one or replaces its rows with a new snapshot, `append to 'dir'` adds a snapshot); `create table` / `drop table` / `truncate` / `create` and `drop schema` / `reshape t { add c: type; rename a as b; exclude c; cast c as type; set c not null; drop c not null; set c = value; drop c default }` / `rename table` / SQL `alter table` work too; views and `update` aren't supported yet, and flows and `wvlet build` run on it by default. Tables are in memory unless the connector sets a `warehouse` folder: then each table persists across sessions as an Iceberg table at `<warehouse>/<schema>/<table>`, with atomic commits, copy-on-write `delete` / keyed `append`, `rename table` (keeps the current snapshot only), and optional `keepSnapshots: N` to expire older snapshots and their files. With `--host`, `-t wvlet` connects to a remote wvlet server instead. Queries compiled for the engine may call only standard library functions and the functions the engine binds: a DuckDB-only function such as `array_unique(a)` fails at compile time with `FUNCTION_NOT_FOUND`, naming the standard library function to use when the name belongs to another engine (e.g. Trino's `approx_distinct` → `count_approx_distinct`); otherwise use `-t duckdb`. To declare keys of Parquet files for join planning, write a sidecar `<name>.keys.json` next to `<name>.parquet` (or `_keys.json` in a folder) with `primaryKey`, `uniqueKeys`, and `foreignKeys`; there is no DDL for it. See [Wvlet Engine](./usage/wvlet-engine.md).
- **DuckDB** (`-t duckdb`, `wv -t duckdb`, or a `"type": "duckdb"` profile) is needed for `sql"..."` / `execute` statements, `.duckdb` files, DuckDB extensions, and table functions such as `read_csv(...)`. `wvlet compile` emits DuckDB SQL by default. `wvlet build` and `wvlet flow` run on the wvlet engine by default (tables persist in the project's `target/warehouse`; `view` models need `-t duckdb`). See [DuckDB](./usage/duckdb.md).
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
