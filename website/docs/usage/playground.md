---
sidebar_label: Playground
---

# Playground

The [Wvlet Playground](https://wvlet.org/wvlet/playground/) runs Wvlet queries in your browser, with no installation and no server. Queries run on Wvlet's own columnar [engine](../engines/wvlet-engine.md), compiled to JavaScript, over TPC-H tables generated in the page.

## What it shows

- **Examples by category**: the left panel lists runnable examples of every major operator and function (basics, column operations, aggregation, joins, set operations, subqueries and models, window functions, expressions, functions, TPC-H queries). Search filters them by title, description, or a word in the query. Use it as a syntax and function reference.
- **Tables**: the TPC-H tables and their column types; click a table to read it. Choose the scale factor (0.001 to 0.1) in the header.
- **SQL**: the SQL that Wvlet generates for the query, in the DuckDB, Trino, Snowflake, Hive, or BigQuery dialect.
- **SQL to Wvlet**: switch the editor to **SQL** to write SQL and see the equivalent Wvlet query. Switching converts the editor text between the two languages when it can.
- **Plan**: the compiler's logical plan in data-flow order. Click a node's line number to select its source in the editor; **Columns** shows each node's output columns.
- **Engine**: how the engine plans the query, in three stages: **Bound** (the runtime plan before optimization), **Optimized** (after the logical passes), and **Operators** (the physical operators that run). It lists the optimizer rules that changed the plan, and **Optimizations** turns individual optimizations off, to see how the plan and the run time change.
- **Result**: the rows in a table you can sort by clicking a column header, with column types, row counts, and the run time. Doubles are rounded to 6 decimal places.

## Keys

| Key | Action |
|-----|--------|
| Cmd/Ctrl + Enter | Run the statement at the cursor |
| Shift + Enter | Describe the columns of the query at the cursor |
| Ctrl + Enter | Run the innermost subquery at the cursor |

Edits run automatically after you stop typing. **Share** copies a link that opens the current example or query.

## Limitations

- The generated TPC-H tables follow the TPC-H specification's row counts, keys, and value domains, but not the reference `dbgen` values, so query answers differ from the official ones.
- Only what the Wvlet engine supports runs; `sql"..."` blocks, DuckDB-only functions, and `test` assertions don't run in the playground.
