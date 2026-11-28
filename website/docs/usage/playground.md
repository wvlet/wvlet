---
sidebar_label: Playground
---

# Playground

The [Wvlet Playground](https://wvlet.org/wvlet/playground/) runs Wvlet queries in your browser, with no installation and no server. Queries run on Wvlet's own columnar [engine](../engines/wvlet-engine.md), compiled to JavaScript, over TPC-H tables generated in the page.

## Notebook

The middle of the page is a notebook, like Jupyter's: **text cells** (markdown) and **query cells**, each query with its result table right below it.

- A query cell runs after you stop typing, or with Cmd/Ctrl + Enter (the statement at the cursor); `[n]` beside it counts its runs. **Run all** in the header runs every cell from the top.
- A query cell sees the `val`s and models of the cells above it, so a notebook can build a query step by step.
- **+ Query** and **+ Text** add a cell; hover a cell to add a cell after it or delete it. Double-click a text cell to edit its markdown (headings, **bold**, `code`, links, and lists); Esc or Cmd/Ctrl + Enter finishes.
- The right panel (**SQL**, **Plan**, **Engine**, **Lineage**, **Debug**, **Docs**) follows the focused cell; ◨ in the header folds it away to give the notebook the full width.
- An example opens as a notebook of its title and description and its query; a function opens as a notebook of its docs and one cell per example.
- In a file, a notebook is a `.wv` file whose cells are separated by `-- %%` lines (`-- %% [markdown]` for a text cell), the convention of jupytext; since these are comments, the file compiles as is. **Share** copies a link to the whole notebook.

## What it shows

- **Examples by category**: the left panel lists runnable examples of every major operator and function (basics, column operations, aggregation, joins, set operations, subqueries and models, window functions, expressions, functions, TPC-H queries, dbt-style models, testing and debugging). Search filters them by title, description, or a word in the query. Use it as a syntax and function reference.
- **Docs** (right panel): every [standard library function](../syntax/stdlib-reference.md) by category (text, pattern matching, numeric, date and time, aggregate, window, array, ...), with a search box. Pick a function to open it as a notebook: its signature and description, links to the bundled examples that call it, and one query cell per example with its result computed in your browser.
- **Editor help**: hover a function in the editor to see its signature, description, and example, or a column to see its type; type `.` after a value to list the functions you can call on it, and press Ctrl + Space for keywords and columns.
- **Tables**: the TPC-H tables and their column types; click a table to read it. Choose the scale factor (0.001 to 0.1) in the header.
- **SQL**: the SQL that Wvlet generates for the query, in the DuckDB, Trino, Snowflake, Hive, or BigQuery dialect.
- **SQL to Wvlet**: switch the editor to **SQL** to write SQL and see the equivalent Wvlet query. Switching converts the editor text between the two languages when it can.
- **Plan**: the compiler's logical plan in data-flow order. Click a node's line number to select its source in its cell; **Columns** shows each node's output columns.
- **Engine**: how the engine plans the query, in three stages: **Bound** (the runtime plan before optimization), **Optimized** (after the logical passes), and **Operators** (the physical operators that run). It lists the optimizer rules that changed the plan, and **Optimizations** turns individual optimizations off, to see how the plan and the run time change.
- **Lineage**: how the file's source tables and seeds (`val` tables) flow into its models and queries, as a top-down graph. Hover a node to highlight what it reads and what reads it; click it to select its definition. Materialized models have an outlined box, and **wvlet build order** lists the order [`wvlet build`](../syntax/model-pipelines.md) persists them in. Try it with the **dbt-style Models** examples.
- **Debug**: the rows of every `debug { ... }` block in the file, in source order. The editor marks each debug line with ◆, and opening a file with debug blocks shows this tab. Click a block's line to select it in the editor.
- **Result** (below each query cell): the row count and run time, then the rows in a table you can sort by clicking a column header (each header shows the column's name and type). It shows the first 10 rows; **+N more rows** shows more, and a wide result scrolls sideways. **Copy** copies the rows as tab-separated text, and ▾ in the gutter folds the result. Doubles are rounded to 6 decimal places.
- **Tests** (below each query cell): the outcome of the cell's [`test`](../syntax/test-syntax.md) assertions, the way `wvlet test` reports them; a failed test shows the actual and the expected value. The editor marks each test line with ✓ or ✗, and the status bar says how many failed. Try it with the **Testing & Debugging** examples.

## Keys

| Key | Action |
|-----|--------|
| Cmd/Ctrl + Enter | Run the statement at the cursor in the cell |
| Shift + Enter | Describe the columns of the query at the cursor |
| Ctrl + Enter | Run the innermost subquery at the cursor |

Edits run automatically after you stop typing. **Share** copies a link that opens the current example or query.

## Limitations

- The generated TPC-H tables follow the TPC-H specification's row counts, keys, and value domains, but not the reference `dbgen` values, so query answers differ from the official ones.
- Only what the Wvlet engine supports runs; `sql"..."` blocks and DuckDB-only functions don't run in the playground.
