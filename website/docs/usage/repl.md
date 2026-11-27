---
description: "Use the interactive Wvlet shell (wvlet, or its wv alias) to explore data and run flow-style queries against DuckDB, Trino, and other engines."
sidebar_label: Interactive Shell (REPL)
sidebar_position: 2
---

# Interactive Shell

Run `wvlet` with no arguments on a terminal (or `wvlet repl`) to launch an interactive query editor for the console. In the shell, you can write SQL queries, run them, and see the results interactively. The shell comes with every [distribution](install.md): npm and the JVM CLI, where `wv` is an alias of `wvlet repl`.

![wvlet shell](/img/demo.gif)

:::note
On the JVM CLI, line editing, history, and completion need Java 22 or later. On older JVMs, the shell reads input line by line without editing.
:::

The npm `wvlet` runs the same shell on the in-process wvlet engine, DuckDB, and Trino. Snowflake and other JDBC connectors, and flows, need the [JVM CLI](install.md#jvm-cli).

Ctrl-C during a query cancels only that query on every CLI, and tables saved earlier in the session stay available. On the npm `wvlet`, the query stops at its next batch (wvlet engine), at once (DuckDB), or at its next result page (Trino, which is also asked to abort it); if a query doesn't stop, a second Ctrl-C exits `wvlet`.

Queries run on the in-process [wvlet engine](../engines/wvlet-engine.md) by default. Start the shell with `wvlet repl -t duckdb` to run on DuckDB instead (needed for `sql"..."` / `execute` statements and `.duckdb` files), or with `wvlet repl -p <name>` to use a [profile](cli-reference.md#profile-configuration).

## Commands

By typing `help` in the shell, you can see the list of available commands:

```bash
$ wvlet
wv> help
[commands]
 help       : Show this help message
 quit/exit  : Exit the REPL
 clear      : Clear the screen
 context    : Show current database context (catalog and schema)
 clip       : Clip the last query and result to the clipboard
 clip-result: Clip the last result to the clipboard in TSV format
 rows       : Set the maximum number of query result rows to display (default: 40)
 col-width  : Set the maximum column width to display (default: 150)
 git        : Run a git command in the shell
 gh         : Run a GitHub command in the shell
```

## Shortcut Keys

Wvlet shell basically uses [GNU readline](https://readline.kablamo.org/emacs.html)-style shortcut keys. Here is the list of shortcut keys that are most frequently used in the shell: 

### Navigation

| Keys | Description                                                                      | 
|---|----------------------------------------------------------------------------------|
| ctrl-a | Home: Move to the beginning of the line                                          |
| ctrl-e | End: Move to the end of the line                                                 |
| ctrl-j ctrl-a | Move to the beginning of the query                                          |
| ctrl-j ctrl-e | Move to the end of the query                                                |
| ctrl-p | Up                                                                               | 
| ctrl-n | Down                                                                             |
| ctrl-f | Right                                                                            |
| ctrl-b | Left                                                                             |
| ctrl-r | Search the history. Type ctrl+r again to continue to search the previous history | 
| ctrl-k | Delete until the end of line from the cursor position                            | 
| tab | Complete a file path inside a quoted string, e.g., `from 'data/<tab>`            |

### Running Queries

| Keys | Description |
|---|---|
| ctrl-j ctrl-r | Run the whole query | 
| ctrl-j ctrl-t | Test run the query fragment up to the line with debug mode, showing the result above the query |
| ctrl-j ctrl-d | Describe the schema of the current line of the query fragment |
| ctrl-c | Cancel the current query, or exit the shell |
| ctrl-d | Exit the shell (on an empty line) |

## Switching Database Context

You can switch the database schema context in the REPL using the `use` statement:

```sql
-- Simplified syntax (recommended for common case)
wv> use analytics

-- Explicit syntax (optional)
wv> use schema analytics

-- Switch to a schema in a different catalog
wv> use production.analytics

-- Show current context
wv> context
Current context: catalog=memory, schema=analytics
```

The context switch affects all subsequent queries in the REPL session. The simplified `use <schema_name>` syntax is recommended for the common case of switching schemas within the same catalog.

