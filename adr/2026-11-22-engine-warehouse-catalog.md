# ADR: The wvlet engine persists tables as Iceberg tables in a warehouse folder

Date: 2026-11-22 · PR #2232

## Context

The in-process engine ([ADR](2026-11-21-wvlet-engine-connector.md)) kept `create table` and `save to t` results in memory, so they were lost when the session ended. Users of the `wvlet` connector need tables that persist, the way a DuckDB database file keeps them, on JVM, Native, and Node.js. Design note: `plans/2026-11-22-engine-warehouse-catalog.md`.

## Decision

- **A `warehouse` connector property turns persistence on.** Without it the engine stays in memory, like DuckDB's `:memory:`. The browser never gets a warehouse.
- **Each table is a Hadoop Iceberg table at `<warehouse>/<schema>/<table>`, and each schema is a sub-folder.** A single database file was rejected: it would be a new format that only wvlet reads, with its own page manager and WAL. Iceberg folders are readable by DuckDB, Spark, Trino, and pyiceberg, and uni already writes them.
- **The folder listing is the catalog.** There is no catalog file that could disagree with the folders after a crash or a manual copy. A warehouse table whose name a `dataDir` / `tables` entry also registers is an error at session start, so a query never silently reads the wrong table.
- **Every write is one Iceberg commit.** `save to`, `truncate`, and `create or replace` commit overwrite snapshots, and `save to` may change the columns by committing a new schema. `append to` commits an append. `delete` and `append to t on k` are copy-on-write: only the data files with changed rows are rewritten, in one snapshot that removes them. Merge-on-read delete files were rejected because uni's reader doesn't read them yet; copy-on-write tables read everywhere. The uni commit creates `vN.metadata.json` only if it is absent, so a write is atomic, and of two concurrent writers only one wins each version.
- **`alter table` / `reshape` commit metadata only when Iceberg allows it.** That covers adding an optional column with no default, dropping, renaming, widening, defaults, and dropping NOT NULL. Other changes (a new column with a default or NOT NULL, a narrowing cast) rewrite the table in one overwrite snapshot. Renaming a warehouse table is refused until uni can move a table's recorded location.
- **Declared defaults and constraints live in a table property (`wvlet.table-spec`, JSON)**, not in Iceberg required fields, so writes and alters keep the same checks in every session, and other engines see plain optional columns.

## Consequences

- `create table`, `save to t`, and writes made in one `wvlet run` are read back by the next one, and other engines can read the tables.
- Overwrites and copy-on-write rewrites keep old data files for time travel. Nothing expires snapshots yet, so a warehouse grows until the tables are rewritten elsewhere or dropped.
- A dropped table stays in the session's compile-time catalog until the session ends (`InMemoryCatalog` has no drop), as for in-memory tables.
