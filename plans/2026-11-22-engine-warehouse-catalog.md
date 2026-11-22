# A persistent catalog for the wvlet engine

## Goals

- A `wvlet` connector with a `warehouse` folder keeps its tables across sessions, as a DuckDB database file does: `create table`, `drop table`, `alter table` / `reshape`, `create` / `drop schema`, and `save to t` / `append to t` / `append to t on k` / `delete` / `truncate` on table names write to the warehouse, and the next session reads them back.
- Warehouse tables are plain Apache Iceberg tables (Hadoop layout), so DuckDB, Spark, Trino, and pyiceberg can read them.
- Without `warehouse`, the engine stays in memory as today (DuckDB's `:memory:`). The browser always stays in memory.
- JVM, Scala Native, and Node.js.

## Background

- `EngineCatalog` maps names to `TableSource`s and is rebuilt for each session. `TableWriter` (#2217, #2225) runs table writes and DDL on in-memory `MemTable`s, keyed `t` / `s.t` relative to the session's catalog and default schema (`WvletEngineSession.tableKey`). `create table` keeps its defaults and NOT NULL / unique constraints in `TableWriter.specs`. All of it is lost at exit.
- `dataDir` / `tables` register files as read-only tables.
- #2220 writes Parquet / CSV / JSON files (`DataFileWriter`). The engine Iceberg work (session `engine-iceberg-read-write`, `plans/2026-11-22-engine-iceberg.md`) adds `save to '<dir>' with format: 'iceberg'` and `append to '<dir>'` through a new `catalog/IcebergWriter`, which converts engine schemas and batches to uni's Parquet writer.
- uni (before this work) creates Iceberg tables and appends snapshots. It can't overwrite, remove data files, change a schema, or drop a table, and its reader refuses renamed columns because it matches columns by name.
- The parser already produces `AlterTable` with `AddColumnOp`, `DropColumnOp`, `RenameColumnOp`, `AlterColumnSetDataTypeOp`, `AlterColumnDropNotNullOp`, `AlterColumnSetNotNullOp`, `Alter/DropDefault`, and `RenameTableOp`, from both SQL `alter table` and Wvlet `reshape t { add / rename / exclude }`. The engine rejects all of them today.

## Design

Lane: Standard. Always-gate: data storage (the engine writes and deletes files in a user folder).

### Configuration and layout

```yaml
connectors:
  - name: local
    type: wvlet
    properties:
      warehouse: ./warehouse   # relative to the work folder
```

- A managed table is a Hadoop Iceberg table at `<warehouse>/<schema>/<table>/` (`metadata/vN.metadata.json`, `metadata/version-hint.text`, `data/`). The default schema `main` is `<warehouse>/main/`.
- A schema is a sub-folder. `create schema s` creates the folder; `drop schema s` removes an empty one, and `with cascade: true` drops its tables first. The default schema can't be dropped (as today).
- The folder listing is the catalog: no catalog file, so there is nothing to keep in sync with the tables, and a table copied into the warehouse by another tool shows up.
- Names are kept as written; lookups ignore case, as today.
- A warehouse on a platform without a file system (the browser) is a configuration error, not silently in memory.

### Session start

- List `<warehouse>/*/*/` and register each folder with `metadata/*.metadata.json` as a managed table: a `ParquetSource` over the folder (uni reads Iceberg), and its compile-time columns from the current schema (metadata only, no data file is opened).
- `dataDir` / `tables` entries register as external read-only tables, as today. A name that is both a managed table and an external one is an error at session start, so a query never silently reads the other one.

### Writes

Every write ends in one uni commit, which creates `v{N+1}.metadata.json` only if it doesn't exist: a write is all or nothing, and two sessions writing the same table can't both win (the loser retries or fails with a conflict error).

| Statement | Iceberg commit |
|---|---|
| `create table t` | a new table with an empty snapshot |
| `create or replace table t` | an overwrite snapshot with the declared schema (history kept) |
| `save to t` | an overwrite snapshot; a result with other columns commits a new schema |
| `append to t` | an append snapshot (rows cast to the table types, as today) |
| `truncate t` | an overwrite snapshot without files |
| `delete from t where ...` | copy-on-write: only the data files with deleted rows are rewritten, in one overwrite snapshot that removes them |
| `append to t on k` | copy-on-write: the files with matching keys are rewritten with the updated rows, and new rows are added, in one snapshot |
| `drop table t` | the table folder is deleted (metadata first) |
| `alter table` / `reshape` | a metadata-only schema change (below) |

- Defaults, NOT NULL, and unique keys go into one table property (`wvlet.table-spec`, JSON), read back when a session first writes the table; constraints are checked on every write as today. Columns stay optional in Iceberg, so other engines see plain columns and alters never fight required fields.
- Writes to external tables stay read-only errors; `drop table` on one only unregisters it.

### `alter table`

Metadata-only Iceberg schema changes, on managed and in-memory tables alike:

- `add column c t` / `reshape { add c: t }`: an optional column, null in existing rows. A column with a default or `not null` rewrites the table with the column filled (Iceberg v2 can't fill existing rows from metadata).
- `drop column c` / `reshape { exclude c }`: refused for a partition column.
- `rename column a to b` / `reshape { rename a as b }`: keeps the field ID, so old files read under the new name.
- `alter column c type t` / `reshape { cast c as t }`: widening (int → long, decimal precision up) is metadata-only; other casts rewrite the table in one snapshot.
- `alter column c drop not null`, `set not null` (checked against the rows), and `set` / `drop default` update the spec property.
- `rename table` works on in-memory tables; on a warehouse table it is an error for now (uni would need to move the table location).
- `alter` on an external table is an error.

### uni changes (separate uni PR, needed first)

- Overwrite commits (`ParquetWriteMode.Overwrite` on Iceberg), with a new schema when the rows don't fit.
- `withReplacedFiles(paths)`: an overwrite commit that removes only those data files (copy-on-write), rewriting their manifests with EXISTING / DELETED entries; a removed file that is no longer live fails the commit.
- `Parquet.alterIcebergTable(root, changes*)`: add, drop, rename, widen, make optional; one new schema, no new snapshot.
- `Parquet.dropIcebergTable(root)`.
- Reads resolve columns by field ID, so renamed and re-added columns read correctly; the current snapshot reads with the current schema.
- `ParquetDataset.withFiles` for reading a table file by file during a copy-on-write rewrite.
- Table properties on create and in `alterIcebergTable` (for defaults and unique keys).

### Code layout and phasing

1. uni PR (above), then a uni release and a `UNI_VERSION` bump.
2. The engine Iceberg PR (#2224) merged first; this PR builds on its `IcebergWriter.write`.
3. This PR: `catalog/Warehouse` (listing, schema folders, commits) and `ManagedTable`, the `warehouse` connector property, and `TableWriter` dispatching each statement to an in-memory table or a warehouse commit. Copy-on-write `delete` / keyed append read the table file by file (`withFiles`) through an overlay catalog. `alter table` / `reshape` on both kinds of table. Pruning files by min/max statistics during copy-on-write is a follow-up.
4. Tests: JVM round trips that close the session, open a new one, and read the tables back, plus a DuckDB `iceberg_scan` read of each write; cross-platform cases in `PlanExecutorWvletEngineTest` (JVM, Node.js, Native).
5. Docs: a "Persistent tables" section in `usage/wvlet-engine.md`, and the `warehouse` property in `for-ai-agents.md`.

## Alternatives and Why Not?

- **A single database file, like DuckDB's.** A new file format nothing else reads, plus a page manager and WAL to write. Iceberg folders are readable by every major engine and uni already writes them.
- **A catalog file (JSON or SQLite) listing the tables.** A second source of truth that can disagree with the folders after a crash or a manual copy; the folder listing already names every table.
- **Plain Parquet files per table, replaced on write.** No atomic commit (a crash mid-write leaves a half table), no history, and copy-on-write would rewrite the whole table.
- **Merge-on-read deletes (position delete files).** Cheaper deletes, but uni's reader doesn't read delete files yet; copy-on-write keeps every table readable by uni and DuckDB alike.
- **An Iceberg REST catalog.** Needs a server; a later option for shared warehouses, with the same table store interface.
