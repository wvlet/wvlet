# ADR: How the wvlet engine recognizes and writes Iceberg targets

Date: 2026-11-22 · PR #2224

## Context

The engine already read Iceberg tables through uni's `Parquet.dataset` (a folder with `metadata/*.metadata.json`, or a `.metadata.json` path). uni 2026.3.1 added an Iceberg dataset writer: `ErrorIfExists` creates a table, `Append` commits a snapshot, and `Overwrite` is refused. It commits only Hadoop-style tables (`metadata/vN.metadata.json`); a table that a catalog owns (`00000-<uuid>.metadata.json`, as pyiceberg or a REST catalog writes) is refused. The reader refuses delete files, renamed columns, ORC data files, and format v3. File writes (`save to` / `append to '<file>'`) pick their format by extension, and `append to` takes no `with` options.

## Decision

- An Iceberg write target is a folder named explicitly: `save to '<dir>' with format: 'iceberg'` creates it, and `append to '<dir>'` commits a snapshot only when `<dir>` already holds an Iceberg table. An extension-less path with no format is an error that names `format: 'iceberg'`. Treating any folder as Iceberg was rejected: a typo'd path or a Hive-partitioned Parquet folder would silently become a table.
- `save to` on an existing Iceberg table, or on a non-empty folder, fails. The engine never deletes table files to emulate an overwrite, since that would lose the snapshot history; it waits for uni's Iceberg overwrite.
- `append to` a folder that is not a table fails, rather than letting uni's append create one (or write plain Parquet). `append to` a `.metadata.json` path fails, so a metadata file is never appended to as JSON lines.
- `IcebergWriter` (in `wvlet-engine` `catalog/`) takes a table location, an engine schema, and batches, with create and append entry points that don't depend on a plan node, so a future engine catalog can store managed tables through it. Rows appended map to table columns by position and are cast to their types, as for Parquet files.
- uni's read refusals surface as `NOT_IMPLEMENTED` errors naming the table.

## Consequences

- Iceberg tables can be created, appended to, and read on JVM, Node.js, and Native with no Spark or DuckDB.
- Gaps that need uni: overwriting an Iceberg table, reading delete files (and writing row-level deletes / copy-on-write), commits that change the schema (uni's append requires the table's column types), and committing to catalog-owned tables.
