# Engine: show tables in the built-in tpch/tpcds catalogs

## Goals

- `show tables in tpch.tiny` on the default Wvlet engine lists the 8 TPC-H tables, and `show tables in tpcds.<schema>` lists the 24 TPC-DS tables, for any scale-factor schema (`tiny`, `sf<n>`).
- The quick start (`website/docs/syntax/quick-start.md`) can show `show tables in tpch.tiny` instead of listing the tables in prose.

## Background

- The engine runs `show tables` as the SQL that `SqlGenerator` emits: `select table_name as name from information_schema.tables where table_catalog = ... and table_schema = ... order by name`.
- `EngineCatalog.informationSchema` serves `information_schema.tables` with only the registered tables, under `memory.main`, so the `tpch`/`tpcds` filter matches nothing and returns 0 rows.
- The built-in catalogs (#2478) resolve any scale-factor schema on demand (`BenchmarkCatalogs.find`), so there is no finite list of schemas to put into `information_schema.tables`.

## Design

- Lane: Express — a bug fix with a clear diagnosis in the engine's SHOW lowering, no always-gate area.
- `WvletEngine.lowerShow` checks a `show tables` whose target (the `in` clause, else the session's catalog and schema) is a built-in benchmark catalog with a valid scale-factor schema, and lowers it to a sorted `VALUES` list of that catalog's table names, keeping the `name` column `show tables` already returns.
- `BenchmarkCatalogs` gets a `tableNames(catalog, schema)` helper next to `find`/`tableDef`; names come from the tiny generator, as `tableDef` already does for column types.
- Any other target keeps the current information_schema path.
- Tests: a `spec/benchmark-catalogs` case for `show tables in tpch.tiny` (8 rows) and `tpcds.sf1` (24 rows), plus a `BenchmarkCatalogsTest` case for the helper.
- Docs: restore `show tables in tpch.tiny` in the quick start.
- Out of scope: making `use tpch.tiny` resolve bare names (`from customer`); left as a follow-up on the work item.

## Alternatives and Why Not?

- Add benchmark rows to `information_schema.tables`: would need a fixed schema list (`tiny`, `sf1`, ...), so `sf5` would still show nothing.
- Change `SqlGenerator`'s SHOW SQL: it serves DuckDB/Trino too; the gap is engine-only.
