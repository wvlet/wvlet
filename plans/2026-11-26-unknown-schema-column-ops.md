# Column operators on a table with no known schema

## Goals

- `rename`, `exclude`, and `shift` must never silently produce wrong SQL when the compiler does not know the input columns (e.g. `wvlet compile` with no catalog).
- The introduction's example (`from lineitem add … rename … exclude … shift … limit 5`) should compile to SQL that returns all columns, as it does when the schema is known.

## Background

- These operators expand to an explicit column list computed from the input's relation type. An unknown table has an `UnresolvedRelationType` with no fields, so the list is empty.
- An empty select list prints as `select *`, so a lone `rename` or `exclude` is a silent no-op today (`exclude l_comment` keeps `l_comment`).
- `add` builds a projected type whose fields are only the added columns, so a later `exclude` sees just `[revenue]` and emits `select revenue`. The projected type does keep the unknown table as its base, but so does an explicit `select a, b`, whose column list is complete, so walking the base type cannot tell the two apart.
- The Wvlet engine computes these schemas itself from real tables (`Planner.renameSchema` etc.), so it is unaffected; the bug is in the SQL path.
- DuckDB (the default `wvlet compile` target) and Snowflake support star modifiers (`* exclude (…)`, `* rename (… as …)`). BigQuery has `* except (…)` but no rename. Trino, Hive, `generic`, and the other targets have neither.

## Design

Lane: Standard — it changes the SQL users see for these operators and adds a new compile error for some targets.

- Track "the column list is incomplete" in the relation type: `add`/`prepend`/`transform`/`rename`/`exclude`/`shift` over an unknown input keep the known columns but stay marked as open, instead of looking fully known.
- Mark open types explicitly. A column-preserving operator over an open input produces an open type; `select`, `group by`, and similar operators close it.
- Record star-modifier support as capability flags on `DBType`: exclude for DuckDB, InMemory, Snowflake, and BigQuery (as `except`), and rename for DuckDB, InMemory, and Snowflake.
- Let the star select item carry its exclude and rename lists, so SqlGenerator prints them in each dialect's syntax.
- When the input is open and the target supports the modifier, emit it:
  - `exclude a` → `select * exclude (a)`
  - `rename a as b` → `select * rename (a as b)`
  - `shift a, b` → `select a, b, * exclude (a, b)` (`shift to right` puts the star first)
- When the input is open and the target lacks the modifier (Trino, Hive, `generic`, …), fail with a clear error naming the operator and the table whose columns are unknown, suggesting a schema or an explicit `select`.
- Known schemas keep today's explicit column lists, so existing output and specs do not change.
- Out of scope: parsing `* exclude` / `* rename` in SQL input.
- Tests: SQL snapshot specs for each operator over an unknown table per dialect, plus the introduction's chained query.

## Alternatives and Why Not?

- **Always error when columns are unknown.** Simplest, but breaks `wvlet compile` for the common no-catalog DuckDB case that the docs show.
- **Ignore unknown columns (`select *` passthrough), maybe with a warning.** Keeps `exclude`/`rename` wrong in the generated SQL, which is the bug.
