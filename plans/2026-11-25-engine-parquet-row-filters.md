# Hand exact scan conjuncts to uni's Parquet row filter

## Goals

- Selective Parquet scans (TPC-H q19, q12, q6, q14; TPC-DS fact scans with date filters) decode every selected column for every row, then the engine's `FilterOp` throws most rows away. uni 2026.3.12 (wvlet/uni#1189) can decode the filter columns first and the other columns only for matching rows, when given the predicate with `ParquetReadOptions.withFilter`.
- Outcome: the engine passes the conjuncts uni evaluates exactly to `withFilter`, drops them from its own filter, and the A/B on SF10 shows lower time and allocation on q19/q12/q6/q14 without slowing q1.

## Background

- Today `PushScanFilters` attaches an *implied* `ParquetFilter` (conjuncts it can't express are dropped) to the runtime plan's `Scan`, used only as `withRowGroupFilter`; the `Filter` above still evaluates everything.
- A row filter must be *exact*, not implied: every row uni drops must be one the engine would drop. AND / OR of translatable comparisons are exact (no NOT is produced, so NULL-as-false matches the engine's filter semantics).
- uni's `withFilter` fails at read start for columns or literals it can't compare: DECIMAL, floats, lists, column-to-column, unsupported physical/logical types. `ScanFilters` already excludes decimals and floats; the column check needs the Parquet field type too.
- uni evaluates top-level AND conjuncts in the given order: the first conjunct's columns decode for every row, later ones only for survivors. So order matters.
- uni's `readRowGroup` (the morsel path) also applies the filter; `ParquetDataset` (folders, Iceberg) applies it after decode, which is correct but not faster.
- The runtime plan's `Filter` over `Scan` feeds row estimates (join ordering, build sides) and `PushScanFilters` is re-run after CTE pushdowns, so removing conjuncts from the plan itself would change estimates and lose filters on re-runs.

## Design

Lane: Standard (engine behaviour change behind a toggle), direction transcribed from the work item.

- New toggle `Optimization.ScanRowFilters` (`scan_row_filters`). Off reproduces today's plans exactly.
- The split happens in `Lowering` (physical), not in the runtime plan: a `Filter` directly over a `Scan` whose source supports row filters lowers to a `ScanOp` carrying a `rowFilter` and a `FilterOp` with only the remaining conjuncts (none when all moved). Plan estimates and the re-runnable pass are untouched.
- Exact translation is a new `ScanFilters` entry next to `toParquet`: a conjunct moves only when all of it translates (comparison with a same-typed literal for integers, dates, timestamps, booleans; string equality; IS [NOT] NULL; IN lists up to 64; AND/OR of those).
- `ParquetSource.canRowFilterOn(column)` limits row filters to top-level primitive columns whose Parquet type uni compares the way the engine does (signed INT32/INT64, DATE, MILLIS/MICROS INT64 TIMESTAMP, BOOLEAN, String/Enum/plain BYTE_ARRAY). NANOS timestamps stay out: the engine floors them to micros while uni compares the raw nanos. Anything else stays in the engine.
- Conjunct order: most selective first, estimated per column: integer/date ranges from a new `ParquetSource` column min/max lookup (footer statistics, no data read), `=`/IN only from an already cached distinct estimate (no column scan at lowering time), otherwise the default 0.3; ties put numeric columns before strings (cheaper to decode for every row).
- `ScanOp` carries `rowFilter` through every rebuild (`project` for physical column pruning, `withSchema`, `withFilter`) and the morsel split; columns only the moved conjuncts read are pruned from the scan, and uni reads them on its own.
- Single-file sources only, as the request scopes it: `ScanSpec` gets a `rowFilter`, which `ParquetSource` passes with `withFilter` (keeping the implied filter as `withRowGroupFilter`). Folders and Iceberg tables keep engine-side filtering until uni filters datasets before decode.
- The accelerator's CPU fallback lowering receives the same flag. Runtime join filters keep their current path (follow-up: `In` conjunct in uni).
- `explain` shows `row_filter=` on the scan.
- Tests: translation exactness (what moves, what stays), results equal with the toggle on and off over Parquet files with mixed types, through column pruning and the morsel path.
- A/B with `WVLET_ENGINE_BENCH_AB=scan_row_filters` on TPC-H SF10 q19/q12/q6/q14 plus q1; default on only if it wins without regressions, then regenerate golden plans.

## Alternatives and Why Not?

- **Move conjuncts in the runtime plan (`PushScanFilters`).** Changes row estimates and drops row filters when the pass re-runs over CTE bodies.
- **Pass the whole condition and let uni reject it.** uni fails the read instead of falling back, so the engine must decide per conjunct before the read.
- **Keep the engine's cheapest-first order.** Cheapest-first saves CPU in the engine's filter, but in uni the first conjunct decides what decodes for every row, so selectivity matters more.
