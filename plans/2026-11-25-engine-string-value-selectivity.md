# Engine: string selectivity from sampled Parquet values

Work item: "Engine: per-column string selectivity from Parquet dictionaries for scan row filters and join estimates".

## Problem

- String `=`, IN and LIKE use 1 / ndv (join search, only for files up to 1M rows) or the default 0.3 (scan row-filter gate). Selective string filters stay out of `selective_scan_row_filters` (#2415), and #2429's column-implied row filters let unselective `customer_demographics` filters through (26% kept, estimated 9%), so it was closed.

## Approach

- `TableSource.valueCounts(column)`: a `ValueCounts` sample (distinct strings, row counts, NULLs) of a single Parquet file, from up to 4 row groups spread over the file, up to 250K rows, counted per dictionary code. None above 4096 distinct values. Cached per file version with the distinct-count cache (#2426).
- `ScanFilters.valueSelectivity`: a condition over one string column (the same shapes `dictionary_filters` evaluates per entry) runs once per sampled value; the kept fraction is the estimate. Used by the row-filter gate and conjunct order (`ScanFilters.selectivity`) and the join search (`Stats.selectivity`), toggle `string_value_selectivity`.
- Re-apply #2429's `column_implied_row_filters`, now gated by the sampled estimates.

## Validation

- `EngineScanRowFiltersTest` cases for the sample, the gate, and implied filters; same rows with each toggle off.
- SF10 A/B per toggle on TPC-H q19, q3, q12, q16 and TPC-DS q91, q85, q13, q48, q34, q73, plus golden-changed queries.
