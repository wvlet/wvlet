# ADR: Exact scan conjuncts move into uni's Parquet row filter when lowering

Date: 2026-11-25 · PR #2404

## Context

- Selective Parquet scans decoded every selected column for every row, then the engine's `FilterOp` dropped most rows (TPC-H q19 keeps 21K of 60M lineitem rows at SF10).
- uni 2026.3.12 (wvlet/uni#1189) decodes the columns of a `withFilter` predicate first, conjunct by conjunct in the given order, and the other columns only for matching rows. Its row filter must be exact, and it fails the read for predicates it can't compare exactly (DECIMAL, floats, two columns, lists).
- The runtime plan's `Filter` over a `Scan` feeds row estimates (`Stats`, join ordering, build sides), and `PushScanFilters` recomputes the scan's implied row-group filter from that `Filter` each time it re-runs (after pushing more conjuncts into a shared CTE body).

## Decision

- **`scan_row_filters` splits the filter while lowering, not in the runtime plan.** A `Filter` directly over a `Scan` of a source that `supportsRowFilter` lowers to a `ScanOp` with a `rowFilter` and a `FilterOp` with the remaining conjuncts (none when all moved). Estimates and the re-runnable pass see the same plan as before.
- **Only exact conjuncts move** (`ScanFilters.exact`). A conjunct moves only when all of it translates: comparisons with a same-typed literal on integers, dates, timestamps and booleans, string equality, IS [NOT] NULL, IN lists up to 64 values, a boolean column, and AND/OR of those. There is no negation, so treating NULL as not matching agrees with the engine. The implied row-group filter stays, unlike `toParquet`, which drops what it can't express.
- **The source decides which columns are exact** (`canRowFilterOn`). For Parquet, these are top-level signed INT32/INT64, DATE, BOOLEAN, String/Enum/plain BYTE_ARRAY, and MILLIS/MICROS timestamps. NANOS timestamps are excluded: the engine floors them to micros, while uni compares the nanos, so `ts <= T` would disagree.
- **Single files only.** uni's `ParquetDataset` filters rows after decoding them, so folders and Iceberg tables would gain nothing over the engine's filter and lose its late gather.
- **Most selective conjunct first** (`ScanFilters.ordered`), because the first conjunct's columns decode for every row. Conjuncts over one column are estimated together, and they stay adjacent. Integer and date ranges are estimated from the footer min/max (`TableSource.columnRange`), and `=`/IN from the distinct estimate (not for strings, whose estimate may scan the column). Anything else counts 0.3. Ties put numeric columns before strings.
- **Only selective row filters move** (`selective_scan_row_filters`, added by PR #PRNUM). The exact conjuncts move only when `ScanFilters.estimatedSelectivity` keeps at most `MaxRowFilterSelectivity` (15%) of the rows; otherwise the whole filter stays in the engine's `FilterOp`. uni evaluates the row filter row by row in three-valued logic and gathers the other columns for the kept rows, which costs more than decoding every row when few drop: TPC-DS q09's five `ss_quantity between` scans (20% each) ran 1.68x slower at SF10, and TPC-H q1's `l_shipdate <=` (96%) 2.77x at SF1, while the 4–14% scans of TPC-H q4, q6, q12 ran faster.
- **A `count(*)` scan keeps a column the row filter reads** (`ColumnPruner`), not column 0, which uni would otherwise decode for every matching row.

## Consequences

- At SF10, TPC-H q6/q12/q19 use 10–25% less CPU, and the q1/q6/q12/q14/q19 suite runs 0.93x wall (PR #2404).
- DECIMAL, float, column-to-column, `!=` and LIKE conjuncts stay in the engine until uni's row filter supports them.
- Runtime join filters (`RowKeyFilter`) still narrow after decode. They can become row-filter conjuncts once uni has an `In(column, values)` conjunct.
