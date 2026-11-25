# ADR: Selective runtime join filters go into uni's Parquet row filter

Date: 2026-11-25

## Context

- Row-level runtime join filters (`RowKeyFilter`, `row_join_filters`, #2389) drop probe rows in `ScanOp.narrow`, after uni has decoded every selected column for every row. At SF10, Parquet decode is 47–55% of engine CPU in star-join probes (TPC-DS q73, q12, q70), while DuckDB's `IN PRF(...)` dynamic filters drop rows during decode.
- uni (wvlet/uni#1190) adds `ParquetFilter.In(column, IntegerSet)`. With filter-first decoding (uni#1189), uni decodes each conjunct's column for the rows still passing and the other columns only for the rows passing all. `IntegerSet` is a trait, so a bitmap or Bloom filter passes through without a copy.
- Filter-first decode costs more than decoding everything when most rows pass. In the first SF10 A/B, store_sales date filters keeping 20–23% of the rows used 13–19% more CPU in the source (q47, q57, q70). `narrow` pauses filters that keep most rows; a row filter given to the source can't pause.

## Decision

- **`scan_join_row_filters` decides per scan when it opens** (`ScanOp.spec`). By then the hash join builds above have set their filters (morsel scans run `prepare` first). The plan doesn't change, so plans and golden files stay the same.
- **Estimate each filter's pass rate from the row-group statistics** of its column (`TableSource.partRanges`, falling back to the file's [min, max] in `columnRange`). Only the row groups that the filter's range doesn't skip count. In each one, the share of its [min, max] that the filter accepts (`RowKeyFilter.acceptedIn`: exact for bitmaps and ranges; for Bloom filters, build keys scaled to the overlap plus ~1% false positives) is weighted by the group's rows. This assumes values spread evenly within a row group. The whole column's range alone underestimates data clustered by the key: catalog_sales is clustered by date, so q20's one-month date filter came out at 1.7% over the file but 55% over the row groups its range leaves, and pushing it cost CPU. At SF10 the per-group estimates matched the observed rates (web_sales date 1.7%, store_sales date 20–23%).
- **All or nothing per scan.** The filters with an estimated rate of at most 0.5 go in together, in ascending order, and only when the product of their rates is at most 0.05 (`ScanOp.MaxSourcePassRate`). The product is the share of rows whose other columns get decoded, and the break-even measured at SF10 lies between 0.05 and 0.2. Otherwise every filter stays in `narrow` and keeps its pausing.
- **Static conjuncts go first.** The query's own row-filter conjuncts (`scan_row_filters`) keep their place, and the join filters follow.
- **Only INT32, INT64, and DATE scan columns** that the source can row-filter exactly (`canRowFilterOn`). Timestamps and decimals stay in `narrow`, since uni's `In` compares raw values in another unit or fails the read.
- **`narrow` skips the filters the last spec gave the source.** The hash join still matches every key, so a filter applied twice, or in neither place, only changes how many rows go up. The scan never depends on it for correctness.

## Consequences

- SF10 with uni 2026.3.13, 16 TPC-DS star-join and window queries: 0.93x wall, 0.91x CPU (q73/q19/q34/q03 ~0.80, q68/q79 CPU 0.76–0.77). TPC-H SF10 (prototype): 0.99x wall, 1.00x CPU (q3/q12 CPU 0.91–0.93).
- A probe with few, cheap other columns gains little from skipping them, while uni's per-row `IntegerSet.contains` call costs more than the engine's inlined loop: TPC-DS q12 (web_sales, two integer key columns and one decimal) runs at the same wall time with ~11% more CPU (~11 ms).
- Filters keeping 5–50% of the rows still narrow after decode. Decoding them sooner would need a cheaper filter-first path in uni (e.g. one membership test per dictionary entry).
- Multi-file datasets (`ParquetDataset`) don't support row filters yet, so they keep `narrow`.
