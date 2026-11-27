# ADR: Count partition pruning by join keys in join order, and decide partition-key row filters per file

Date: 2026-11-27 · PR #2517

## Context

- #2508 reads Hive-partitioned Parquet folders as their files, and a runtime join filter on a partition key skips the partitions holding no build key. The exact join search (`JoinEnumerator`) charged a probe the rows its join filters keep (`probe_scan_cost`), but not the rows a scan reads. A filter on a key spread over every part (store_sales' tickets) reads every part and drops rows after decoding, while a filter on a partition key reads only the matching parts.
- So TPC-DS q25 over date-partitioned store_sales built store_sales ⋈ (catalog_sales ⋈ store_returns) and probed it with d1's 30 dates. That read all 1823 partitions and was 1.64x slower than #2508's off arm, which planned without statistics.
- Partitioned folders also had no source row filter (`supportsRowFilter` was false), because each file lacks the partition columns that a row filter's conjuncts may test.

## Decision

- `partition_probe_scan_cost`: the search tracks, per input set, the share of each input's rows its scans read. A join whose build keys reach a probe scan column on the probe pipeline lowers that share to the build keys' share of the column's values, and the cost is credited with the rows no longer read. The column qualifies only when every part (Hive partition or row group) holds one value and the source skips parts by key sets (`Stats.partitionKey`). Only then is the share of values read equal to the share of rows read, without knowing the build keys at plan time.
- `partition_row_filters`: a partitioned folder read as its files takes an exact row filter. `ParquetFolder.decideKeys` decides the filter's partition-key leaves by each file's values (true, false, or the rest over file columns; a NULL value passes only `IS NULL`), skips the files it rejects, and gives each file the rest.

## Consequences

- q25 0.15x and q17 0.32x wall at SF10 on date partitions; the row filters gave 0.92–0.95x wall over the partition-scanning queries.
- Row groups with value ranges (plain single files) get no credit. Their pruning by sparse keys depends on which keys the build holds.
- Excluding partition keys from `canRowFilterOn` was rejected: #2501's key-set partition pruning reads the same check.
- `decideKeys` repeats the per-partition simplification that uni's `selectFiles` already does internally, since uni doesn't expose the per-file leftover filter.
