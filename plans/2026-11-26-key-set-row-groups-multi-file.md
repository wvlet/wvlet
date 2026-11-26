# Key-set row-group skipping for multi-file Parquet tables

## Goals

- Give folders of Parquet files (and Iceberg tables read through `ParquetSource`) the key-set row-group skipping that #2487 added for single files (TPC-DS q17, q25, q29).
- Check whether builds above 4096 keys, or a range-set summary, would skip row groups for q50 and q93.

## Background

- #2487 adds `In(column, prunedSet)` to a scan's row-group filter for per-row runtime filters with few keys, but only when the source supports a row filter. `ParquetSource` supports one for single files only, since uni's dataset reader tests row filters after decoding.
- uni's `ParquetDataset` does apply a row-group filter to each file's row groups, and evaluates `In` on a partition column with the set's `contains`. So the row-group part of #2487 already works for datasets once the engine sends it.

## Design

- Lane: Express. The work item carries the diagnosis and design, the change is internal to the engine behind the existing `key_set_row_groups` toggle, and it touches no always-gate area.
- Add a `TableSource.supportsKeySetRowGroups` capability (defaults to `supportsRowFilter`). `ParquetSource` returns true for folders and Iceberg tables too. `ScanOp` checks it instead of `supportsRowFilter` before adding the key-set conjuncts.
- Keep the `key_set_row_groups` toggle and add no new one: with it off, datasets behave as before, so a one-JVM A/B on a multi-file copy measures the change.
- Keep `MaxSortedKeys = 4096`. On the SF10 metadata, q93's 36.8K and q50's 26.8K returned tickets fall in all 236 store_sales row groups, so a higher cap or a range-set summary would skip nothing.
- The bench reads a `<table>/` folder instead of `<table>.parquet` when one exists in the data dir, so a multi-file copy (made under `target/`) can be timed.
- Tests cover a folder of 2 files and a Hive-partitioned folder: the row groups read with the toggle on and off, and the join results.

## Alternatives and Why Not?

- Turn on the full source row filter for datasets: uni filters dataset rows after decoding, so it would not save decoding work.
- Prune Hive partitions by join keys: uni reads partition values as strings unless given types, and string keys have no runtime set filter, so this needs more work (follow-up).
