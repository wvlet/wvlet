# Parallel folder scans with join-order statistics

## Goals

- Read a folder of Parquet files about as fast as one file holding the same rows. At SF10, TPC-DS q17 over folders took ~550 ms vs 47 ms on single files (#2501).

## Background

- `ParquetSource` reads a folder or glob through uni's `ParquetDataset` on one thread, with no `scanSplits`, `estimatedRows`, `columnRange` or `partRanges`. So the planner has no row counts for join ordering, and the scan neither runs in parallel nor tests a source row filter.
- uni's public dataset API has no per-row-group read. It handles what plain files don't need: partition values, schema evolution across files, and Iceberg/Delta deletes.

## Design

- Lane: Express. The work item carries the diagnosis, the change is internal to the engine behind a new toggle, and it touches no always-gate area.
- A folder of plain Parquet files is read as a list of single-file `ParquetSource`s (`ParquetFolder`). This holds only when every file reads as a single file with the folder's column names and types, and there are no partition folders, Iceberg or Delta metadata, or object-store paths. Anything else stays on the dataset reader.
- Splits are one per row group of every file. Each file opens when its first part runs and closes after its last.
- Statistics: rows summed, ranges merged, row-group ranges concatenated, distinct counts as for single files, and samples taken from files spread over the folder.
- A catalog source re-lists its folder at most once a second and reopens files only when the listing changed, since catalog sources outlive queries.
- New toggle `folder_file_scans`, applied where the planner resolves sources, because the A/B engines share catalog sources.
- Tests cover splits and statistics, the fallback for partitioned and differing folders, re-listing, and query results with the toggle on and off.

## Alternatives and Why Not?

- Per-file splits through `ParquetDataset.withFiles`: parallel only by file, and still no row filter or row-group statistics.
- Add per-row-group dataset reads to uni: needs a uni release. Partitioned and Iceberg folders are a follow-up.
