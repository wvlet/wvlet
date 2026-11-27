# Hive-partitioned folders read as their files

## Goals

- Give Hive-partitioned folders (`key=value/`) the parallel splits and statistics that #2502 gave plain folders. Let filters and join keys on partition columns skip whole partitions.

## Background

- uni reads partition values as strings unless told their types (or `_common_metadata` records them). DuckDB's `COPY ... (partition_by ...)` records none, so integer and date keys get no range statistics, no key sets (#2501) and no range filters.
- Listing a 1823-partition folder takes ~125 ms in uni, and the dataset reader reads files one at a time.

## Design

- Lane: Express. The coordinator relayed the direction, the change is internal to the engine behind `folder_file_scans`, and it touches no always-gate area.
- Type partition keys whose values are all integers as BIGINT, and all `yyyy-MM-dd` dates as DATE, as DuckDB's `hive_types_autocast` does. Cache the types per path so that later opens list the folder once.
- A partitioned scan picks files with uni's `selectFiles` (partition pruning, including #2501's key sets via `contains`). It reads each file as a single file, splitting by row group, without the filter's partition-key conjuncts, and adds the partition values as constant columns.
- Statistics: file columns from the footers. A partition key's range and its one-part-per-file ranges come from the values, and its distinct count is exact.
- No source row filter for partitioned folders yet. A large folder is re-listed after 50x its listing time.

## Alternatives and Why Not?

- Per-file reads through `ParquetDataset.withFiles`: 2–4 ms of setup per file, and no row-group splits.
- Teach the planner about partition pruning by join keys: a separate planner change (follow-up; q25 shows the need).
