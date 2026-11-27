# Partition pruning in join order, and row filters for partitioned folders

## Goals

- Make the join search count the partitions a probe scan skips by a join on its partition key, so that TPC-DS q25 over date-partitioned store_sales keeps its 30-date d1 join over the store_sales probe (#2508 follow-up).
- Give the files of a Hive-partitioned folder a source row filter, with the partition-key conjuncts decided per file.

## Design

- Lane: Express (coordinator-relayed engine loop item; internal to the engine, behind toggles).
- `partition_probe_scan_cost`: `JoinSearch.partitionKey` tells whether an equality side is a scan column whose parts (Hive partitions or row groups) each hold one value, on a source that skips parts by key sets. `JoinEnumerator` keeps, per DP set, the share of each input's rows its scans read; a join whose build keys reach such a column on its probe pipeline lowers that share to the build keys' share of the column's values, and the cost gets the rows no longer read as a credit.
- `partition_row_filters`: `ParquetSource.supportsRowFilter` holds for partitioned folders read as files. `ParquetFolder.decideKeys` decides the partition-key leaves of the row filter per file (true, false, or the rest over file columns); rejected files are skipped. Scans that fall back to `scan()` read through the same per-file path when they carry a row filter.

## Alternatives and Why Not?

- Charge every probe scan its read rows (not only partition pruning): changes every plan's cost, and plain row-group pruning by sparse keys needs the build keys, which planning doesn't have.
- Drop partition keys from `canRowFilterOn`: also disables #2501's key-set partition pruning, which goes through the same check.
