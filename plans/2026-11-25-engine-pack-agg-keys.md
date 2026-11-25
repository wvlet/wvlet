# Engine: group partitioned aggregations by packed integer keys

## Goals

- Cut q78's final aggregation (SF10, after #2392): each channel groups ~5M rows by (year, item, customer), and `GroupTable` compares the three Int64 keys through its generic per-column path (13% of CPU before #2392).

## Background

- `GroupTable` has a single-Int64 fast path (`findLong`); multi-column keys hash each column and compare through virtual `KeyStore.equalsAt` calls.
- `PartitionedAggOp` materializes each partition's rows before aggregating them, so the key ranges of a partition are known up front, as the build side's are for `KeyPacker` in hash joins.

## Design

Lane: Standard (an executor change behind a toggle); within the launch prompt's loop.

- New toggle `pack_agg_keys` (on by default): a `PartitionedAggOp` partition with two or more Int32 / Date / Int64 key columns takes each column's min / max over its rows. If the codes fit in 63 bits, it groups by one packed Int64 key (`AggKeyPacking`: code 0 for NULL, `v - min + 1` otherwise, each column in its own bits) and unpacks the key columns of the groups.
- Packed keys are equal exactly when the keys are, NULLs equal to each other, so groups, values, and first-seen order are unchanged. Other key types, a single key, or wider ranges keep the unpacked path.
- Plans don't change: `EngineOptimizerToggleTest` ignores the toggle; `EngineAdaptivePartialAggsTest` checks the flag reaches the partitioned aggregations; `AggKeyPackingTest` (shared) checks round trips with NULLs, equality, ranges over several batches, the fallbacks, and the same groups in the same order from a partitioned aggregation.
