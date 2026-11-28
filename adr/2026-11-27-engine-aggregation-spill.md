# ADR: Aggregations spill their input rows by key hash

Date: 2026-11-27 · PR #2584

## Context

- `PartitionedAggOp` reads its whole input before it routes rows to partitions, so its memory is mostly buffered input rows, not group tables. Every group falls in one hash partition, which makes the partition the natural spill unit.
- DataFusion spills sorted partial aggregation state; DuckDB spills radix-partitioned aggregate state.

## Decision

- Past the spill threshold, the aggregation routes the rows it buffered and every later row by key hash into 64 spill files, releasing the buffer. Tasks hash, gather, and encode chunks of the input; the reader thread appends their blocks in chunk order. The files are aggregated in waves: as many partitions as fit the headroom (at least a quarter of the threshold) at twice their estimated bytes (rows plus about as much for groups). A wave of one partition runs the in-memory parallel aggregation over it; a wider one runs a task per partition. A wave's reservations return when the next wave starts, after the consumer took its groups.
- A file needing more than half the threshold is split once more with another hash seed; deeper, it runs anyway and meets the query's hard limit.
- Plain strings at most half distinct in a block are written as entries plus codes: smaller files, and partitions read back coded, so grouping uses codes.
- Sorted splits and packed routing apply only to inputs that fit; spilled partitions pack their keys from their own ranges. Output order changes only when the query spills (it is unspecified).
- Behind `spill_aggregation` (on by default); off, the input is held whole as before.

## Consequences

- Spilling input rows needs no serialization of aggregate state, so every aggregate (DISTINCT, quantiles, FILTERs) spills the same way; a heavily reducing aggregation writes more bytes than state spilling would, but such aggregations rarely pass the threshold.
- Plain `HashAggOp` (global aggregates, morsel partials) and `DistinctOp` still don't spill; skew past one re-partition (one huge key) still fails at the limit.
- Measured at SF10 with a 256 MB threshold (vs. in memory, one-JVM A/B): TPC-H q13 1.5x, q18 1.9x; TPC-DS q04 1.17x, q23 1.4x, q75 1.95x, q22 2.4x, q67 7.3x (16 spilling aggregations writing ~1.7 GB each before string dedup). Early versions were far slower: serial routing (q67 38x), splitting every file when other holders took the headroom, and one file set per routing chunk (144K files).
