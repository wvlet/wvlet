# ADR: Hash joins spill only the build partitions that don't fit (hybrid grace hash join)

Date: 2026-11-27 · PR #TBD

## Context

- Join builds are the first holders to run out of heap at SF100: TPC-H q9 needs ~20 GB live heap and fails on a 12 GB heap in the join build. The spill framework ([ADR](2026-11-27-engine-spill-framework.md)) gives `tryReserve`, per-query spill files, and `SpillBlocks`, and aggregations already spill ([ADR](2026-11-27-engine-aggregation-spill.md)).
- A join build is probed in parallel by morsel parts sharing one `JoinBuild`, then right/full joins emit their unmatched rows in a tail run after every part. Runtime filters from the build keys reach the probe scan before it starts.
- DuckDB's radix-partitioned hash join spills partitions under `memory_limit`; DataFusion's hash join does not spill.

## Decision

- **Hybrid, not all-or-nothing.** A build reads its input with `tryReserve`; when the query would pass its spill threshold, it routes the buffered and remaining rows into 64 partitions by a reseeded hash of the build-typed keys. Partitions stay in memory while the query stays under its threshold and the kept partitions take at most half of it; past that, the largest kept partition is written to its file, and its later rows go straight to disk. The kept partitions become the ordinary in-memory table. A build just over budget writes, and its probe side re-reads, only a few partitions.
- **The build counts what building adds.** A spillable build reserves each input batch twice (the batch and its concatenated copy) plus per-row bytes for valid rows, packed keys, chains, and table entries; once built, the table's actual sizes replace that reservation. The trigger therefore fires before concatenation and indexing take the heap past the threshold. This replaces the flat 12 B/row estimate for spillable builds only; with `spill_join` off, a build counts as before.
- **Probe rows of written partitions go to files.** Each probe copy routes rows of written partitions to that partition's probe file, buffering at most 4 MB per copy (parts run without a memory context). NULL keys go to partition 0 on both sides, so NULL-equal keys (INTERSECT/EXCEPT) still meet.
- **The spill tail is decided at run time.** Plans stay the same: each pipeline also carries spill tails, which `MorselOp` runs instead of its planned tails only when some build of the pipeline spilled. A spill tail is a streamed run of the pipeline over an empty input with each join a copy that, once its input ends, emits the kept build's unmatched rows (right/full), frees the kept table, and joins each written partition's build and probe files with a fresh join of the same kind. Nested joins compose, because a lower join's partition rows flow through the copies above it. A written partition that still doesn't fit splits once more by another hash; past that it builds anyway and meets the hard limit.
- **Runtime filters see every key.** A spilled build collects the key range and, up to `RowKeyFilter.MaxKeys` rows, the keys themselves while it routes, and sets the same range, bitmap, Bloom, and key-set filters an in-memory build would. `left_match_prefilter` keeps every row of a spilled build.
- Behind `spill_join`, on by default like `spill_aggregation`: it acts only past the threshold, where the query would otherwise fail. JVM and Native only; builds or probes with `AnyT` columns don't spill.

## Consequences

- When nothing spills, nothing changes but the reservation of spillable builds (closer to their live heap).
- The spill tail runs on the calling thread: the joins of written partitions, and the operators above them in the pipeline, are serial. Parallel spill tails (one task per partition, as `parallel_outer_tail` splits by ranges) are a follow-up.
- Writing evicted build partitions and encoding probe blocks happen on the threads that produce them; files are not compressed.
- Output order changes when a join spills (it is unspecified).
