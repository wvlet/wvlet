# ADR: Emit only the unmatched rows of a LEFT join filtered on a NULL build key, in the executor

Date: 2026-11-26 · PR #2493

## Context

TPC-DS q78 keeps the sales rows without a return with `left join returns ... where r_key is null`. A logical rewrite of this shape into an anti join (`left_join_to_anti`), moved above the date join and built on the sales side as DuckDB's RIGHT_ANTI does, measured neutral at SF10: the date runtime filter already reaches the sales scan, and building on sales was 1.28x slower. The remaining cost was in the operator: the LEFT join made a match pair for every return only for the filter to drop it, placed the unmatched rows after the matched ones (so the gathers above read the probe out of order), and each of the ~90% missing probe keys read an entry of a 30–130 MB table.

## Decision

- Keep the plan, and change how the join runs: a physical pass after column pruning (`Planner.markUnmatchedLeftProbes`, `unmatched_left_probe`) marks a LEFT hash join without a residual whose parent filter has a conjunct `k is null` for one of the join's build key columns. The join then emits only its unmatched probe rows in probe order, padded with NULL build columns (`exec/ProbeRows.scala`). The output schema and the filter stay, so no projection is rewritten, and other conjuncts still see the same rows. This holds because a matched row's build key equals a non-NULL probe key; joins with NULL-equal keys are excluded.
- Give only joins whose probes mostly miss (anti joins and those LEFT joins) a Bloom filter of the build keys in a flat `LongJoinTable` of at least 16 MB of entries (`probe_miss_filter`). It is split by the table's partitions so it builds in parallel without atomics. It uses 8 bits per key: in a micro-benchmark of 2.9M keys with 90% misses, probes ran 1.75x faster than through the table alone, against about 1.25x for 16 bits and 1.35x for 4 bits. Inner joins don't get it: their probe scans already drop most missing keys through runtime row filters, so the extra word read would be pure cost.

## Consequences

- q78 runs about 0.93x wall and 0.97x CPU at SF10 (five one-JVM A/Bs). No other golden plan changes.
- The anti joins that `Parallelizer` makes from INTERSECT / EXCEPT are created after the marking pass, so they do not get the Bloom filter yet.
- New probe code goes in `exec/ProbeRows.scala`, because Operators.scala sits at the size where any growth makes scalafmt rewrap unrelated code across the file.
