# External merge sort and window spill

## Goals

- Item 7 of the SF100 goal: sorts and windows hold their whole input today (`SortOp` materializes it into one batch, `WindowOp` too), so a sort or window whose input passes the query's spill threshold runs out of heap instead of spilling. DuckDB and DataFusion sort out of core.
- Outcome: sort- and window-heavy queries (DS q67, q51, q47, q89) complete at SF10 with a threshold that forces spilling, rows matching the in-memory run; nothing changes when the input fits.

## Background

- The spill framework (#2584) gives `tryReserve` (false past the threshold), per-query spill files of `SpillBlocks`, and `AggSpillRouter`, which routes rows by key hash into 64 files in parallel tasks. Grace hash join (#2597) spills only what doesn't fit and counts what building adds (each batch twice: it and its concatenated copy).
- `SortOp` with a limit already keeps a bounded heap per chunk, but only after materializing the whole input. TPC-DS sorts are almost all `limit 100`, so the window is the holder that matters at SF100; a full external sort matters for unlimited ORDER BY.
- A window's rows depend only on their window partition, so the input can be split by any subset of the PARTITION BY keys shared by every call, and each split evaluated alone. The output order of a window is already unspecified.
- The streaming window top-N (`stream_window_top_n`) keeps only its top rows and stays in memory.

## Design

- Lane: Standard by scope, but the direction is the one the work item states and the launch prompt authorized merge, so it proceeds without a design gate.
- **External sort (`spill_sort`, on by default):** `SortOp` buffers its input with `tryReserve` (each batch twice plus index bytes, as the join build counts). Past the threshold it sorts the buffer with the existing parallel sort, writes it as one sorted run file, releases it, and starts the next run. At the end the last buffer stays in memory as a run, and a k-way merge of the runs streams the output, ties going to the earlier run, so the result equals the stable in-memory sort.
- **Top-N stays in memory:** with a limit, a full buffer is cut to its top `limit` rows instead of written, as long as that halves it; only a large limit writes runs (each cut to the limit).
- **Window spill (`spill_window`, on by default):** the spill keys are the PARTITION BY expressions every call shares. Past the threshold, `WindowOp` routes the buffered and remaining rows by those keys into 64 files (`AggSpillRouter`, reseeded so the in-memory parallel split inside a file still spreads), then evaluates one file at a time with a fresh window over it, deleting each file once read. A file that still doesn't fit splits once more with another seed; past that it runs anyway and meets the hard limit.
- Windows with no shared PARTITION BY key, streamed top-N windows, and inputs with `AnyT` columns don't spill and reserve as before.
- Moving `SortOp` out of `Operators.scala` into its own file (plan-identical) keeps the at-limit file from reflowing.
- Tests force spills with tiny thresholds (`SortSpillTest`, `WindowSpillTest` on the JVM) against the in-memory run: ASC/DESC/NULLS ordering, ties and stability, limits, multiple runs and multi-pass merge, several call groups, re-split, cleanup and returned reservations. ADR plus `design/engine.md` Spilling update.

## Alternatives and Why Not?

- Window spill via an external sort on (partition keys, order keys), then streaming partitions: needs one sort order for all call groups and a streaming evaluator; hash routing reuses the in-memory evaluator unchanged.
- Parallel final merge (split by output ranks across runs on disk): needs sampled cut points across files; the serial merge is enough for the rare unlimited spilled sort, and is a follow-up.
