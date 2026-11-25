# ADR: Runtime join filters drop probe rows per row in the scan

Date: 2026-11-24 · PR (row-level runtime join filters)

## Context

- The runtime join filters of [2026-11-23](2026-11-23-engine-runtime-join-filters.md) only skip probe-side row groups outside the build's [min, max] key range. Every row of a surviving row group is decoded, gathered, and pushed through the probe pipeline (other joins' probes, filters, projections) before its own join rejects it.
- DuckDB 2.0.1 also tests membership per row in the probe scan (an IN list up to 50 keys, a bitmap over dense integer keys, a blocked Bloom filter) and pauses a filter that keeps most rows. At SF10, its probe scans of TPC-DS q80, q17, and q73 return a few thousand rows where the engine read millions.

## Decision

- With `row_join_filters`, a build that sets a range filter also sets a `RowKeyFilter` over its non-NULL integer keys (Int32, Int64, dates, timestamps): a bitmap over [min, max] when it takes at most 32 bits per build row (or 64K bits), else a register-blocked Bloom filter (16 bits per row, 4 bits per key in one 64-bit word) checked inside the range. Builds with more than 4M rows set the range only, tested per row: their filter costs more to build than it saves, and they are rarely selective.
- No separate IN list: for a small build, a bitmap or a two-word Bloom filter costs as little per row, and an IN list would only add row-group pruning on unclustered data, where min/max already covers clustered keys.
- `ScanOp.narrow` applies the set filters to each batch the source returns, on the sequential path and on every morsel part, and narrows the batch with a deferred `take` (a gather when `late_gather` is off). NULL probe keys are dropped: these joins never match them.
- The filter is read per batch, not once when the scan opens. No explicit build-before-probe scheduling was added: builds already finish before their probe scans start (`HashJoinOp` builds before it pulls its probe, `MorselOp` runs its pipeline's builds in `prepare`), and a scan that starts earlier picks the filter up from its next batch.
- Pausing: a filter keeping at least half the rows it checked (90% for a range-only one) over 64K rows skips the next 4 batches, twice as many after each pause in a row (up to 256), then samples again. The counters are shared by all scan copies and updated without locks; they steer only when the filter runs, never which rows it may drop.
- The filters go to the same scans as the range filters (`Lowering.runtimeFilters` and `scanColumns`, unchanged). The Lean model `JoinKeyFilter/Probe.lean` proves that a test true on every non-NULL build key keeps the join's rows for exactly the probe inputs `Lowering.filteredProbe` picks, and `JoinKeyFilterConformanceTest` checks the two agree. The path from the join down to the scan is not modeled.
- Explain text is unchanged: `join_filter=` already names the filtered columns.

## Consequences

- SF10 A/B (`WVLET_ENGINE_BENCH_AB=row_join_filters`, 3 runs, load avg 7–15) on the queries the gap matrix named: TPC-DS 0.88 wall / 0.81 CPU over 12 queries (q80 0.69, q17 0.76, q25 0.78), TPC-H 0.91 wall / 0.85 CPU over 12 (q9 0.75, q5 0.83, q17 0.83). Single-query regressions (q2 1.14, q87 1.10 in one run) did not reproduce in repeated runs.
- TPC-H q18 and q7 gain little: q18's selective key comes from the build side of the lower join (it needs the semi join placed first), and q7's nation filter is an OR over both sides that isn't pushed to either scan yet.
- Follow-ups: pass the filter to uni's Parquet reader so payload columns decode only for surviving rows; string keys (hash-based Bloom filters); parallel filter builds for builds near the 4M-row cap.
