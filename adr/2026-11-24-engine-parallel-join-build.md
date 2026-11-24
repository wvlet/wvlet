# ADR: Large hash join builds run their steps on the task pool

Date: 2026-11-24 · PR #2360

## Context

- A hash join build is prepared once, from the calling thread, before the probe morsels start ([morsel parallelism](2026-11-22-engine-morsel-parallelism.md)). Every pool thread waits while it runs.
- Only the build's input scan ran on the pool. Concatenating the input, the NULL-key scan, the key min/max, and the direct-index insertion (#2354) ran on the calling thread. At SF10 this was 68% of TPC-H q21 (a 60M-row `lineitem` build) and 69% of q4.
- Probes read chains in input order, and `uniqueKeys` decides the no-chain probe, so a parallel build must give the same `heads`, `chain`, and `uniqueKeys` as the serial one.

## Decision

- With `parallel_join_build` (on by default), a build of at least `JoinBuild.MinParallelRows` rows runs these steps as tasks on the query's runner (`JoinBuildSteps`). It is still prepared from the calling thread, which waits for its tasks, as hashed builds already did for their partitions.
- The direct index is split by key range, not by row: one slice of `heads` per task. Row chunks count their rows per slice, a prefix sum places each (chunk, slice) block, and chunks scatter their rows in chunk order. Each slice then lists its rows in input order, and its task inserts them in reverse, as the serial build does.
- The parallel concat lives in `JoinBuildSteps`, not in `Batch.concat`, so the `vector` package does not depend on `TaskRunner`, and Sort and nested-loop joins keep the sequential copy.

## Consequences

- TPC-H SF10 total 0.92x (q4 0.63x, q21 0.79x, q7 0.82x, q18 0.89x); SF1 suites unchanged. CPU rises 14–22% on the build-heavy queries because of the scatter pass.
- The scatter takes one more int per valid build row (about 240 MB for q21's build) while the build runs.
- A key range concentrated in one slice falls back to that slice's single task. It stays correct, but that task is as slow as the serial build.
- Builds of different joins in one pipeline still run one after another.
