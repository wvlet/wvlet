# Engine hash join tables (DuckDB / DataFusion layouts)

## Goals

- Close the largest remaining CPU gap to DuckDB: hash join is 3.7x DuckDB on TPC-H (1.05 s vs 0.28 s per SF1 suite run) and 2.9x on TPC-DS (2.55 s vs 0.87 s).
- Cut the dependent random loads per probe row, which the profile shows as the cost (findLong cache misses 16–21%, probeBatch self 10–13%, partition gather and scatter ~10%).
- Keep results identical to DuckDB on every conformance suite.

## Background

- A probe row today goes through four dependent random loads: the GroupTable slot, the LongStore key value, `heads(g)` for the first build row, then `chain(row)` for the next match (usually -1).
- Partitioned builds (>= 64K rows, parallel) also make every probe batch split rows by partition, gather keys per partition, and scatter results back.
- #2349 found that probing partition tables in place regressed TPC-H q9 by 15%, because rows grouped by partition read probe keys at random positions. Probe keys must stay sequential.
- #2350 packs multi-column integer keys into one Int64 (from their build min / max), so most TPC-H / TPC-DS joins already probe a single Int32 / Int64 key. Single-column keys have no range computed yet.
- DuckDB: salted pointer entries in one array, a vectorized probe (hash the whole vector, then slots, then compare), and a perfect hash join (direct array) for dense integer build keys.
- DataFusion: one flat `next` chain array over build row indices plus a hash map from key to the last row, batch lookup, and a visited bitmap for outer joins (our `chain` and `matched` already match this).

## Design

- Lane: **Express**. Internal executor change with no plan or result change, no always-gate area. Merge authorized in the launch prompt.
- `JoinBuild.firstMatches` picks the index the build chose; the GroupTable tables stay as the fallback for strings, doubles, and keys that cannot be packed.
- PR 1, **direct index** (DuckDB's perfect hash join, extended with chains for duplicate keys):
  - A range pass over the single Int32 / Int64 / Date key (packed or not). When it spans at most 4 slots per build row (at least 1024, at most 2^27 slots), `heads(key - min)` is the first build row. No hash, no compare, one load per probe row. 4 bytes per slot keeps it at most 16 bytes per build row, under a half-full hash table.
  - Probe keys outside the build range and NULL probe keys never match; chains keep build rows in input order, as the hash tables do.
  - A `join_direct_index` optimizer toggle turns it off, so EngineOptimizerToggleTest keeps checking the hash-table path, and A/B runs can use one build.
  - Built on the calling thread in one pass; no partitioning, so probes skip partitionRows and the gather and scatter.
  - Targets TPC-DS surrogate keys (date_dim, item, customer_demographics, store) and TPC-H part / supplier / customer / nation keys.
- PR 1 also skips the chain walk when the build has no duplicate keys (a flag set while building), so unique-key joins never read `chain(row)`.
- PR 2, **flat Int64 hash table** for sparse single-Long keys (e.g. filtered orders):
  - Open addressing over one `Array[Long]` of (key, first row) pairs: one cache line per probe instead of slots plus key stores. Empty slots are marked by row -1, since any Long can be a key.
  - Each hash partition owns a contiguous region of the array, built in parallel as today; a probe computes its region and slot directly, walking probe rows in order, so it needs no partition split, gather, or scatter.
  - Separate passes per batch, as DuckDB's probe: slot indexes for the whole batch (hashing is already batched), then entry loads, then a compare pass for the few rows that collided, so independent misses overlap.
- PR 3, only if the profile still shows it: fewer per-batch allocations in probeBatch (reused pair buffers, no trim copies).
- Each PR is measured against a detached origin/main worktree at SF1 in alternating rounds, with an async-profiler bucket of the join before and after.
- Tests: conformance suites (TPC-H, TPC-DS, basic spec, parallel join, decorrelation, NOT IN, runtime join filters) plus unit tests comparing each index's `firstMatches` against the GroupTable fallback, including NULL, out-of-range, duplicate, and negative keys.

## Alternatives and Why Not?

- Software prefetch / batched slot loads: the JVM has no prefetch intrinsic; the two-pass loop is the closest form.
- Radix-partitioned probe (DuckDB's external join): needed only for builds that spill; ours fit in memory.
- Replacing GroupTable for aggregation too: aggregation is not in scope and its key ranges are unknown up front (#2350).
