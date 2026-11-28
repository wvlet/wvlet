# Bound Parquet decode memory in scans

## Goals

- At ~1.1 GB RSS (`-Xmx768m`), the engine completes 4 of 22 TPC-H SF10 queries; DuckDB completes 18. Even q1 and q6 run out of memory, and the out-of-memory sites are uni Parquet row-group buffers.
- We want scans to fit a small heap with no change at the default heap.

## Background

- `MorselOp` keeps up to 2 × parallelism parts in flight (40 on a 20-core Spark). Each part decodes one whole row group: the compressed chunks, the decompressed pages, and the decoded vectors. A part's output batches stay held until the consumer dequeues it.
- Nothing counts these bytes. `MorselOp.runPart` builds its pipeline under `MemoryContext.NoOp`, and the spill threshold sees only large holders.
- The SF10 files have one page per column chunk (see the engine-parquet-one-page-per-chunk memory). Decoding page by page in uni would therefore save nothing at SF10, so this change needs no uni PR.
- Item 6d (spill threshold headroom) needs a bound on untracked scan memory to subtract from the threshold.

## Design

- Lane: Express. The change is behind a toggle, has no plan change and no UI, and touches no always-gate area. The work item already carries the diagnosis and the direction.
- Each part gets an estimate of its decode bytes from footer metadata: the compressed plus twice the uncompressed size of the column chunks it reads. These estimates ride on `ScanSplits` next to the part row counts.
- `MorselOp` submits the next part only while the estimated bytes of all in-flight parts, across every `MorselOp` in the process, stay within a decode budget. Bytes return when a part's task finishes, since its decode buffers are garbage by then.
- One MorselOp always has at least one part in flight, so there is no blocking and no deadlock. It simply submits fewer parts while the budget is full.
- The budget is process-wide (`ScanDecodeBudget`). It defaults to the larger of an eighth of `Runtime.maxMemory` and all but 1 GB of it on the JVM and can be overridden by the `wvlet.engine.scanDecodeBudget` system property. It is unbounded on Scala.js and Native. On a heap of 4 GB or more, 20 parts fit, so nothing changes; at 2g TPC-H is 1.13x slower.
- The query's `MemoryContext` reports its in-flight scan bytes and their peak as a separate gauge. They count toward neither the limit nor the spill threshold, so 6d's headroom formula stays the only input to spill decisions.
- Toggle: `bound_scan_decode`, on by default if the A/B holds.
- Validation:
  - Unit tests cover admission under a tiny budget, with rows unchanged.
  - Run an A/B of SF10 at the default heap.
  - Repeat the 5a matched-memory runs at 768m and 1536m on spark1, then calibrate the fraction and the estimate formula from them.

## Alternatives and Why Not?

- Page-by-page decode in uni: it saves nothing with one page per chunk, and it is a larger uni change.
- Counting scan bytes as a `MemoryContext` reservation: past the threshold it would trigger aggregation spills, and under a small `queryMemoryLimit` it would fail scans. That would break the existing spill tests and mix this change with 6d.
- A fixed cap on parts in flight: it ignores how wide a row group is. A narrow scan would lose parallelism for nothing.
