# ADR: A window under a rank bound keeps each input chunk's top rows as it reads them

Date: 2026-11-25

## Context

`window_top_n` (#2385) gives a window op whose calls are row_number() / rank() under a filter `r <= k` a per-partition heap, but the op still materialized its whole input first: one serial `Batch.concat` of every column, then on the parallel path a gather of the partition keys for every hash part (`StrVec.gather` for q67's `i_category` was the largest window frame in an SF10 profile). TPC-DS q67 ranks several million ROLLUP rows in about ten partitions and keeps 100 of each.

## Decision

- Toggle `window_top_n_stream` (on by default, needs `window_top_n`): the window reads its input in chunks of about 64K rows. Each chunk is a task of the window's runner that keeps, per batch and then for the chunk, only the rows within the top k of their partitions (`WindowTopN.keepTop`), in input order. Only the kept rows are concatenated and go through the existing top-N window evaluation.
- It is exact: a row outside its chunk's top has k rows ranked before it in the chunk (rank: strictly before it), which are before it in the whole input too. `topRows` now returns its heap in input order, so row_number() numbers tied rows in input order with and without chunks, as the stable full sort does.
- When a chunk keeps more than half of its rows (many small partitions), the later chunks are kept whole, so the worst case is the previous materialization plus a pass over the chunks already in flight.
- EXPLAIN shows `top=k with ties, streamed`.

## Consequences

- SF10 one-JVM A/B (10 runs): q67 431 → 313 ms wall (0.73x), CPU flat (0.99x); q44 and q70 within noise; q36 and q86 (no rank bound) unchanged plans.
- Golden plans of q44, q67, and q70 show `streamed`.
- The chunks are cut in the window's thread as batches arrive; the upstream pipeline isn't asked to keep top rows inside its own morsel tasks.
