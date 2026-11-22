# Engine sliding-frame min / max with a monotonic deque

## Goals

- Make min, max, max_by / arg_max, and min_by / arg_min over sliding ROWS frames (`rows[-N,M]`, both bounds given) linear in the partition size, instead of O(n·w) for frame width w.
- Keep the output identical to the per-frame path: values, vector class, NULLs, and which of several equal-comparing values is returned (-0.0 vs 0.0, decimals of different scale).

## Background

- After #2246, sliding ROWS frames add and remove rows for counts and Int64 / Decimal sums. Every other aggregate over a bounded frame still builds a fresh accumulator, copies the frame's row indexes, and gathers its arguments for each row.
- `MinMax` folds a frame's rows left to right, seeded by the first non-NULL value, and replaces the current value only on a strict improvement. So among equal-comparing values the leftmost wins. It compares three ways, chosen by the result type:
  - Int32 / Int64 / Date / Timestamp as Longs.
  - Float64 with primitive `<` / `>`: a NaN seed is never replaced, and a NaN after a non-NaN seed is never taken. The result is NaN when the frame's first non-NULL value is NaN, and otherwise the leftmost max (min) of the non-NaN values. -0.0 and 0.0 are equal, so the earlier one wins.
  - Everything else (strings, decimals as BigDecimal, booleans, arrays) with `ValueOps.compare`, which is a total preorder over one column's values. BigDecimal `compareTo` ignores scale, so 1.50 and 1.5 tie.
- `ByKey` (max_by / min_by) is the same leftmost-strict fold over the non-NULL keys with `ValueOps.compare` (`Double.compare` for doubles, so NaN and -0.0 are ordered). It returns the value of the winning key's row, which may be NULL.
- Both frame bounds of a ROWS frame only move forward as the row advances (#2246's two pointers), so each row enters and leaves the frame once.

## Design

Lane: **Express**. An internal performance change in `wvlet-engine` with no change in behaviour, outside the always-gate areas. The launch prompt authorizes auto-merge for this engine work.

- **Pick the winning row, not the value.** Per partition, a monotonic deque of row positions gives, for each frame, the row the per-frame fold would end on: rows enter at the back after dropping earlier rows that are strictly worse, and leave at the front when they fall out of the frame. Keeping equal rows makes the front the leftmost best row. O(n) amortized per partition, whatever the frame width.
- **Float64 NaN rule.** NaN and NULL rows never enter the deque. A forward pointer tracks the frame's first non-NULL row: if it is NaN, that row wins; otherwise the deque front does.
- **The real accumulator builds the value.** The winning rows are gathered once and fed to one `MinMax` / `ByKey` accumulator, one group per output row (no rows for an empty or all-NULL frame, so it stays NULL). A frame's per-frame result equals the accumulator's result over its winning row alone, so value conversion, vector class, BigDecimal values, and NULLs come from the same code. Each row's result is copied into the output column as the per-frame path copies its one-row vector.
- **Empty and all-NULL frames.** The accumulator is sized for every row of the partition, so rows whose frame feeds nothing come out NULL. The deque needs no reset when a frame jumps past every row seen so far: the old rows leave at the front.
- **Comparison layouts.** Long-backed values (integers, dates, timestamps, and a Decimal column's own unscaled Longs, which share one scale, so no rounding is involved) compare as Longs; Float64 as doubles; everything else, and ByKey keys, as `ValueOps.compare` on values read once per partition.
- **Scope.** `isSlidingFrame` adds min / max with one argument and max_by / arg_max / min_by / arg_min with two. Everything else is unchanged: other aggregates, frame shapes, parallel parts (each part writes only its own rows; no task waits on another), and `runningFrames = false` as the reference path.
- **Tests** compare the sliding path with the per-frame one over many frame shapes (narrow, wide, both PRECEDING or both FOLLOWING, empty frames, bounds beyond the partition) for Int / Long / Date / Timestamp / Float64 (NaN first, NaN later, -0.0 next to 0.0) / string / decimal (including different scales and BigDecimal-only values) columns with NULLs, and for max_by / min_by with NULL keys, NULL values, and tied keys.
- **Measurement.** A temporary probe on TPC-H sf=1 lineitem: max(l_extendedprice) and min(l_shipdate) over `partition by l_suppkey order by l_orderkey` with `rows[-5,5]` and `rows[-200,200]`, interleaved A/B against the #2246 branch with checksums. The ADR's "Windows split by the PARTITION BY key" bullet gains min / max; the columnar-engine plan records the numbers.

## Alternatives and Why Not?

- **Write the deque's value straight into the output column.** Skips one gather and accumulator per partition, but re-implements MinMax's result conversion per layout (Int32 narrowing, decimal rescale, BigDecimal switch); feeding the winners to the accumulator keeps that exact by construction.
- **Sparse table or segment tree.** O(n log n) build and per-frame queries; no gain over the deque for frames that only move forward.
- **Van Herk / Gil-Werman block min/max.** Also O(n), but needs an associative combine; MinMax's Float64 NaN rule depends on fold order, so it doesn't combine blockwise.
