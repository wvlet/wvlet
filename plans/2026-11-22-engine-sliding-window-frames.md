# Engine sliding-frame window aggregates with exact add/remove

## Goals

- Make count and integer / decimal sums over sliding ROWS frames (`rows[-N,M]`, both bounds given) linear in the partition size, instead of O(n·w) for frame width w.
- Keep the output identical to today's: values, vector class, NULL masks, Decimal scale, the BigDecimal switch, and the overflow error of integer sums.
- Leave every other aggregate and frame shape on its current path.

## Background

- After #2230, frames that start at UNBOUNDED PRECEDING fold one accumulator per partition. Sliding ROWS frames still call `aggregate` per row: a fresh accumulator, a copy of the frame's row indexes, a gather of each argument, and a one-row result vector. The plan for #2230 deferred them because subtracting doubles changes the summation order.
- Removal is exact for counts (count_star, count, count_if) and for integers held in two's-complement Longs. Integer sums (`SumLong`) and decimal sums on unscaled Longs (`SumDecimal`) share that property; sum_if wraps either with a condition mask.
- Today's per-frame fold has two overflow behaviours: `SumLong` uses `Math.addExact` and fails the query when any prefix of a frame (folded left to right) overflows; `SumDecimal` switches that frame to BigDecimal instead, and the result builder turns a BigDecimal back into an unscaled Long when it fits.
- Wrapping Long arithmetic gives the exact frame sum whenever that sum fits, whatever the intermediate states were. Overflow in the per-frame fold can happen only when the frame's sum of absolute values exceeds the Long range.
- For a ROWS frame, `frameRange` clamps the start at 0 and the end at the last row, and both only move forward as k advances, so two pointers cover a partition in O(n). Frames can be empty (a start past the partition end, an end before the partition start, or start > end); count gives 0 and sum gives NULL for them, as today.

## Design

Lane: **Express**. An internal performance change in `wvlet-engine` with no change in behaviour, outside the always-gate areas. The launch prompt withholds merge authorization, so the PR asks the user before enabling auto-merge.

- A new sliding path in `WindowOp` handles ROWS frames whose start and end are both bounded, for count_star, count, count_if / countif, and sum / sum_if whose result type is Int64 or Decimal. Float64 sums, avg (which sums doubles), and every other aggregate stay on the per-frame path.
- Per partition, the arguments are gathered once, and each row is reduced to a "counted" flag (non-NULL argument, true condition; none for a NULL-typed sum argument) and, for sums, a Long value (an integer or an unscaled decimal at the result scale). Two pointers add rows entering the frame and remove rows leaving it, keeping a count and a wrapping sum.
- Exactness check, once per partition: the path is used only when the sum of the absolute values of the counted rows over the whole partition fits in a Long (computed with `Math.addExact`; Long.MinValue does not fit), and every decimal value has an unscaled Long at the result scale. Then no frame prefix can overflow, so the per-frame path would neither fail nor switch to BigDecimal, and the wrapping sum equals its result. Otherwise the partition falls back to the per-frame path, which keeps today's overflow error and BigDecimal switch exactly.
- Results are written straight into the output column (count, or the sum with NULL when the frame counts no rows), with an unscaled-decimal setter on `OutColumn`, so no one-row vectors are built.
- The partition loop, parallel parts, and output slots are unchanged: each part still writes only its own rows, and no task waits on another.
- `runningFrames = false` keeps disabling every incremental path, so tests compare the sliding path with the per-frame one over many frame shapes (narrow, wide, both PRECEDING, both FOLLOWING, empty frames, bounds beyond the partition), NULL arguments and conditions, decimals near the Long limit (fallback), and integers whose partition fails the check while no frame overflows (the query succeeds with the per-frame values) next to ones where a frame overflows (both paths fail).
- Measurement: a temporary probe on TPC-H sf=1 lineitem (sum with `rows[-5,5]` and `rows[-200,200]`, and a count over a sliding frame), interleaved A/B against main, with result checksums. The ADR's "Windows split by the PARTITION BY key" bullet is rewritten to describe the add/remove path, its allowlist, and the per-partition check (it says sliding frames still recompute each frame today); the columnar-engine plan records the numbers.

## Alternatives and Why Not?

- **Add/remove for Float64 sums and avg.** Subtracting doubles changes the result bits; out of the identical-output requirement.
- **Monotonic deque or segment tree for sliding min/max.** Exact, but needs one implementation per value layout plus NaN, -0.0, and string ordering that must match `MinMax`; a separate follow-up if sliding min/max shows up in workloads.
- **Track overflow per frame instead of per partition.** For decimals the per-frame result equals the wrapping sum whenever the frame's own sum fits, since the builder turns a fitting BigDecimal back into a Long. Would keep the fast path on partitions with huge values, but needs the maximum prefix sum of each frame (a segment tree); partitions near the Long limit are rare enough to recompute.
- **A `remove` method on `Accumulator`.** Touches the shared accumulator API and the GROUP BY path for three aggregates; the window path needs only Longs and flags.
