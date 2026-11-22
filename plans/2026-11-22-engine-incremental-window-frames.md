# Engine incremental running-frame window aggregates

## Goals

- Make window aggregates whose frame starts at UNBOUNDED PRECEDING linear in the partition size, instead of quadratic. This covers the default frame with ORDER BY (running totals), `ROWS UNBOUNDED PRECEDING AND CURRENT ROW / N PRECEDING / N FOLLOWING`, and `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`.
- A running sum without PARTITION BY over millions of rows must finish in about the time of a sort, where today it never finishes.
- The output stays identical to today's: values, vector class, NULL masks, Decimal scale, and floating-point sums down to the bit.

## Background

- `aggregateOverFrames` calls `aggregate` for every row. That creates a new accumulator, gathers the frame rows, and folds them, so a partition of n rows costs O(n·frame). On TPC-H sf=1 lineitem, `sum(l_quantity) over (partition by l_suppkey order by l_orderkey)` (about 600 rows per partition) takes 0.7 s, against 0.26 s for `partition by l_partkey` (about 30 rows).
- For a frame that starts at the partition start, the frame end never moves backwards as the current row advances: it is `k + N` clamped, the last peer of row k, or the partition end. So each frame extends the previous one.
- Every accumulator folds rows one at a time in row order, and its `result` doesn't change its state. Feeding the frame rows in several `update` calls therefore gives the same state, and the same floating-point summation order, as feeding them in one call. The order is the one the per-frame path uses today: from the partition start forward.
- Some accumulators collect the frame's values (string_agg, array_agg, histogram, quantiles, median, mode). Their `result` copies or sorts what they collected, so it costs O(frame) or more per row, and an incremental fold would stay quadratic.
- The planner encodes an UNBOUNDED bound as `None`, CURRENT ROW as `Some(0)`, and N PRECEDING / FOLLOWING as `Some(-N)` / `Some(N)`. `frameRange` reads a RANGE bound other than UNBOUNDED as a peer bound.
- Sliding ROWS frames (`N PRECEDING AND M FOLLOWING`) already cost O(n·w) with frame width w, which is linear in the partition size. Making them incremental needs add/remove, which changes the floating-point summation order, or a segment tree for min/max.

## Design

Lane: **Express**. An internal performance change in `wvlet-engine` with no change in behaviour, outside the always-gate areas. The launch prompt withholds merge authorization, so the PR asks the user before enabling auto-merge.

- For a frame whose start is `None` (UNBOUNDED PRECEDING), keep one accumulator per partition. For each row, feed only the rows between the previous frame end and the new one, then take the result. Frame ends follow `frameRange`'s semantics, so RANGE `N FOLLOWING` still ends at the last peer, as today.
- Peer-group ends are found in one forward pass: a row inside the previous peer group reuses its end, so a partition where every row is a peer stays linear.
- Rows whose frame end equals the previous row's (peers under the default or RANGE frame, or rows past the end when the frame ends at UNBOUNDED FOLLOWING) reuse the previous result vector instead of calling `result` again.
- Each newly framed range is gathered straight from the input through a reused index array, so no row is gathered twice and no `Array.tabulate` index runs on the hot path (`Vec.slice` uses one). `MinMax.result` for Int32 / Date switches from `Array.tabulate` to a loop for the same reason.
- Empty frames (e.g. `ROWS UNBOUNDED PRECEDING AND 1 PRECEDING` on the first row) take the fresh accumulator's result, as today: NULL for sum, 0 for count.
- Only accumulators that fold rows in order, leave their state unchanged in `result`, and produce a result in O(1) use the running path: count, count_if, sum, sum_if, product, avg, min, max, first / arbitrary / any_value, last, max_by / min_by, bool_and / bool_or, stddev / variance, and count_distinct (whose seen-set grows with the distinct values, as it does today). Other aggregates stay on the per-frame path.
- Sliding ROWS frames and frames that start after the partition start (`CURRENT ROW AND UNBOUNDED FOLLOWING`) stay on the per-frame path in this PR. The note's alternatives say why; the plan records them as follow-ups.
- Integer sums keep `Math.addExact`: the running path adds the same values in the same order, so an overflow fails the query as it does today.
- Tests compare the running path with the per-frame path over every frame shape and every allowlisted aggregate, with NULLs, peers (including a partition where every row is a peer), empty frames, decimals that overflow a Long, and doubles whose sum depends on order. The existing window and TPC-H specs must still pass.
- Measurement: a temporary probe on TPC-H sf=1 lineitem (the two queries above, a sliding ROWS frame, and a running sum without PARTITION BY over all 6M rows, with main measured on a smaller input since it would not finish), interleaved A/B runs against main, and a result checksum compared with main.
- The ADR's "Windows split by the PARTITION BY key" bullet and the columnar-engine plan's progress table and window results are updated with the measured numbers.

## Alternatives and Why Not?

- **Add/remove for sliding frames (sum, count, avg).** Subtracting doubles changes the summation order and the result bits, which breaks the identical-output requirement. It is exact for count and integer or decimal sums; it can follow if sliding frames show up in profiles.
- **Segment tree (or monotonic deque) for sliding min/max.** Correct and exact, but sliding frames are already linear in the partition size; a follow-up if wide frames matter.
- **Reverse feeding for suffix frames (`CURRENT ROW AND UNBOUNDED FOLLOWING`).** Reverses the summation order for doubles, and an integer sum could overflow at a different point; left for a follow-up limited to order-insensitive aggregates.
- **A new accumulator API (`reset`, `remove`, read one group without building a Vec).** Touches every accumulator and the GROUP BY path; the existing `update` / `result` pair is enough for running frames.
