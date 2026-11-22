# Engine window follow-ups: typed outputs, primitive index sort, reused partition hashes

## Goals

- Remove the remaining per-row boxing in `WindowOp`, so window aggregates (sum or count over a partition) and value functions (lag, lead, first_value, last_value, nth_value) run at the speed of the ranking functions.
- Sort partition rows without boxing indexes into `Integer[]`.
- Hash the PARTITION BY keys once per window call instead of twice on the parallel path.
- The output stays identical to the current one, vector class and NULL masks included.

## Background

- PR #2222 added `WindowOp.OutColumn`, which writes Int64 / Float64 results to primitive arrays, but only the ranking functions call `setLong` / `setDouble`. Aggregates and value functions go through `update(i, Any)`, which boxes every value. Every other result type keeps an `Array[Any]` and is rebuilt with `Vec.fromValues`.
- `partitions` boxes each partition's row indexes into `Integer[]` for `java.util.Arrays.sort`; `SortOp.sortRange` does the same. `RowComparator.compareRows` already compares unboxed indexes.
- On the parallel path, `keyHashes` hashes every row to split the rows into parts, then each part's `groupIds` hashes the gathered keys again.
- `aggregate` builds the frame's row list with `(s to e).map(rows).toArray`, which boxes, and creates a new accumulator per frame. Running frames (ORDER BY with the default frame) cost O(n·frame) overall.
- Another session is changing `SortOp` (sort merge, top-N heap); edits there must stay minimal.

## Design

- Lane: **Express**. An internal performance change in `wvlet-engine` with no change in behaviour, outside the always-gate areas. The launch prompt withholds merge authorization, so the PR asks the user before enabling auto-merge.
- `OutColumn` keeps one typed array per physical layout: Int (Int32, Date), Long (Int64, Timestamp), Decimal (unscaled longs), Float64, Str, and Bool. All other types (arrays, maps, AnyT) keep boxed values.
- A new `copyFrom(i, src, j)` copies one row of a source vector. When the source has the column's layout and type, it copies the raw value. Otherwise it converts the way `VecBuilder` would, so the result equals today's `Vec.fromValues` output.
- lag, lead, first_value, last_value, and nth_value copy from the argument vector; aggregates copy from the accumulator's one-row result vector.
- Decimal: a raw copy needs the same `PType.Decimal`, scale included. Every other value is rescaled to the column scale and checked with the same `bitLength() < 64` test as `DecimalBuilder.addBig`. Values that don't fit go to a per-row overflow array, allocated before parts are submitted, so parallel tasks never switch the column's storage. If any slot overflowed, the result is the same `ObjVec` of rescaled BigDecimals that `DecimalBuilder` produces today.
- first_value, last_value, and nth_value scan the frame with a loop, from its start or end and skipping NULLs under IGNORE NULLS, instead of building `(s to e).filter(...)` for every row.
- A shared stable merge sort on `Array[Int]` ranges using `compareRows` (insertion sort for short runs) replaces both `Integer[]` sorts. It lives in its own file; `SortOp.sortRange` only switches its body to it. Any stable sort gives the same order as the TimSort it replaces.
- A `GroupTable.groupIds` overload takes precomputed hashes. Each parallel part passes the `keyHashes` values of its rows instead of re-hashing.
- `aggregate` fills the frame's row indexes with a loop instead of a boxed range. Reusing accumulators across frames, or incremental running frames, is done only if the probes show the per-frame cost matters.
- The ADR's "Windows split by the PARTITION BY key" bullet and the columnar-engine plan's progress notes are updated to cover the typed layouts, the shared index sort, and the measured results.
- Tests cover every output layout with and without NULLs (including a decimal column that mixes in-range and overflowing rows) against the boxed output, and the merge sort's stability against `java.util.Arrays.sort`.
- Measurement: the TPC-H sf=1 lineitem probes from #2222 (rank by l_orderkey; row_number by l_partkey with sum by l_suppkey), A/B against main with interleaved runs, plus a lag / first_value and a decimal-sum probe.

## Alternatives and Why Not?

- Keep `Array[Any]` storage and only remove the `Vec.fromValues` copy: every value still boxes, which is the cost we want to remove.
- Use `it.unimi.dsi.fastutil` or another library's primitive sort with a comparator: it adds a dependency that must also work on Scala.js and Scala Native.
- Pass whole partitions to the accumulator with group ids per partition: this changes the aggregation structure more than this follow-up needs; it is a possible next step if per-partition accumulator setup shows up in profiles.
