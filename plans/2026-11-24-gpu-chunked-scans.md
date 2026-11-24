# Run TPC-H SF100 on one DGX Spark with chunked GPU scans

## Goals

- All 22 TPC-H queries at SF100 return correct results with GPU offload on one DGX Spark (GB10, 128 GB shared memory).
- Each offloaded query is faster than the CPU engine. A query that can't be made faster stays on the CPU from the start, at CPU speed, instead of failing on the device and then falling back (q1: 42 s against 5.6 s on the CPU engine).
- SF10 doesn't regress (0.74-0.79 s today).

## Background

- The GPU executor reads each scanned column whole into an LRU column cache, then runs the subtree over whole columns. At SF100 this fails three ways:
  - q1: about 28 GB of lineitem columns, the filter's copy of them, and columns that earlier queries cached exceed the 75 GiB RMM pool.
  - q12 and q13: a whole `l_shipmode` or `o_comment` column has more than 2 GB of characters. cudf's Parquet reader rejects it ("String column exceeds the column size limit"), because large-strings support is off.
  - q7, q18, and q19: the Java heap (-Xmx16g) runs out in the CPU engine's hash join build, on the CPU side of the A/B. This is the CPU engine, not the GPU.
- A runtime failure falls back to the CPU only after the failed attempt, so these queries take 3-8x as long as the CPU engine.
- cudf 26.10 provides `ParquetChunkedReader`, which reads a file in pieces of bounded byte size, and `HashJoin`, which builds a hash table once and probes it many times. Its Java API can't select Parquet row groups.
- Sirius with pinned tables runs SF100 in 5.10 s; the CPU engine took 43 s on the 16 queries that worked on the GPU.

## Design

Lane: Standard. This changes how the GPU module executes large tables, and the parts that land depend on A/B results.

- **Each part is a `GpuFeature`**, A/B'd on its own: `chunked_scans`, `string_pieces`, `stream_residual`, `reserve_memory`, and `offload_limits` (named in the bullets below).
- **Streamed scans (`chunked_scans`).** A scan of a table with more than a row threshold (default 100M rows: SF100 lineitem and orders; no SF10 table) runs in row ranges of a fixed size (e.g. 32M rows) instead of whole. The nodes above it run once per range, up to the first node that can't stream:
  - Filters, projections, and joins whose streamed input is the probe side (both sides of an inner join; the left side of left, semi, and anti joins). The other join input is computed once for all ranges, and its hash table is built once (`HashJoin`). When both inputs of a join are over the threshold (lineitem ⋈ orders in q3, q9, q18, q21), the input with more rows streams, and the other one is computed once as the build side, concatenating its own ranges.
  - An aggregation above a streamed input aggregates each range, then merges the partial results on the device: sums of sums, sums of counts, and min of mins. avg merges the exact partial sums (decimal or INT64) and the counts, then divides once in Float64, so the only reordering is in the exact sums. A global aggregation whose ranges have no rows still gives a count of 0 and a NULL sum. `count(distinct)` keeps the distinct (keys, argument) rows of each range and counts after the merge. The decimal sum split applies to the merge step as well.
  - Anything else (sort, distinct, limit, the root, the build side of a join) gets the ranges' outputs concatenated. They are filtered and projected by then, so they are much smaller than the scanned columns. Columns that no node above reads are not concatenated (the same pruning as today).
- **The column cache keeps large columns in pieces (`string_pieces`).** A string column of a large table is read with `ParquetChunkedReader` into pieces of at most about 1 GB, and cached as those pieces, so `l_shipmode` and `o_comment` never exist as one column. Fixed-width columns are still read and cached whole. A streamed range copies its rows out of the cached pieces.
- **Right-only residuals of left joins move into the right input when it streams (`stream_residual`, q13).** The `o_comment NOT LIKE` filter then runs per range, and `o_comment` is never concatenated. At SF10 this was slower (see the runtime-choices ADR), so it applies only to streamed inputs.
- **Memory is reserved before a subtree runs (`reserve_memory`).** Before a GPU subtree runs, the cache evicts least-recently-used columns that the subtree doesn't read, until the subtree's estimated working memory fits in the pool. The estimate comes from the Parquet footer, read through the engine's footer cache: row counts, fixed column widths, and for strings rows × the longest min/max statistic. This adds a footer lookup to planning in the GPU module; the device is still untouched until a GPU operator runs.
- **Plan-time rejection (`offload_limits`).** A subtree stays on the CPU when a string column of a table over the row threshold would have to be concatenated (it is read above a concatenation point), or when its estimated working memory exceeds the pool even with the cache empty. The string rule goes by rows, not by an estimate of bytes, because the footer understates dictionary-encoded strings. So these subtrees run on the CPU from the start instead of failing on the device first.
- **CPU heap for SF100.** Check whether q7, q18, and q19 only need a larger test heap. If so, raise `-Xmx` for the GPU bench, or make it settable for SF100, rather than change the CPU join build. The JVM heap and the RMM pool share the GB10's 128 GB, so size `-Xmx` together with `WVLET_GPU_POOL_FRACTION`, and record both in the README.
- **Validation.** `GpuTPCHTest` on spec/tpch, with `WVLET_GPU_TEST_DATA` at SF10 against DuckDB, and with a low streaming threshold so that streaming runs over the small data in CI-sized tests. SF100 bench `gpu_offload` A/B with `rows_match` true and no "GPU execution failed" lines. A/B `chunked_scans` at SF100 and SF10. A per-query SF100 table against Sirius pinned (5.10 s) and the CPU engine. A test with a low streaming threshold and a filter that matches no rows covers the empty global aggregation.
- **Docs.** A new ADR for streaming and the piecewise cache, the SF100 section of examples/dgx-spark, design/engine.md (Accelerators), and this plan's status.

## Alternatives and Why Not?

- **Read row groups selectively:** the Java API can't select row groups. We'd have to rewrite the Parquet footer ourselves, as spark-rapids does, which is a lot of code for an example module.
- **Stream straight from Parquet with no cache:** simpler, but every query would decode lineitem again (Sirius unpinned: 31.6 s at SF100, against 5.10 s pinned).
- **Large strings (`LIBCUDF_LARGE_STRINGS_ENABLED`):** I'll try it as a quick check. It removes the read error, but it doesn't solve q1's memory, and 64-bit string offsets may not be supported by every op or by our host copy. It's a complement to streaming, not a replacement.
- **Stream by estimated bytes instead of rows:** Parquet metadata understates dictionary-encoded strings (`l_shipmode`'s 7 values), so a byte rule would miss the 2 GB case. Row counts are exact.
- **Retry on the GPU after an OOM:** it still pays for the failed attempt. It's kept only as a safety net behind the plan-time checks.

## Status (2026-11-24)

- SF100, best of 2 hot runs: all 22 queries run on the GPU with rows matching the CPU engine and no fallback, 9.17 s against 99.3 s on the CPU engine and 5.10 s for Sirius pinned. SF10 stays at 0.76 s. See [the ADR](../adr/2026-11-24-gpu-chunked-scans.md).
- Landed, each a `GpuFeature`: `chunked_scans`, `string_pieces`, `stream_residual`, `reserve_memory`, and `offload_limits`. The CPU heap is `WVLET_GPU_TEST_HEAP` (28g for SF100).
- Changed from the design: a large table streams only when it must (long strings, or a whole scan over 0.9 of the pool by a filter- and aggregation-aware estimate). Streaming every scan of lineitem and orders made q5, q7, q8, q9, and q10 slower.
- Dropped: cudf large strings, because host copies of a sliced large column came back empty.
- Not done: zero-copy ranges (cudf's Java API makes a copy per slice), and rejecting subtrees by estimated memory at plan time, since the pool size is known only after the first GPU run. `reserve_memory` guards the run instead.
