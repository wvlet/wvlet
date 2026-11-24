# ADR: The GPU executor streams large tables in row ranges and caches long strings in pieces

Date: 2026-11-24

## Context

[The GPU executor](2026-11-24-engine-gpu-executor.md) reads each scanned column whole and runs a subtree over whole columns. At TPC-H SF100 (lineitem: 600M rows, orders: 150M) on one DGX Spark, this failed three ways, and each failure fell back to the CPU only after the failed device attempt:

- q1 ran out of the RMM pool (75 GiB): its lineitem columns, the filter's copy of them, and the columns that earlier queries cached didn't fit.
- q12 and q13 read `l_shipmode` and `o_comment`, whose characters exceed cudf's 2 GB limit for one string column ("String column exceeds the column size limit").
- q7, q18, and q19 ran out of Java heap (-Xmx16g) in the CPU engine's hash join build, on the CPU side of the A/B.

cudf 26.10's Java API has `ParquetChunkedReader` (bounded-size pieces of a file), `HashJoin` and `DistinctHashJoin` (build once, probe many times), but no selection of Parquet row groups.

## Decision

- **Scans of tables over `streamRows` rows (default 100M) stream in row ranges when they must** (`chunked_scans`, 32M rows a range): when a string column is too long to concatenate (below), or when the whole scan's working memory would exceed `streamFraction` (0.9) of the pool. That memory is the scan's estimated size times 1, plus 1 for a filter between the scan and the next join or aggregation, plus 1 when that is an aggregation: TPC-H q1 (scan, filter, aggregate) gets 3 and streams; q9's lineitem, reduced by a join first, gets 1 and runs whole. Streaming every large scan was tried first: copying each range out of the cached columns made q5, q7, q8, q9, and q10 0.2-0.37 s slower than running them whole. The subtree above a streamed scan runs once per range, up to the first node that can't stream:
  - Filters, projections, and joins whose streamed input is the probe side: either input of an inner join, the left input of left, semi, and anti joins. When both inputs of an inner join stream (lineitem ⋈ orders), the one with more rows does; the other runs once and concatenates its own ranges.
  - The rest of the subtree that reads no streamed scan runs once per loop and is reused by every range, as are the hash tables built over it (`HashJoin`; semi and anti joins probe a `DistinctHashJoin` of the distinct right keys). Join key filters from a streamed left input are not passed to the right input, which must not depend on a range.
  - An aggregation over a streamed input aggregates each range and merges the partial results on the device: sums of sums, sums of counts, min of mins, and avg as an exact sum and a count divided once. `count(distinct)` keeps the distinct (keys, argument) rows of each range, deduplicates their concatenation, and counts. The DECIMAL64 split applies to the merge too.
  - Anything else concatenates the ranges' results. Columns that no node above reads become all-NULL placeholders first, as join outputs do.
- **The column cache keeps long strings in pieces** (`string_pieces`). String columns of tables over `streamRows` are read with `ParquetChunkedReader` in pieces of about 1 GB, and any column whose whole read hits cudf's size limit falls back to pieces. A range copies its rows out of the pieces; a loop holds its scan's cached columns, so that eviction cannot make later ranges read them again.
- **A left join's right-only residual moves into its right input when that input streams** (`stream_residual`). q13's `o_comment NOT LIKE` then filters each range of orders, and `o_comment` is never concatenated. The runtime-choices ADR measured this move as slower at SF10, where nothing streams, so it applies only to streamed inputs.
- **Memory is reserved before a subtree runs** (`reserve_memory`). The cache evicts the least recently used columns that the subtree doesn't read, until the columns it reads and its estimated working memory (rows × column widths, twice for a whole scan, plus four ranges for a streamed one) fit in 90% of the pool.
- **Plan-time limits** (`offload_limits`). A subtree stays on the CPU when it would concatenate a string column of a table over `streamRows` whose estimated characters exceed 1.5 GB. The estimate comes from the Parquet footer: the uncompressed size of plain-encoded column chunks, and rows × the mean length of the min and max values of dictionary-encoded ones, whose pages hold only the distinct values.
- **cudf's large strings are not used.** With `LIBCUDF_LARGE_STRINGS_ENABLED=1`, cudf 26.10 reads `o_comment` whole (8.5 GB in 1.3 s) and filters it correctly, but values copied to the host from a slice of the large column came back as empty strings. Streaming keeps every string column below 2 GB instead.
- **The SF100 bench needs a larger test heap** for the CPU engine's join builds: `WVLET_GPU_TEST_HEAP` (default 16g) sets `-Xmx` for the GPU module's tests. It shares the GB10's 128 GB with the RMM pool (`WVLET_GPU_POOL_FRACTION`).
- Each part is a `GpuFeature`, on by default; `GpuStreamTest` runs spec/tpch with every table over 1,000 rows streaming in ranges of 2,500 rows, and strings over 4 KB cached in 16 KB pieces, against DuckDB.

## Consequences

- TPC-H SF100 on one DGX Spark, best of 2 hot runs: all 22 queries run on the GPU and match the CPU engine's rows, in 9.17 s against 99.3 s on the CPU engine and 5.10 s for DuckDB + Sirius with pinned tables. q1, q12, q13, and q19 stream (1.41, 0.65, 1.18, and 0.74 s); before, they failed on the device and took 17-37 s on the CPU. Per query: [the dgx-spark example](../examples/dgx-spark/README.md#sf100-2026-11-24).
- In `chunked_scans` A/B runs at SF100, q1, q12, q13, and q19 took 2-4% of their time with it off (where they fall back to the CPU), and with the final rule q5, q7, q8, q9, and q10 run whole, within 4% of their time with it off.
- `reserve_memory` was neutral in its A/B (9.01 against 8.98 s), where both sides share one column cache; it stays as a guard for subtrees whose columns crowd out the pool.
- SF10 is unchanged (757 ms with every new feature on, 762 ms off): no SF10 table has more than 100M rows, and the memory and string checks are skipped without such a table.
- Rows are counted from Parquet metadata; a table without row estimates never streams.
- A streamed subtree copies each range out of the cached columns once, and a probe side's hash table is not reused across queries. A table whose columns don't fit in the pool at all still fails at run time and falls back to the CPU; reading ranges straight from Parquet would need row-group selection, which cudf's Java API lacks.
