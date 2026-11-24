# ADR: The GPU executor keeps per-column order and dictionaries in its cache and folds filters into aggregations

Date: 2026-11-24

## Context

After [the runtime choices](2026-11-24-gpu-executor-runtime-choices.md), TPC-H SF10 on one DGX Spark took 0.74-0.79 s with the GPU, against 0.82 s for DuckDB + Sirius with pinned tables. The largest gaps, from `WVLET_GPU_PROFILE=1`:

- q1 (127 vs 66 ms). A filter that keeps 59M of 60M lineitem rows copied every column, including two string columns (49 ms). The aggregate arguments ran about 10 elementwise kernels, three of them FLOAT64 casts for `avg` (45 ms). The group-by over two 1-character string keys took 32 ms.
- q18 grouped all 60M lineitem rows by `l_orderkey` with a hash group-by (48 ms). The file is stored in that order, and cudf's sorted group-by took 27 ms in a microbenchmark.
- q6, q12, and q19 spent 18-26 ms each on filter masks over lineitem, at one kernel per comparison and per AND.
- q13 (76 vs 18 ms): a LEFT join with a residual over 15M orders.

On the GB10, an elementwise kernel over 60M INT64 rows costs 3-4 ms of shared LPDDR5X bandwidth, so copies and intermediate columns dominate.

## Decision

- **Column properties live in the column cache and travel with device tables.** On first use, the cache checks once per integer column whether it is non-decreasing without NULLs. For a low-cardinality string column it builds sorted values and INT32 codes: at most 2^15 values and at most 1/16 of the rows, counted in the cache budget and evicted with the column. A device table records, per column, whether it is sorted and which cached column it holds unchanged. Filters, limits, and pass-through projections keep the order; joins, sorts, key filters, and casts drop it. Only an unfiltered scan column keeps its link to the cache, so its codes line up row for row.
- **A group-by uses those properties.** A single sorted key takes cudf's sort-based path (`sorted_keys`), and a string key with a dictionary groups on its codes, decoding only the result keys (`dictionary_keys`).
- **A filter that keeps most rows becomes a group key** (`masked_filter`). When an aggregation reads a filter whose mask keeps at least half the rows, the mask (NULL as false) is one more group key, and the groups of rejected rows are dropped from the few result rows. This copies nothing and handles keys with NULLs, unlike nulling out the keys of rejected rows.
- **`avg` is an exact sum and a count** (`avg_as_sum`). Results are divided in FLOAT64 over the result groups. The sum reuses the DECIMAL64 and INT64-halves paths, and the existing mean runs when neither applies.
- **Filter comparisons run as one cudf AST kernel** (`ast_filters`). The AND-ed comparisons between fixed-width columns (integers, dates, DECIMAL32/64, FLOAT64), or with constants that convert exactly to the column's type, compile into one `CompiledExpression`. Other conjuncts are evaluated as before and ANDed in.
- Each is a `GpuFeature`, on by default.

## Consequences

- SF10 A/B, best of 3-7 hot runs in one JVM:
  - `masked_filter`: q1 109 to 68 ms.
  - `avg_as_sum`: q1 85 to 69 ms.
  - `dictionary_keys`: q1 68-70 to 65 ms.
  - `sorted_keys`: q18 66 to 45 ms.
  - `ast_filters`: q6 18.8 to 11.5 ms, q12 27.8 to 23.4 ms.
- Results match DuckDB on spec/tpch, in the streaming test, and at SF10.
- Streamed ranges and columns cached in pieces carry neither order nor dictionaries, so these features turn themselves off at SF100 without changing results.
- The sortedness check and the dictionary build run once per cached column, on the first query that needs them (13 ms and tens of ms at SF10).
- q19's filter is an OR over string columns, so it gains nothing from the AST path.
- q13 stays at about 75 ms. Two attempts were dropped:
  - A plain left join against the right rows that pass a right-only residual. It saves the pair filter and the scatter of unmatched rows, but cudf's left join builds its hash table on the 15M filtered orders (43 ms), so q13 went from 75 to 79 ms. Its group-by got 3x faster on the left join's customer-ordered output, which a future join could keep.
  - Packing q1's dictionary codes and mask into one INT32 key. With 4 groups, cudf's hash group-by took 150 ms on one INT32 key against 27 ms on three keys.
