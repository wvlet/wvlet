# ADR: The GPU executor picks decimal widths from the data and rewrites its own subtrees

Date: 2026-11-24

## Context

After [the first GPU executor](2026-11-24-engine-gpu-executor.md), TPC-H SF10 took 2.63 s on one DGX Spark against 0.82 s for DuckDB + Sirius with pinned tables. Profiling each device node (`WVLET_GPU_PROFILE=1`) and timing cudf kernels in isolation showed where the time went:

- A DECIMAL128 sum moves cudf's hash group-by onto its slower path (q1's group-by: 312 ms against 24 ms for the same aggregates over INT64). DECIMAL(15,2) products are declared wider than 18 digits, so q1, q7, and q9 always got DECIMAL128.
- Large inputs were joined or aggregated in full before a later join dropped most of their rows: q17 aggregated all 60M lineitem rows into 2M groups (150 ms) and then kept the groups of 2,044 parts.
- q7 filtered on a nation pair only after all of its joins, and q13 gathered 15M `o_comment` strings that nothing after the join reads.
- q13, q16, and q22 stayed on the CPU over small gaps: a LEFT join with a residual, a `Values` node, and casts around `substring` offsets.

## Decision

- **Decimal widths come from the data, at run time.** The column cache stores max |value| of every numeric column, computed on the device once when the column is cached. Expression evaluation derives bounds from these by interval arithmetic and computes in DECIMAL64 when the result and both operands fit in 18 digits. A sum uses plain DECIMAL64 when bound × rows fits in 63 bits; otherwise each value is split into INT64 halves, summed, and recombined exactly in 128 bits. Results stay exact: a bound never undercounts a value, and anything else falls back to DECIMAL128.
- **The GPU module rewrites only its own subtrees.** It derives predicates implied by OR filters and pushes them below inner joins, filters a join's right input by the left input's keys (pushed down through projections, filters, joins, and group keys), and skips gathering join columns that no node above reads. The engine planner stays unchanged, so CPU plans, golden plans, and the Lean-modeled passes are not touched. A rewrite here only adds filters that the original condition implies, or drops rows that cannot join.
- **A key filter applies only when it pays.** It is used when the left input was reduced to at most 1/4 of its largest table, and applied where the filtered input has more than 4 times as many rows as there are keys. With all suppliers or all customers as keys, it dropped no rows and cost 12-36 ms.
- **Each change is a `GpuFeature`.** Every feature is on by default, can be disabled with `WVLET_GPU_DISABLE`, and is compared on and off in one JVM with `WVLET_GPU_BENCH_AB`. The two accelerators share one column cache.

## Consequences

- TPC-H SF10, best of 3 hot runs: 0.75-0.79 s with the GPU, against 2.57 s with every new feature off, 8.5 s for the CPU engine, and 0.82 s for DuckDB + Sirius with pinned tables. All 22 queries run as one GPU subtree, and `GpuTPCHTest` matches DuckDB on spec/tpch and, with `WVLET_GPU_TEST_DATA=target/tpch-sf10`, at SF10.
- Bounds come from whole cached columns, so a filter that removes the large values does not tighten them. A query over a table with a few huge values falls back to DECIMAL128 and the slower group-by.
- The key filter's thresholds are heuristics measured on TPC-H. A plan whose reduced side still matches nearly all rows pays one extra semi join.
- Pruned join columns are all-NULL placeholders, not removed columns, so positions stay those of the runtime plan; they still cost a validity mask per row.
- Moving residual conjuncts below the join was tried and dropped: without column pruning, the filter copied q13's comments once more (107 to 119 ms).
