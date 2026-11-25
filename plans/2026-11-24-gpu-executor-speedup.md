# Speed up the GPU executor on TPC-H SF10

## Goals

- Cut the Wvlet engine + GPU total on TPC-H SF10 (one DGX Spark, best of 3 hot runs) from 2.63 s toward DuckDB + Sirius with pinned tables (0.82 s), with results still matching DuckDB.
- Each change ships behind a GPU-module flag, A/B'd on the Spark in one JVM, so a regression can be switched off without a rebuild.

## Background

- Per-query gap at SF10 (GPU ms vs Sirius pinned ms): q1 465 vs 66, q13 498 vs 18 (CPU), q9 308 vs 93, q22 269 vs 28 (CPU), q17 179 vs 20, q16 175 vs 48, q7 129 vs 41, q21 94 vs 75, q18 83 vs 78. The other 13 queries take 18-53 ms each.
- q1, q7, q9, and q17 sum decimal products. Their operands are DECIMAL(15,2), so the products get a precision above 18 and run as DECIMAL128, and the sums are DECIMAL(38, s). cudf's hash group-by has no 128-bit atomic add, so likely falls back to a sort-based group-by over 60M rows. This is the suspected main cost; profiling confirms it before any change.
- q16's `count(distinct)` maps to cudf `nunique`, which likely also takes the sort-based path.
- q13 stays on the CPU because the GPU rejects a LEFT join with a residual (`o_comment NOT LIKE ...` in the ON clause). q22 stays on the CPU because of an unsupported node (the scalar subquery), and a whole subtree is rejected when any node in it is unsupported.
- The GPU result is copied to the host once per subtree. Strings and DECIMAL128 values are converted one row at a time in `GpuTypes.toVec`. That matters only for subtrees with large outputs.

## Design

Lane: Standard. This is a multi-part performance change to an optional example module, and which parts land depends on what profiling shows.

- **Measure first.** Add per-node device timings at debug level, and run nsys on q1, q9, q16, and q17, to confirm where the time goes before changing anything.
- **Flag A/B in one JVM.** `GpuConfig` gets a flag per change, read from `WVLET_GPU_*` env vars. A `WVLET_GPU_BENCH_AB=<flags>` mode runs a second accelerator with those flags off. It shares the column cache and reports in the same TSV as `gpu_offload` A/B. This needs a small hook in the shared engine bench (an optional off-accelerator).
- **Candidate changes, in expected-payoff order.** Each is kept only if its A/B is a win and `GpuTPCHTest` still passes.
  1. *Decimal sums without 128-bit group-by.* Sum a DECIMAL64 value as two INT64 halves (high and low 32 bits), which the hash group-by handles, and combine them exactly in DECIMAL128 over the few output groups. Exact for any input; no overflow assumption.
  2. *DECIMAL64 arithmetic from value ranges.* Compute each cached column's min and max once on the device, and bound each expression by interval arithmetic at run time. When the bound fits in 18 digits, compute in DECIMAL64; otherwise keep DECIMAL128. The data decides, not the declared precision, so results stay exact.
  3. *LEFT join with a residual.* Inner-join, filter the pairs by the residual, then add the left rows with no passing pair, padded with NULLs. This brings q13 onto the GPU.
  4. *CPU inputs uploaded to the GPU.* When a subtree joins or aggregates but one input is unsupported, run that input on the CPU, upload its rows, and keep the rest on the device. This targets q22 and similar plans. Reject when the uploaded input is the large side (estimated rows).
  5. *count(distinct) as distinct, then count.* Use a hash-based `dropDuplicates` on (keys, argument), then a hash count (q16).
  6. *Bulk host copy.* Copy string offsets and chars, and DECIMAL128 words, in bulk in `toVec` instead of one row at a time.
- **Not in this PR unless profiling points there:** cudf AST (`CompiledExpression`) fusion of filters and projections. It is worth it only if expression evaluation shows up as a large share after items 1-2.
- **Docs.** Update the ADR's consequences, the plan's Status, the examples/dgx-spark results table, and design/engine.md's Accelerators section with what lands and the new numbers.
- **Validation.** Each item is tested by `GpuTPCHTest` (26 cases against DuckDB). The SF10 bench must report `rows_match=true` and no "GPU execution failed" lines.

## Alternatives and Why Not?

- **DECIMAL64 by declared precision only:** q1's products are declared above 18 digits, so this alone would change nothing.
- **Casting decimal sums to FLOAT64:** fast, but inexact; results would differ from the CPU engine and DuckDB.
- **Writing CUDA kernels (e.g. spark-rapids-jni's 128-bit chunk sum):** adds a native build to an example module. The INT64 split gets the same effect with stock cudf ops.
- **Pinning whole tables up front like Sirius:** the column cache already keeps scanned columns on the device. The remaining gap is in compute, not loading.

## Status (2026-11-24)

- SF10, best of 3 hot runs: 0.74-0.79 s, against 2.41 s with every new feature off and 0.82 s for DuckDB + Sirius pinned. Results match DuckDB on spec/tpch and at SF10. See [the ADR](../adr/2026-11-24-gpu-executor-runtime-choices.md).
- Landed, each a `GpuFeature` (A/B'd on the Spark; each listed with its main gain):
  - `decimal_sum_split`: q1 478 to 292 ms.
  - `decimal_narrow`: q1 to 168 ms.
  - `constant_casts`, `upload_values`, `outer_residual`: q22, q16, and q13 move to the GPU; the suite went from 2.31 to 1.66 s.
  - `bulk_copy`: q11 61 to 37 ms.
  - `join_key_filter`: 1.64 to 1.09 s, with q17 at 0.10x and q9 at 0.27x. After rebasing on #2364, the planner reduces q17 itself.
  - `distinct_count`: q16 146 to 44 ms.
  - `implied_filters`: q7 128 to 28 ms.
  - `prune_columns`: q13 107 to 76 ms.
- Dropped: pushing residual conjuncts below the join made q13 slower, because the filter copied its strings once more.
- Not done here: cudf AST fusion (q1's remaining 127 ms is about one third filter copy, one third argument kernels, one third group-by) and sorted-key group-by (q18, about 20 ms by a microbenchmark). Both landed later; see [the SF10 gaps plan](2026-11-24-gpu-sf10-gaps.md).

