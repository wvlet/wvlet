# Fuse GPU arithmetic into one cudf AST kernel

## Goals

- Evaluate a whole arithmetic expression (projections and aggregate arguments, also inside CASE branches) as one cudf AST kernel instead of one cudf kernel per operator, so intermediate columns are never written.
- A/B set: TPC-H q1 (`l_extendedprice * (1 - l_discount) * (1 + l_tax)`), q3, q5, q10, q14. Other revenue expressions (q7, q8, q9, q19) are affected too.
- Results stay exactly those of the CPU engine; the change is a `GpuFeature` (`ast_projections`), A/B-able with `WVLET_GPU_BENCH_AB`.

## Background

- `ast_filters` already compiles AND/OR comparisons into one AST kernel (q6's mask: 16 to 8 ms). Everything else in `GpuExprs.eval` runs one kernel per node, each writing a full column.
- On the GB10, one elementwise pass over 60M rows costs 3–4 ms of shared memory bandwidth; q1's aggregate arguments ran about 10 such kernels before `avg_as_sum` (see the column properties ADR).
- Decimal arithmetic already computes in DECIMAL64 when value bounds from the column cache prove the result fits in 18 digits (runtime choices ADR). Each side keeps its own scale; a multiply's result scale is the sum of the sides' scales.
- Probed on the DGX Spark (cudf 26.10.2): AST `MUL` over DECIMAL64 adds the operands' scales (0.7404 at scale 4 from two scale-2 columns), `ADD`/`SUB` of different scales align to the larger scale, and nested `a * (1 - b)` with a decimal literal matched the expected values. This is the same scale rule as cudf's binary ops that the per-op path relies on today.

## Design

Lane: Express — a performance change inside the optional, JVM-only GPU module, behind a `GpuFeature`, with no change to engine plans, results, or any always-gate area.

- **Compile decimal nodes directly.** Each side enters the AST at its own scale in DECIMAL64, as the per-op path does; the probe showed the AST's result scales match. Computing on unscaled INT64 values (`bitCastTo`) and relabelling also worked in the probe; it stays a fallback if a TPC-H check disagrees.
- **Where:** inside `eval`, at the root `Arith` of any fusable subtree, so arithmetic reached through CASE branches, casts, and aggregate arguments fuses too.
- **What fuses:** `+`, `-`, `*`, negation, numeric constants, and exact widening casts, over DECIMAL64, FLOAT64, and integer columns.
  - Only where the existing DECIMAL64 narrowing applies at every node; a node that needs DECIMAL128 keeps the per-op path (the AST has no 128-bit decimals).
  - Integer `+ - *` over columns stays off the GPU as today: the CPU raises on overflow and the AST wraps.
  - Division stays per-op: decimal division isn't offloaded at all, and integer and FLOAT64 division need the per-op NULL-on-zero handling.
  - Negation fuses only when its child is already DECIMAL64 at its declared type (negation evaluates its child in the declared type today); otherwise it stays per-op.
  - A non-fusable child (CASE, LIKE, a string function) is evaluated per-op first and enters the AST as an input column, so `case when ... then a * (1 - b) else 0 end` still fuses its arithmetic branch.
- **When:** only a tree with at least two arithmetic operators, as with `ast_filters`; a single operator gains nothing over cudf's binary op.
- **Checks:** results must match DuckDB in `GpuTPCHTest` (spec/tpch and SF10) and `GpuStreamTest`; a new device test compares fused and per-op results on decimal columns with mixed scales, negatives, and NULLs (skipped without CUDA). A/B at SF10 with `WVLET_GPU_BENCH_AB=ast_projections`, best of 5–7 hot runs.
- **Docs:** a short ADR with the probe result and the A/B numbers, listed at the top of `adr/README.md`; one line in `design/engine.md`'s accelerator section.

## Alternatives and Why Not?

- **cudf's JIT `transform` (row-wise CUDA UDFs).** Compiles at run time (latency on first use) and the Java binding's support for several inputs is unclear; the AST already covers these operators.
- **A hand-written fused kernel (PG-Strom style: filter, projection, and partial aggregation in one pass).** The real upside (q1 near a ~10 ms bandwidth floor) but needs shipping our own CUDA code; worth a separate spike once this lands.
- **Fusing filters and projections into one kernel.** The AST returns one column per call, so a filter and its projections can't share one pass; `masked_filter` already avoids the copy for aggregations.
