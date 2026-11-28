# ADR: The GPU executor runs trees of decimal and FLOAT64 arithmetic as one cudf AST kernel

Date: 2026-11-27

## Context

- Outside filters, the GPU executor evaluates expressions one cudf kernel per operator, and each writes a full column. On the GB10, one elementwise pass over 60M rows costs 3-4 ms of shared memory bandwidth, so TPC-H q1's aggregate arguments (`l_extendedprice * (1 - l_discount)` and `* (1 + l_tax)`) spent most of their time writing intermediates.
- [`ast_filters`](2026-11-24-gpu-column-properties.md) already fused comparisons into one cudf AST kernel, but only comparisons: cudf's AST was known to accept DECIMAL64 there, not whether its arithmetic keeps decimal scales the way cudf's binary operators do.
- PG-Strom's approach (one device program per expression, no intermediates) motivated this; a hand-written fused kernel is a larger step left for later.

## Decision

- **`ast_projections` compiles `+`, `-`, `*`, and negation over DECIMAL64 or FLOAT64 into one AST kernel**, at the root `Arith` of any such tree reached by `GpuExprs.eval`: projections, aggregate arguments, and CASE branches.
- **Each node computes in the type the per-operator path uses.** A decimal node fuses only where the value bounds narrow it to DECIMAL64 (the AST has no 128-bit decimals), and each side keeps its own scale. A probe on the DGX Spark (cudf 26.10.2) showed the AST applies the binary operators' scale rules: a product's scale is the sum of its sides' scales, and a sum aligns to the larger scale. So results are identical without rescaling.
- **Anything else is a leaf**: a child that doesn't fuse (CASE, LIKE, a cast that changes the scale, a DECIMAL128 node, an integer column under a decimal) is evaluated per operator first and read by the kernel as one more input column. Constants become AST literals only when they convert exactly.
- **Division, modulo, and integer `+ - *` stay per operator**: the CPU returns NULL on division by zero and raises on integer overflow, and the AST does neither.
- **A tree fuses only with at least two operators and a column read**; one operator gains nothing over cudf's binary operator, and a tree of constants has no column to set the kernel's output rows.
- Computing on unscaled INT64 values (`bitCastTo`) and relabelling the result also worked in the probe; it wasn't needed.

## Consequences

- TPC-H SF10 on one DGX Spark, two one-JVM A/B rounds of 7 hot runs: q1 70.7 to 47.5 ms and 67.5 to 46.7 ms (0.67-0.69x); the suite 733 to 711 ms. Other queries moved within noise (±5%): their revenue expressions run over rows already reduced by joins.
- `GpuAstArithTest` checks fused against per-operator values and types over mixed scales, negatives, and NULLs; `GpuTPCHTest` (spec/tpch and SF10) and `GpuStreamTest` match DuckDB with the feature on.
- A filter and its projections still run as separate kernels: the AST returns one column per call.
