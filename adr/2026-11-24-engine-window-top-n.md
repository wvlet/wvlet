# ADR: Keep only the top rows of each window partition under a rank bound

Date: 2026-11-24

## Context

After #2383, TPC-DS q67 spent most of its time in `rank() over (partition by i_category order by sumsales desc)`, filtered to `rk <= 100`: `WindowOp` fully sorted each of about ten partitions of several million rows, though the filter keeps 100 rows of each. DuckDB ran it in about 420–460 ms at SF10; the engine took 830–890 ms.

## Decision

- Toggle `window_top_n` (on by default). When lowering a filter directly over a window, if every window call is row_number() or rank() over the same PARTITION BY and ORDER BY (ORDER BY required) and a conjunct bounds one call by a constant (`r <= k`, `r < k`, `r = k`, either side), the window op gets `TopN(k, ties)`, with ties when a rank() is present.
- Each partition keeps its first k rows by a bounded max-heap, plus with ties every row not after the k-th; only those are sorted, evaluated, and output. The filter stays above the window and applies the exact condition.
- It is a lowering decision, not a runtime plan node: only the window operator uses the bound, and the operator rewrites that rebuild window ops (parallelization, column pruning) carry it.
- dense_rank(), other window calls (aggregates, lag / lead, ...), and windows without ORDER BY keep the full sort, since their values for the kept rows depend on later rows or the bound doesn't bound rows.

## Consequences

- SF10 one-JVM A/B (10 runs): q67 891 → 425 ms (0.48x); q44 0.88x; other ranking queries (q36, q47, q49, q57, q70, q86) unchanged. SF1: TPC-DS 0.991x total (q67 0.65x), TPC-H 0.996x.
- Golden plans of q44, q67, and q70 show `top=k with ties`.
- The window input is still materialized in full; a streaming top-N could drop rows before materializing.
