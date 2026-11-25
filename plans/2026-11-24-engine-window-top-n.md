# Engine: top-N per window partition

## Goals

- Cut TPC-DS q67's remaining gap at SF10 after #2383: its `rank() over (partition by i_category order by sumsales desc)` filtered to `rk <= 100` sorted millions of rows in about ten partitions (`Long.compare` / `mergeSort` ~25% of CPU).

## Background

- `WindowOp` materializes its input, splits it into partitions, and fully sorts each partition, even when a filter above keeps only the first k rows of each.
- row_number() and rank() of a row depend only on the rows before it in the window order, so dropping the rows after the k-th (after its ties, for rank) leaves every kept row's value unchanged.

## Design

Lane: Standard (an executor change behind a toggle); the launch prompt set the scope and loop, so I proceed and hold the merge for your decision.

- New toggle `window_top_n` (on by default). At lowering, a filter directly over a window whose calls are all row_number() / rank() over the same PARTITION BY and ORDER BY, with a conjunct `r <= k`, `r < k`, or `r = k` on one of them (constant on either side), gives the window op a top-N.
- Each partition keeps its top k rows with a bounded max-heap (O(n log k)), plus every row tied with the k-th when a rank() is present; only those rows are sorted and output. The filter stays above, so the other conjuncts and the exact bound still apply.
- Not applied to dense_rank(), windows with other calls (aggregates, lag/lead), or no ORDER BY.
- Tests: rank with ties at the bound, row_number, `=`, `<`, a constant on the left, NULL order keys, rank and row_number together, a window aggregate alongside (not applied), limits of 0 and above the partition size, against DuckDB with the toggle on and off, sequential and parallel.

## Alternatives and Why Not?

- **A logical TopN node in the runtime plan.** More passes would need to know it; the bound only matters to the window operator.
- **Partial sorts (sort only the first k).** The heap is simpler and needs only one pass over unsorted rows.
