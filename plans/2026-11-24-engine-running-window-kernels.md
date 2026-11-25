# Engine: primitive running window aggregates

## Goals

- Cut TPC-DS q51 (SF10 2.7x DuckDB): its three running windows (`sum(...) over (partition by item order by d_date rows[,0])`, two `max(...)` of the same shape) spend 40% of CPU in `WindowOp.aggregateRunning`.

## Background

- `aggregateRunning` folds each row into a generic accumulator, gathering a one-row vector per row and building a result vector (`acc.result(1)`, `DecimalBuilder`, `BigInteger.valueOf`) per row. q51's partitions are short (one item's days), so the per-row overhead is the whole cost.

## Design

Lane: Standard (an executor change behind a toggle); within the launch prompt's scope and loop, merge held for your decision.

- New toggle `running_window_kernels` (on by default): running count(*) / count(x) over any column, sum over Int64, Decimal (of the result scale), and Float64 columns, and min / max over Int64 and Decimal columns fold primitive values straight from the input arrays into the output column, with the same frame ends as the accumulator path.
- A Long sum that overflows, and every other call or column type, falls back to the accumulator path for that partition, so results are unchanged (decimal sums still switch to BigDecimal there).
- Tests: `WindowOpTest`'s running-frame test now also compares the primitive fold with the per-frame reference over 11 frame shapes, NULLs, ties, overflowing and small decimals, and min / max over Int64 and Decimal; a wiring test checks the flag reaches the window operators.
