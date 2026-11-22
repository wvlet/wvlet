# ADR: Engine join order from distinct-count estimates

Date: 2026-11-22 · PR #2233

## Context

The engine plans inner joins as a left-deep tree. It starts from the largest input, and until now it picked the smallest connected input as the next build side. That rule assumes every join key is unique on the build side. A many-to-many key breaks the assumption. In TPC-DS q24, `s_zip = ca_zip` matches many addresses per store, and the plan joined it before `customer`, which keys `customer_address` uniquely. The engine took about 100x DuckDB's time on the query. Local filters all counted as selectivity 0.3, so `d_year = 2000` (one of about 200 years) looked no more selective than any other filter.

## Decision

- Like DuckDB's greedy join ordering, the next join is the connected input with the fewest estimated output rows, |L| · |R| / ∏ max(ndv(l), ndv(r)) over the key pairs, with ties going to the smaller input
  - A key whose distinct count is unknown is assumed unique on the build side, which is the old foreign-key-to-primary-key behavior
  - The divisor is capped at max(|L|, |R|): keys of one join are usually correlated (a ticket number and an item), and multiplying their distinct counts would estimate almost no rows
- Distinct counts come from `TableSource.estimatedDistinct`. For Parquet files, integer and date columns use the width of the min/max range in the file statistics, at most the non-null count. String columns of files with up to 1M rows are counted exactly by reading the column once, and the count is cached per (path, column). Parquet writers rarely store distinct counts, and DuckDB itself keeps HyperLogLog statistics that we cannot read from the files
- Filter selectivity is 1/ndv for `column = constant` and n/ndv for an IN list of n constants. Any other condition keeps 0.3. `x = null` means `x is null` in wvlet, so it is not treated as an equality

## Consequences

- TPC-DS sf=1 drops from about 11.1s to 8.2s in total. q24 drops from 1952 ms to about 55 ms. TPC-H is neutral
- Planning may read a string join or filter column once per file, up to 1M rows. Larger files fall back to the unique-key assumption
- A range in the statistics overestimates distinct counts of sparse columns. For example, `d_date_sk` spans 73k values while sales cover only about 1.8k of them. Selectivity of range predicates is not estimated yet
