# Stdlib statistical and bitwise aggregates on every engine

## Goals
- Add the ADR's tier-1 aggregate names (`adr/2026-11-23-stdlib-function-naming.md`) to the stdlib: `y.corr(x)`, `y.covar_pop(x)`, `y.covar_samp(x)`, `y.regr_slope(x)`, `y.regr_intercept(x)`, `y.regr_r2(x)`, `y.regr_count(x)`, `x.kurtosis`, `x.skewness`, `x.bit_and`, `x.bit_or`, `x.bit_xor`.
- Run them on the `wvlet` engine with the same results as DuckDB.

## Background
- These are standard statistics with the same names on most engines. The differences are in which engines have them and in small-sample conventions: Trino and Spark compute population skewness, Snowflake names it `SKEW`, and Trino and BigQuery lack `regr_r2` and `regr_count`.
- The engine's aggregates are per-group accumulators that are updated one batch at a time and never merge partial states.

## Design
- Lane: Express. The names come from the approved ADR, and nothing here touches an always-gate area.
- The receiver is the dependent variable, as in SQL: `y.regr_slope(x)` is `regr_slope(y, x)`. Pairs with a NULL are skipped.
- Each function keeps DuckDB's results on degenerate groups. An empty group gives NULL (`regr_count` gives 0). With a constant `x`, `corr` and `regr_slope` give NaN, and `regr_intercept` and `regr_r2` give NULL. With a constant `y`, `regr_r2` gives 1.
- `kurtosis` and `skewness` are the sample (bias-corrected) statistics. They are NULL below 4 or 3 values, or when all values are equal. Trino's population skewness gets the sample correction, Snowflake uses `skew`, and Hive and BigQuery are left out.
- `bit_and`, `bit_or`, and `bit_xor` keep the integer type of their input. The engine rejects non-integer input.
- The engine accumulators use DuckDB's formulas: Welford co-moments for covariance and correlation, and power sums for kurtosis and skewness. Results then agree within floating-point rounding.
- Tests: compare each function against DuckDB over regular, single-row, constant, and all-NULL groups, on doubles and integers. Shared unit tests also run on Scala.js.
