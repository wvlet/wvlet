# ADR: Group aggregations take the column as the receiver

Date: 2026-10-27 · PR #2145

## Context

After `group by`, a non-grouping column has an array type, and the standard library defines aggregation functions on `array[A]` (`wvlet-stdlib/module/standard/array.wv`): `count_distinct`, `max_by(expr)`, `min_by(expr)`, `to_array`, `sum`, and so on. The column is therefore the receiver: `product.count_distinct`, `product.max_by(amount)`. `_` refers to the group of rows itself and is used for row-level aggregations such as `_.count`.

The syntax reference had documented `_.count_distinct(column)`, `_.max_by(a, b)`, `_.min_by(a, b)` and `_.array_agg(column)`. None of them ever compiled ("Too many arguments", or `_` leaking into SQL), and the AI-agent page already listed `_.count_distinct(col)` as a mistake.

## Decision

Keep the receiver form as the only Wvlet spelling and correct the docs. Do not add `_.f(column)` overloads to the standard library. SQL aggregate calls such as `count(distinct product)` and `max_by(product, amount)` remain available as the SQL-style alternative.

## Consequences

- One Wvlet spelling per aggregation, consistent with `column.sum` / `column.avg` and with method chaining (`age.sum.round(1)`).
- `_` keeps a single meaning in `agg`: the group of rows. It never takes a column argument.
- No stdlib change, so the engine function catalogs and the stdlib reference page are unaffected.
- `spec/basic/agg_column_methods.wv` guards the documented forms.
