# ADR: String concatenation treats NULL as an empty string

Date: 2026-11-18 · PR #2184

## Context

- Wvlet's string `+` and `s"..."` interpolation compiled to SQL `concat(...)`. Engines disagree on `concat` with a NULL argument: DuckDB skips it (`concat('a', NULL) = 'a'`, `concat(NULL, NULL) = ''`), while Trino, Snowflake, and Hive return NULL. The same Wvlet query therefore returned different results per engine.
- Trino's `concat` also rejects non-string arguments, and the rule added no cast, so `'hello ' + 2024` failed there.
- The Lean model of `RewriteExpr` (see the Lean formal verification ADR) made these differences explicit: no single definition of `+` was preserved on every engine.

## Decision

- **NULL is `''` in string `+` and interpolation.** `NULL + 'a'` is `'a'`, and `s"id: ${x}"` with a NULL `x` is `'id: '`. SQL's NULL-propagating `||` is not followed: SQL already treats NULL one way in binary operators and another in aggregates, and Wvlet already departs from it (`x = null` means `x is null`). With NULL as `''`, concatenation is associative with `''` as its identity, so the left-nested `concat` the rules build needs no special cases.
- **One SQL form for every engine.** Each operand is passed to `concat` as a string that is never NULL, `coalesce(cast(x as varchar), '')`. The cast is left out for string operands, and the `coalesce` for literals and nested rewrites, which are never NULL. Every engine's `concat` agrees on such arguments, so there is no per-engine printing and no dedicated expression node.
- `+` is rewritten when either side is a string (as the Typer types it), and a single-part `s"${x}"` is a string too.

## Consequences

- On Trino, Snowflake, and Hive, string `+` and `s"..."` with a NULL operand now return the other text instead of NULL. DuckDB results are unchanged, though its SQL gains `cast`/`coalesce` (e.g. the column header of `'hello ' + 2024` becomes `concat('hello ', CAST(2024 AS string))`).
- To get NULL from a NULL operand, users test it explicitly: `if x is null then null else 'id: ' + x`.
- The stdlib's `s.concat(other)` still prints SQL `||` and propagates NULL; aligning it is a follow-up.
- A `+` whose operand types the Typer leaves unresolved is not rewritten, as before.
