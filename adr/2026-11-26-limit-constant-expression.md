# ADR: Fold constant limit expressions at model expansion, keeping Limit a literal

Date: 2026-11-26 · PR #2533

## Context

- `limit` accepted only an integer literal, so `def top(n: int) = order by revenue desc | limit n` and `model m(n: int) = ... limit n` failed to parse.
- `Limit.limit` is a `LongLiteral`. The engine (`RuntimeBinder`, `Stats`, `RankFilterRows`, `PlanValidator`), the GPU accelerator, and the SQL generators read it as a number.
- Vals, model arguments, and for-loop variables are bound only in `GenSQL.expand` (`substituteContextBindings`, `expandModelScan`), which both the SQL and the engine paths run. `def` parameters are substituted earlier, by `FunctionInliner`.

## Decision

- The parser builds `Limit` for an integer literal and `UnresolvedLimit(child, expr)` for any other expression.
- `GenSQL.expandRelation` folds every `UnresolvedLimit` into `Limit` after binding: integer literals, `+ - *` with overflow checks, `%`, and `/` only when it divides evenly.
- Anything else fails with `INVALID_ARGUMENT` ("limit requires a constant non-negative integer"): a column, a non-integer, a negative count, an uneven division, or an overflow.

## Consequences

- No dialect or engine code changed: plans past expansion still carry a literal row count, and the generated SQL is `LIMIT <integer>`.
- Changing `Limit.limit` to `Expression` was rejected because it would move the "is it a number" check into every engine and GPU reader.
- Folding in the parser or typer was rejected because model arguments aren't known until expansion.
- `/` is not folded as integer division, because engines disagree on what `7 / 2` means; an even division is unambiguous.
- `sample` remains literal-only: its `sample <method>(n)` form starts with an identifier, so a bare `sample n` would need its own disambiguation.
