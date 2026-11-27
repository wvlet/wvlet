# Constant expressions in `limit`

## Goals

- Let `limit` take a parameter or val, so `def top(n: int) = order by revenue desc | limit n`, `model m(n: int) = ... | limit n`, and `val n = 10 ... | limit n` compile.
- Keep the generated SQL a plain integer literal for every dialect, and keep the engine's `Limit` a literal.
- Report a clear error when the value is not a constant non-negative integer.

## Background

- `WvletParser.limitExpr` consumes only `INTEGER_LITERAL`, and `Limit.limit` is a `LongLiteral` that the engine (`RuntimeBinder`, `Stats`, `RankFilterRows`, GPU) reads as a number.
- Vals, model arguments, and for-loop variables are bound at expansion time: `GenSQL.expand` replaces their identifiers with the bound expressions (`substituteContextBindings`) for both the SQL and the engine paths.
- `def` parameters are substituted by `FunctionInliner` when the partial query is inlined, also before expansion finishes.
- Wvlet has no `offset` operator (only SQL `OFFSET`), so only `limit` and `sample` have the literal-only restriction.

## Design

- Lane: Standard (syntax change), direction given in the task prompt, so it proceeds without a design card; the user asked not to merge.
- The parser keeps producing `Limit` for an integer literal; any other expression becomes a new `UnresolvedLimit(child, expr)` node.
- `GenSQL.expand` folds `UnresolvedLimit` into `Limit` after binding, evaluating integer literals and `+ - * / %` over them; everything downstream (SQL generators, engine, GPU) still sees only literal limits.
- A non-constant, non-integer, or negative value fails with `INVALID_ARGUMENT`: "limit requires a constant non-negative integer, but got: <expr>", pointing at the limit.
- `sample` stays literal-only in this PR (its `sample <method>(n)` form overlaps with an identifier argument); noted as a follow-up.
- The formatter prints `limit <expr>` back as written.
- Tests: parser and SQL-generation tests for def, model, and val forms; an error test; a spec/basic case; an engine-path conformance check.
- Docs: syntax pages, `for-ai-agents.md`, and the bundled skill if they describe limit's argument; restore `def top(n: int)` in the playground macros example.

## Alternatives and Why Not?

- Change `Limit.limit` to `Expression`: every engine and GPU reader would need to handle a non-literal, losing the guarantee that plans carry a number.
- Fold in the parser or typer only: model arguments are not known until expansion, so `model m(n: int)` would still fail.
