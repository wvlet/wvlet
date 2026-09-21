# ADR: `for` loops iterate at run time, not by compile-time unrolling

Date: 2026-09-20 · PR #2073 · Design note: `plans/2026-09-20-for-loop-syntax.md`

## Context

Wvlet had no way to repeat side-effecting statements (`save to`, `append to`, DDL) over a list of tables, dates, or tenants; a `model` with arguments only parameterizes a single relation. `for x in <iterable> { statements }` fills that gap. The open question was whether a loop is a macro that expands into N copies of its body at compile time, or a plan node the executor iterates.

## Decision

- **Runtime loop, typed once.** `ForLoop` stays a single plan node; the Typer types the body once with the loop variable at the iterable's element type, the planner wraps the body plan in `ExecuteFor`, and the executor runs that plan once per value. Unrolling was rejected because it cannot express query-driven iterables (`for p in (from partitions select name)`), whose values exist only at run time, and it multiplies typing work and diagnostics by N.
- **The loop variable is a `val` binding in a per-iteration child `Context`** (`GenSQL.loopIterationContext`). GenSQL already substitutes context-bound identifiers and evaluates backquote interpolation per region using the `Context` outer chain, so loops needed no new substitution mechanism — an iteration is one more frame, the same shape as a model-argument frame. Vals defined in the body enter the iteration context and do not leak.
- **The variable is bound with `scope.add`, never `Context.enter`.** `Scope.enter` keeps an existing same-named symbol, and child scopes snapshot outer entries, so `enter` silently binds the outer `val` instead of the loop value (found in review). Any future construct that introduces a shadowing local binding must do the same.
- **Query iterables bind the first column only**, run once before the first iteration, and are rejected when the result was truncated by the runner's row limit — iterating a silently truncated list is worse than failing. Row/struct binding is deferred.
- **Declarations in a loop body compile once**, like top-level ones (the SymbolLabeler labels body statements with the enclosing context). The body has no statement restrictions.
- **`wvlet compile` unrolls only constant iterables** (array literals and vals holding them, via `GenSQL.loopArrayValues`, shared with the executor). A query-driven loop emits a warning and no SQL, since there is no engine to ask.
- **Save targets evaluate backquote interpolation** (`` save to s`tbl_${d}` ``) through the same `contextBindingRule` as relation bodies. This was unsupported before and is the main loop use case.

## Consequences

- A loop costs one typing pass regardless of iteration count, and tables named through interpolation stay unresolved at compile time, exactly as inside models.
- Iterations are sequential and fail-fast; a 10,000-iteration cap (`LOOP_LIMIT_EXCEEDED`) guards against unbounded engine queries. Parallel iteration and loops inside `flow` are separate designs.
- Top-level `val`s are entered as global symbols visible across compilation units, so a spec-file `val v` can capture a column named `v` in another spec. Loop variables avoid this by being context-scoped; spec files should keep top-level val names unique (`for_loop_ids`, not `ids`).
- `RunnerSpecNeg` does not assert that an error occurred, so `spec/neg/for-loop-non-array.wv` documents the behavior but would not catch the error disappearing.
