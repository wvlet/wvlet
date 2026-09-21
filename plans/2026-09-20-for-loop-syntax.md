# For-loop statement

Status: implemented · PR #2073 · ADR: `adr/2026-09-20-for-loop-runtime-iteration.md`

## Goals

- Answer "does Wvlet have a for-loop?" — **no**. `for` is already a reserved keyword
  (`WvletToken.FOR`), but its only use is inside `unpivot <value> for <col> in (...)`
  (`WvletParser.unpivotExpr`). There is no statement- or expression-level iteration.
- Add a statement-level `for` loop so a `.wv` file can repeat a block of statements over a small
  list of values — per-date backfills, per-table maintenance, per-tenant exports — without
  copy-pasting the block or generating `.wv` files from a shell script.
- Settle the syntax and the execution semantics (runtime loop vs. compile-time unrolling) before
  writing code.

## Background

- Row-level iteration is already covered and is *not* what this adds: `from`/`select` is a map,
  `unnest` flattens arrays, `fork`/`merge` fan out flow stages. A `for` loop over rows would
  duplicate relational operators and invite row-at-a-time thinking.
- What is missing is **statement repetition**. Today the only parameterization device is a
  `model` with arguments plus backquote interpolation
  (``from s`person_quote_ref_${id}` ``, `spec/basic/backquote-interpolation.wv`). A model yields
  one relation per call; it cannot repeat `save to` / `append to` / DDL, so N targets means N
  pasted blocks.
- The machinery a loop needs already exists:
  - `val` binds a name at run time: `ExecuteValDef` enters a `ValSymbolInfo` into the `Context`
    (`BasePlanExecutor.scala:125`).
  - `GenSQL.expand` substitutes context-bound identifiers and evaluates backquote
    interpolations per region, using the `Context` outer chain as a stack
    (`plans/2026-07-01-stack-based-model-expansion.md`). A loop iteration is one more frame on
    that chain — the same shape as a model-argument frame.
  - `ExecutionPlanner` already turns a statement list into `ExecuteTasks`; the executor walks it
    sequentially and threads `lastResult` for `test`.
- SQL spelling rule: `for` has no SQL statement meaning outside vendor procedural dialects
  (PL/pgSQL `FOR x IN ... LOOP`), so a novel block spelling carries no conflicting SQL semantics.

## Design

### Syntax

```wvlet
for d in ['2026-09-01', '2026-09-02', '2026-09-03'] {
  from s`events_${d}`
  where status = 'ok'
  group by user_id
  agg _.count as cnt
  add d as dt
  append to daily_summary
}
```

- `for <identifier> in <iterable> { <statements> }` — a statement, legal wherever `val` /
  `save to { }` is (top level of a file). Braces match the existing block statements
  (`save to x { }`, `reshape t { }`, `flow f { }`).
- `<iterable>` is either an **array-valued constant expression** (an array literal, or a `val`
  holding one: `val days = [...]`; `for d in days { }`) or a **parenthesized query**:
  `for p in (from partitions select name) { ... }`. A query iterable is executed once before the
  first iteration and the loop iterates the values of its **first column** in result order.
- The body is an ordinary statement list with no restrictions: queries, `save`/`append`/`delete`,
  DDL, `val`, `test`, nested `for`, and declarations. Declarations are compiled once (not per
  iteration) like their top-level counterparts.
- The loop variable is an immutable, body-scoped `val` of the array's element type. It can be used
  anywhere a `val` can: expressions, `s"..."`, `` s`...` `` table names, model arguments.
- No `break` / `continue` / `while`, no mutable accumulator. A loop is a bounded repetition, not
  general control flow.
- Parsing is unambiguous: `unpivot ... for` is only reachable inside `unpivotExpr`; a
  statement-head `FOR` is currently `unexpected(t)`.

### Semantics: runtime loop, typed once

- New plan node `ForLoop(variable: TermName, iterable: Expression, body: List[LogicalPlan])`
  (`LanguageStatement`, next to `ValDef` in `plan.scala`) and
  `ExecuteFor(loop: ForLoop, body: ExecutionPlan)` in `execution.scala`.
- **Compile time**: SymbolLabeler creates a symbol for the loop variable in a fresh scope owned by
  the loop (bound with `scope.add` so it shadows same-named outer symbols); Typer types the iterable, requires an array type when the type is known (new
  `StatusCode.INVALID_LOOP_ITERABLE` otherwise), and types the body **once** with the variable at
  the element type. Tables named through `` s`..${d}` `` stay unresolved at compile time, exactly
  as they do inside models today.
- **Planning**: `ExecutionPlanner` plans the body statements once with `evalQuery = true` (each
  body statement is a top-level statement of its iteration) and wraps them in `ExecuteFor`.
- **Run time**: the executor evaluates the iterable to literal elements; for each element it opens
  `ctx.newContext(loopSymbol)`, enters the variable as a `ValSymbolInfo` bound to that element, and
  `process`es the body plan under that context. `GenSQL` substitution then resolves the variable
  per iteration with no GenSQL change. A `val` defined in the body lives in the iteration's
  context and does not leak out.
- Iterations run **sequentially, in array order**. A failing statement aborts the loop with the
  usual error (no partial-failure collection). `test` in the body runs per iteration against that
  iteration's preceding query.
- Result of the statement: the results of all iterations, like `ExecuteTasks`.
- `wvlet compile` (SQL output without execution): emit each iteration's SQL in order, separated by
  `;`, since a constant iterable is fully known at compile time.
- Safety net: iteration count capped (default 10,000, `StatusCode.LOOP_LIMIT_EXCEEDED`) so a typo'd
  iterable can't issue unbounded engine queries.

### Out of scope for the first PR (follow-ups, called out so the syntax leaves room)

1. **Row/struct binding** for multi-column query iterables (`p.name`); the first PR binds the
   first column only.
2. **Ranges** — Wvlet has no range expression; `for i in range(1, 10)` should come from a stdlib
   `range` array function rather than new loop-specific syntax.
3. **Loops inside `flow`** as dynamic stage fan-out. Flow stages are a static DAG; this is a
   separate design.
4. **Parallel iterations.**

### Deliverables

- Parser + `ForLoop` node, WvletGenerator printing (round-trip), formatter/LSP keyword support
  where keywords are enumerated (highlighting grammars already know `for`).
- SymbolLabeler / Typer scope handling; TyperCoverageCheck must not regress.
- ExecutionPlanner + BasePlanExecutor `ExecuteFor`.
- Specs: `spec/basic/for-loop.wv` (literal iterable, val iterable, table-name interpolation with
  `save to`/`append to`, query iterable, nested loop, body-local val, empty array); `spec/neg/`
  for a non-array iterable.
- `save to` / `append to` targets evaluate backquote interpolation (`` save to s`tbl_${d}` ``),
  which was unsupported before and is the main loop use case.
- Docs: a "Loops" section in `website/docs/syntax/index.md` with the backfill example.

## Alternatives and Why Not?

### Compile-time unrolling (macro expansion into N copies of the body)

Simplest mental model and gives `wvlet compile` output for free, but it closes the door on
query-driven iterables — the most valuable follow-up (iterate over partitions discovered at run
time). It also multiplies typing work and error messages by N. The runtime loop handles constant
iterables just as well and reuses the existing `val`-binding path.

### Expression-level `for` comprehension (`for x in xs yield ...`)

Overlaps with `select`, `unnest`, and array lambda functions. Two ways to map rows would blur
Wvlet's flow-style identity; statement repetition is the actual gap.

### Flow-style pipe form: `from days | each d { ... }`

Reads naturally in Wvlet, but it makes a relation operator whose "output" is side effects rather
than a relation, which breaks the rule that every pipe step yields a relation. A statement keyword
keeps relations pure. (If query-driven iterables land, `for x in (from ...)` covers this need.)

### Do nothing: generate `.wv` files / loop in shell around `wvlet run`

Works today, but loses single-file reviewability, shared `val`/model definitions across
iterations, and per-iteration `test` assertions; each `wvlet run` also pays connection bootstrap
(~1.5 s for DuckDB JDBC).

## Decisions (2026-09-20)

1. Runtime loop — approved.
2. Query-driven iterables are part of the first PR.
3. No body restrictions: any statement may appear in a loop body.
