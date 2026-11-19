# Prove SQL SELECT-block folding in Lean, and fix the folds it rejects

## Goals

- Show, for every pipeline of relational operators, that `SqlGenerator`'s choice to fold an operator into the current SELECT block or to open a subquery gives the same rows as running the pipeline step by step.
- Fix the folds that change results today. The proof is the guard against new ones.
- Keep the model tied to the shipped code with a Lean-exported fixture that a Scala test replays, as the activity, typer, and model-build models already do.

## Background

- `SqlGenerator.relation` walks the plan from the last operator down to the source. `SQLBlock` collects clauses from operators applied *later*; an earlier operator joins that block when its `accept*` check passes, otherwise it becomes a subquery.
- The checks only ask whether the operator's own clause slot is empty (`acceptLimit` also rejects an existing ORDER BY). They don't ask whether the block holds a later clause that SQL evaluates *before* that slot. SQL evaluates WHERE, GROUP BY, HAVING, SELECT, DISTINCT, ORDER BY, OFFSET, LIMIT in that order.
- Mis-folds confirmed with the current CLI (`wvlet compile`); ✗ marks changed results or invalid SQL:
  - `limit 3 | where x > 0` → `where x > 0 limit 3` ✗ (filters before limiting)
  - `limit 3 | group by k` → `group by k limit 3` ✗
  - `limit 3 | select distinct a` → `select distinct a … limit 3` ✗
  - `select distinct a | select b` → `select distinct b from (select a …)` ✗ (DISTINCT moves to the outer projection)
  - `group by k select k, _.count as c | where c > 1` → `where c > 1 group by k` ✗ (aggregate in WHERE)
  - `limit 3 | agg x.sum` → `select sum(x) from t limit 3` ✗ (global aggregation folds over the limit)
  - `count | limit 0` → `select count(*) from (select * from t limit 0)` ✗. `Count` hands its outer block to its child, so later clauses land under the count
  - `dedup | select a` → `select distinct a from (…)` ✗. `Dedup` marks the outer block DISTINCT
  - `order by x | group by k` and `order by x | select distinct a` → ORDER BY on a column the block no longer has ✗ (invalid SQL)
  - `select a as b | where b > 0` → `select a as b from t where b > 0` ✗. WHERE resolves `b` to the *input* column: DuckDB returns 0 rows instead of 2 when `t` has its own `b`, and Trino rejects it when it doesn't. The same capture hits `order by b | select a as b`.
- `offset` exists only in SQL input; WvletParser has no `offset` syntax. `SqlParser` builds `Offset(Limit(…))` for `LIMIT l OFFSET o`, which read as a pipeline means "take l, then skip o". It renders correctly only because the generator folds it back. `acceptOffset` then refuses an offset under a limit, the order a pipeline reading needs.
- SQL doesn't keep a subquery's ORDER BY unless LIMIT or OFFSET sits in the same block (Trino drops it).
- `group by k | group by k` used to emit `select * … group by k` for the inner block (invalid SQL), because a nested GroupBy lost its default select items. The nested path now goes through the same GroupBy rendering.

## Design

Lane: Standard — changes the SQL generated for existing queries, and the scope of the fixes needs your call.

- **Lean model** (`formal/WvletFormal/SqlFold/`, its own `lake exe sql-fold-traces` and fixture, separate from ModelBuild):
  - Operators: filter, project, distinct-project, dedup, aggregation (an opaque `List Row → List Row`), sort by key, limit, offset. A filter directly over a group by is HAVING, as in the generator.
  - Plan nodes map onto them as follows. `Agg` over a `GroupBy`, a global `Agg`, a bare `GroupBy`, and `Count` are each an aggregation followed by a projection. `Transform`, `Exclude`, and the other column-level `Selection`s are projections. Pivot, joins, set operations, and other nodes stay outside the model and always start a block.
  - Predicates, projections, and keys are opaque functions over one row type. NULLs and 3VL are out of scope.
  - One block is evaluated in SQL clause order; ORDER BY sees the projected row, as SQL resolves output aliases first.
  - Nested blocks are evaluated as subqueries that keep their row order. SQL may drop a subquery's ORDER BY without LIMIT/OFFSET; `fold_sound` states this assumption. Pipelines that rely on it (`order by x | select a | select b`) are a known gap, and the fix (hoisting ORDER BY outward) is a follow-up.
  - Alias scope is a flag on each filter or sort key: "names no column that the adjacent projection computes or renames". A name the projection doesn't output still resolves to the input column, so SQL input like `select a as b from t order by a` stays flat. The theorem assumes the flag is truthful (`p ∘ f = p`); Scala computes it from column names.
  - `fold` mirrors the generator: walk from the last operator, merge when `accepts block op`, else open a nested block.
- **Theorems**:
  - `fold_sound`: evaluating the nested blocks equals evaluating the pipeline, for every pipeline and source.
  - Necessity: for each rule that refuses a fold, a concrete counterexample where merging changes the rows. Rules without one are recorded as merely conservative.
- **Corrected rules**. Aggregation is treated like GROUP BY. Count and Dedup go through the same rules instead of passing the outer block inward. An operator joins the block only if no later clause in it sits in an earlier SQL slot, unless they commute):
  - LIMIT: no WHERE, GROUP BY, HAVING, DISTINCT, ORDER BY, OFFSET, or LIMIT in the block.
  - OFFSET: no WHERE, GROUP BY, HAVING, DISTINCT, ORDER BY, or OFFSET. A LIMIT is allowed.
  - ORDER BY: no GROUP BY, DISTINCT, or ORDER BY; under a projection, only scope-safe keys.
  - GROUP BY: no WHERE in the block (a later filter must be HAVING or nested).
  - Projection: under a WHERE, only scope-safe predicates; never under a GROUP BY or HAVING. A later `dedup` is fine (`select a | dedup` becomes `select distinct a`). `select distinct` and `dedup` are refused when the block already has a projection or DISTINCT.
  - An aggregation's select list stays in the block of its GROUP BY (including a HAVING in between), so a selection directly over a GroupBy is folded as one aggregation.
  - Conservative but kept, to keep the SQL readable: `limit | limit`, `order by | order by`, `group by | group by`.
- **SqlParser**: build `Limit(Offset(…))` for `LIMIT l OFFSET o`, so plan nodes read as a pipeline. Generated SQL stays `limit l offset o`.
  - SQL→Wvlet output (`WvletGenerator` prints in node order) changes from `limit l offset o` to `offset o limit l`. Wvlet can't parse `offset` today either way; a codegen test pins the new order.
- **Scala refactor**: move the decisions into one pure `SQLBlock.accepts(op)`, which `relation` calls for every case (including Distinct and the Filter-over-Agg special case). The fix then lands in one place.
- **Conformance**:
  - The fixture lists every operator sequence up to length 4, with each scope-flag value, and the blocks `fold` produces.
  - A langJVM test replays it against `SQLBlock.accepts`.
  - The same test compiles the Wvlet sequences end to end and checks the number of SELECT blocks in the generated SQL.
  - The `Formal` workflow checks fixture freshness. README and ADR get a row and a paragraph.
- **Regression tests**: each mis-fold above is checked by its generated SQL and by its DuckDB result. The spec/sql suites guard SQL input such as ORDER BY on an unselected column.

## Alternatives and Why Not?

- **Prove and export today's rules without fixing them.** `fold_sound` would be false; we'd ship a fixture that pins known wrong SQL.
- **Always nest a filter or sort that crosses a projection.** Simpler, but it nests almost every `select … | where`; the name-based scope check keeps the common case flat.
- **Model SQL text or the full generator.** Too large and too dependent on the printer; block boundaries are the decision that carries the semantics.
