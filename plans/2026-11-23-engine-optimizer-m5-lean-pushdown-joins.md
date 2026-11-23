# Lean bag algebra with 3VL, and filter pushdown through joins

## Goals

- Start the M5 entry "Lean bag-algebra core with 3VL; models + trace conformance" with a shared core that later rules (NOT IN → anti join, OR factoring, EXISTS/IN decorrelation) reuse.
- Prove the first NULL-sensitive engine rule on it: where `PushDownFilters` moves a WHERE conjunct that sits over a join.
- Replay the model's decisions and its rows against the shipped engine, so the proof keeps describing the code.

## Background

- `PushDownFilters` (wvlet-engine runtime) moves a conjunct over a join as follows: into the preserved input of a LEFT or RIGHT join when it reads only that input; into the left input of a semi or anti join; into the condition list of an inner join tree; never below a FULL join. Everything else stays above the join.
- The NULL trap is the null-supplying side: below a LEFT join, `r.c is null` holds on padded rows, so pushing it into the right input changes the result. The engine never converts an outer join to an inner join on a NULL-rejecting conjunct, so there is no such rule to model yet.
- `formal/WvletFormal/Rewrite/Value.lean` already has SQL values and Kleene logic (`Tri`), written to be reused by optimizer models.
- Existing models follow one pattern (ADR 2026-11-18): a Lean model with proofs, a `lake exe <name>-traces` fixture, a Scala conformance test, and a freshness check in the `Formal` workflow.

## Design

- Lane: Express. A formal model plus a test; no engine behavior change unless the model finds a bug, in which case the fix stays minimal and the lane is re-assessed.
- New `formal/WvletFormal/Algebra/` core:
  - rows as maps from column ids to SQL values (`Rewrite.Value`), relations as lists (bags; results compared up to order);
  - conditions over rows evaluated in 3VL (`Rewrite.Tri`): comparisons, `is null`, `not`, `and`, `or`; a filter keeps only rows whose condition is TRUE;
  - joins: inner, left, right, full, semi, anti, each with a general 3VL ON condition (equality keys are a special case), and NULL padding for outer joins.
- New `formal/WvletFormal/Pushdown/` rule model:
  - `place`: the join kind and which inputs a conjunct reads decide where it goes (left input, right input, the inner join's conditions, or above), mirroring `PushDownFilters`. A conjunct that reads no column counts as reading only the preserved input, as in the pass.
  - Soundness: every placement keeps the rows, for every input and ON condition, NULLs included.
  - Necessity: for each refusal (null-supplying side of LEFT/RIGHT, either side of FULL), a counterexample where pushing anyway changes the rows.
- Fixture (`lake exe pushdown-filter-traces`): join kinds inner/left/right/full × conjuncts over the left input, the right input, both, and NULL tests (`is null`, `is not null`, comparisons, `or`), over small inline tables with NULL keys and values. Each case gives the model's placement and the result rows.
- `PushDownFiltersConformanceTest` (engineJVM) writes each case as a Wvlet query over inline values, runs `Normalize` and `PushDownFilters` on the bound plan to check the placement, and runs the query to compare rows with the model's.
- Out of scope, named in `formal/README.md`: the equalities to constants the pass infers inside inner join trees (the placement check tolerates the extra conditions); the hash keys it extracts from outer join ON conditions, which the model treats as part of a general ON condition (the row replay still checks that NULL keys never match); right semi/anti joins, the `Join` nodes `ReorderJoins` builds, and ASOF joins.
- The test runs `Normalize` before `PushDownFilters`, as the optimizer does, so conjuncts reach the pass in the shape the pipeline gives them.
- This departs from the framework plan's `(input plan, rewritten plan)` traces replayed by one `LogicalRuleConformanceTest`: placement plus rows per rule is what the pass can be checked against without hand-built plans. The plan's "Lean coverage" bullet is amended to say so.
- Semi and anti joins are proved in Lean but replayed later, with the EXISTS/IN decorrelation model, since queries reach them only through subqueries.
- Updates: CLAUDE.md formal section, `formal/README.md`, the `Formal` workflow (freshness step and paths), and the M5 item's list with the remaining rules.

## Alternatives and Why Not?

- Hand-built `RuntimePlan`s in the conformance test: closer to the pass, but couples the test to plan ids and binder internals; queries over inline values also check join execution under NULLs.
- Model NOT IN first: it needs the same core plus null-aware anti join semantics; pushdown is the smaller first rule that exercises every join kind.
- Put the conformance test in wvlet-lang: the rule lives in wvlet-engine, which wvlet-lang cannot depend on.
