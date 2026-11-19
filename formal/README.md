# Formal models (Lean 4)

Machine-checked models of Wvlet behavior where example-based tests sample poorly:

- The activity item state machine: the fenced `ItemStore` operations, `TaskQueue` restarts and
  lease expiry, and `BatchExecutor` restart reconciliation.
- The compiler's common-type and coercion rules (`TypeInference` in wvlet-lang): `unify`,
  `findCommonType`, `canCoerce`, and `widen`.
- `wvlet build` ordering (`ModelOrder` in wvlet-lang): edges through ephemeral models, selector
  closures, the build order, and cycle detection.
- SQL generation's SELECT-block folding (`SQLBlock.accepts` in `SqlGenerator`): when a pipeline
  operator can join the current SELECT block, and when it must open a subquery.

See
[adr/2026-11-18-lean-formal-verification.md](../adr/2026-11-18-lean-formal-verification.md) for
the reasoning behind this setup.

## Setup

Install [elan](https://github.com/leanprover/elan) (the Lean toolchain manager). `lean-toolchain`
pins the Lean version, and elan downloads it on first use. The project has no dependencies
(no Mathlib), so a full build takes seconds.

```bash
cd formal
lake build --wfail          # check every proof; warnings (including `sorry`) are errors
lake exe activity-traces    # regenerate the activity conformance fixture
lake exe typer-traces       # regenerate the typer conformance fixture
lake exe model-build-traces # regenerate the model build conformance fixture
lake exe sql-fold-traces    # regenerate the SQL block folding conformance fixture
```

## Layout

| File | Models | Main theorems |
|------|--------|---------------|
| `WvletFormal/Activity/ItemState.lean` | `ItemState`, result outcomes, the item record | final ⇒ terminal |
| `WvletFormal/Activity/Store.lean` | `claimItem`, `markCancelling`, `reportResult` | stale attempts and final items are never changed; attempts are never changed by the store; the transition table (`StoreEdge`) |
| `WvletFormal/Activity/Protocol.lean` | new attempts, lease expiry, `TaskQueue.recover`, `BatchExecutor.recoverItems` | a restart never re-executes an in-flight attempt; reruns need their policy flag; `claimed_nodup`: no attempt is ever claimed twice, under any interleaving |
| `WvletFormal/Activity/Traces.lean`, `Main.lean` | — | exports the activity conformance fixture |
| `WvletFormal/Typer/Coercion.lean` | `unify`, `findCommonType`, `canCoerce`, `widen` over a type domain where `other n` stands for every type no rule mentions | `unify` is the least upper bound of a widening order (`unify_isLub`), hence commutative, associative, and idempotent, with NoType as identity and `error` absorbing; `findCommonType_eq_foldl`: on NoType-free lists, `findCommonType` is a fold of `unify`, so `CASE` typing ignores branch order; `unify_upper_canCoerce` / `unify_least_canCoerce`: the result relates to `canCoerce` as an upper bound, and as the least one once the string-concatenation rule is excluded |
| `WvletFormal/Typer/Traces.lean`, `TyperTraces.lean` | — | exports the typer conformance fixture |
| `WvletFormal/ModelBuild/Explore.lean` | the work-list search shared by `closure` and `upstreamsOf` | `mem_explore_iff`: the search visits exactly the nodes reachable from its work list, once each (`explore_nodup`); termination is the `termination_by` measure (new nodes visited, then work-list length) |
| `WvletFormal/ModelBuild/Graph.lean` | `upstreamsOf`, `closure`, `parseSelectors`, `select` | `mem_upstreamsOf`: edges are the materialized models reachable through ephemeral models only, self-edges included; `mem_closure_ups` / `mem_closure_downs`: closures are transitive upstream / downstream reachability; `selectParsed_spec`: `select` returns exactly the models the selectors select, in definition order, without duplicates |
| `WvletFormal/ModelBuild/Order.lean` | `buildOrder` rounds, `findCycle` | `buildOrder_spec`: the order is a permutation of the selection in which every model follows the selected models it reads, and it fails exactly when the selection has a cycle, reporting a real one; `rounds_level`: every model after the first round reads a selected model of the round just before it (so no model waits longer than its upstream models require), and `rounds_sublist`: each round keeps input order; `findCycle_spec`: a cycle is reported exactly when one exists; `buildOrder_ok_of_acyclic`: after `findCycle` passes, every selection orders |
| `WvletFormal/ModelBuild/Traces.lean`, `ModelBuildTraces.lean` | — | exports the model build conformance fixture |
| `WvletFormal/SqlFold/Model.lean` | pipeline operators over a list of rows (filter, projection, `select distinct`, `dedup`, group by, aggregation, sort, limit, offset); one SELECT block in SQL clause order; `accepts` and `fold`, which mirror `SQLBlock.accepts` and `SqlGenerator.relation` | — |
| `WvletFormal/SqlFold/Sound.lean` | — | `fold_sound`: evaluating the nested blocks `fold` builds gives exactly the rows of running the pipeline, provided the scope flags it relies on are truthful (`Scoped`); `merge_sound`: each accepted operator's clause computes the operator; `fold_shapes`: the exported shape-level fold decides as `fold` does; filters commute with sorting (`filter_sortBy`) and duplicate elimination (`filter_dedup`) |
| `WvletFormal/SqlFold/Necessity.lean` | — | for each refusal that guards rows (LIMIT or OFFSET under WHERE, GROUP BY, HAVING, DISTINCT, ORDER BY; LIMIT under OFFSET; ORDER BY under GROUP BY or a renaming projection; a projection under a filter it renames or a grouping; `dedup` under a projection; a grouping under WHERE), a block and rows where merging anyway changes the result |
| `WvletFormal/SqlFold/Traces.lean`, `SqlFoldTraces.lean` | — | exports the SQL block folding conformance fixture |

## Keeping the model and the code in sync

A proof only describes the shipped code while the two agree, so each model exports its expected
behavior as traces:

- `lake exe activity-traces` writes
  `wvlet-runner/.jvm/src/test/resources/wvlet/lang/runner/activity/activity-traces.json`.
- `ActivityModelConformanceTest` (sbt, no Lean needed) replays every trace against
  `InMemoryItemStore`, `SQLiteItemStore`, `TaskQueue`, and `BatchExecutor.recoverItems`.
- `lake exe typer-traces` writes
  `wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/typer/typer-traces.json`: every pair of
  the domain for `unify`, `canCoerce`, and `widen`, and every list of up to three types for
  `findCommonType`. `TypeCoercionConformanceTest` (langJVM) replays it against `TypeInference`,
  binding each `other n` to a concrete type (short, date, two decimals, varchar) and comparing errors
  by error-ness, since the model abstracts error messages.
- `lake exe model-build-traces` writes
  `wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/pipeline/model-build-traces.json`: every
  graph on three models (512, self-loops included) with its cycle, the build order of every
  selection, and every single selector; selector parsing edge cases; and edge derivation over
  every reference set of two materialized models and one ephemeral model, plus longer ephemeral
  chains. `ModelBuildConformanceTest` (langJVM) replays it against `ModelOrder`.
- `lake exe sql-fold-traces` writes
  `wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/codegen/sql-fold-traces.json`: every
  block shape folding can reach (384) with the operators it accepts, and every pipeline of up to
  three operators with its blocks. `SqlFoldConformanceTest` (langJVM) replays the decisions
  against `SQLBlock.accepts`, and compiles each pipeline that Wvlet can write one node per operator
  to check its number of SELECT blocks.
- The `Formal` workflow checks the proofs and fails when a committed fixture is stale.

When you change an activity state transition, a coercion rule in `TypeInference`, the build
ordering in `ModelOrder`, or a folding rule in `SQLBlock.accepts`, update the model first, fix the proofs, regenerate the fixture, and then change the Scala code until the
conformance test passes.

## Known gaps of the typer model

The model proves today's behavior. These are left for follow-ups:

- Type variables are out of scope: `unify(TypeVar a, TypeVar b)` returns `b`, so it is not
  commutative on type variables.
- Decimals have no common type with other numerics or across precisions, unlike arithmetic typing
  (`decimal + int` is a decimal). Short, byte, real, varchar, and char have no rule either.
- The arithmetic result table in `TyperRules` is not yet tied to `unify`.

## Known gaps of the SQL block folding model

- Subqueries are assumed to keep their row order. SQL may drop the ORDER BY of a subquery that has
  no LIMIT or OFFSET (Trino does), so a pipeline whose sort ends up in a nested block can lose its
  order: `order by x | select a | select b` sorts the inner block only. Hoisting such an ORDER BY
  to the outer block is a follow-up.
- Expressions are opaque. The scope flag stands for the name check in `SqlGenerator.scopeSafe`,
  which the model trusts; NULLs and three-valued logic are out of scope.
- A grouping is an opaque function of its input rows, and an aggregation includes its select
  list. The model does not check that SQL accepts a select list over a GROUP BY.
- Operators outside the model (joins, set operations, pivot, sampling, window functions) always
  start a SELECT block of their own.

## Known gaps of the model build model

- Selector parsing (comma splitting, trimming, `+` stripping) is replayed by the fixture but not
  proved; the proofs start from parsed selectors.
- Models are names: the model trusts that `ModelBuildPlanner.graph` extracts each model's direct
  references from its plan. A cycle made only of ephemeral models is left to SQL generation, which
  reports it as a recursive model reference.
