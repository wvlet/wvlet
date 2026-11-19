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
- The expression rewrite rules (`RewriteExpr` in wvlet-lang): string `+`, `s"..."`
  interpolation, and `if(...)`, over SQL values with NULL and three-valued conditions.

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
lake exe rewrite-expr-traces # regenerate the expression rewrite conformance fixture
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
| `WvletFormal/Rewrite/Value.lean` | SQL values with NULL, static types, `Tri` (Kleene three-valued logic, kept independent of the rules for reuse), `ite3` (`if` / `CASE WHEN`) | `Tri` laws (double negation, commutativity, associativity, De Morgan) |
| `WvletFormal/Rewrite/Expr.lean` | the expression shapes `RewriteExpr` touches, their Typer types, and one evaluator: Wvlet meaning for string `+`, `s"..."`, and the `if(...)` call; engine meaning for `concat`, `coalesce`, `IfExpr`, and `cast`, where an `Engine` says whether `concat` skips NULLs (DuckDB) or returns NULL (Trino, Snowflake, Hive), and whether it accepts non-strings | — |
| `WvletFormal/Rewrite/Rules.lean` | `RewriteStringConcat`, `RewriteStringInterpolation`, `RewriteIfExpr`, the bottom-up pass, the phase, and the rules as they shipped before (`Legacy`) | — |
| `WvletFormal/Rewrite/Sound.lean` | — | `eval_fits`: values fit their static types; `nonNull_sound`: what the rules treat as never NULL is never NULL; `concatRule_sound`, `interpRule_sound`, `ifRule_sound`: each rule keeps the type and the value of every expression on every engine (for `if`, every engine with an `if` function), NULL inputs included; `phase_eval`: so does the whole phase |
| `WvletFormal/Rewrite/Idem.lean` | — | `phase_clean`: no string `+`, `s"..."`, or `if(...)` call is left; `phase_idem`: the phase is idempotent, so rewriting a nested query once per enclosing query is harmless |
| `WvletFormal/Rewrite/Findings.lean` | — | what the old rules got wrong: a NULL operand made `concat` NULL on Trino but not DuckDB (`f1_*`), no cast for a non-string operand (`f2_no_cast`), a string `+` with the string on the right was never rewritten (`f3_right_string`), and `s"${x}"` stayed `x` (`f4_single_part`) |
| `WvletFormal/Rewrite/Traces.lean`, `RewriteExprTraces.lean` | — | exports the expression rewrite conformance fixture |

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
- `lake exe rewrite-expr-traces` writes
  `wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/transform/rewrite-expr-traces.json`:
  `+` over every pair of atoms (literals of each type, NULL, and typed columns) and nested `+`,
  `s"..."` with up to three parts, `if(...)` with two and three arguments, and the rules inside
  each other, each with its rewritten expression and its DuckDB value on 12 rows covering NULL and
  non-NULL columns. `RewriteExprConformanceTest` (langJVM) rewrites each input with the shipped
  rules and compares the result node by node, types included; checks that a second run changes
  nothing; and runs the rewritten SQL on DuckDB to compare values with the model.
- The `Formal` workflow checks the proofs and fails when a committed fixture is stale.

When you change an activity state transition, a coercion rule in `TypeInference`, the build
ordering in `ModelOrder`, a folding rule in `SQLBlock.accepts`, or a rule in `RewriteExpr`, update the model first, fix the proofs, regenerate the fixture, and then change the Scala code until the
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

## Known gaps of the expression rewrite model

- Function names are lower-case strings in the model; Scala matches `if` case-insensitively, which
  `RewriteExprConformanceTest` checks separately.
- `cast(v as varchar)` renders integers and booleans the same way on every engine (`'7'`,
  `'true'`); the DuckDB replay checks this only for DuckDB.
- Snowflake has `IFF`, not `IF`, and SQL generation prints `IfExpr` as `if(...)` everywhere, so
  `ifRule_sound` assumes an engine with an `if` function.
- The rules trust the Typer's types. A `+` whose operand types the Typer leaves unresolved (e.g.
  columns of some inline tables) is not rewritten and reaches SQL as `+`.
- The stdlib's `s.concat(other)` prints SQL `||`, which is NULL-propagating; it is a function call,
  not a rewrite rule, so it is out of scope.
