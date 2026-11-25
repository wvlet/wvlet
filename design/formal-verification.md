# Formal verification (Lean)

This page owns the rules that keep the Scala code in sync with the Lean models under `formal/`. What each model proves, its layout, and its known gaps are in [`formal/README.md`](../formal/README.md); why the project uses Lean with trace conformance is in [the ADR](../adr/2026-11-18-lean-formal-verification.md).

## How it works

- `formal/` is a standalone Lean 4 project, not part of sbt. Install [elan](https://github.com/leanprover/elan); `formal/lean-toolchain` pins the version.
- Each model has a `lake exe <name>-traces` executable that writes a JSON fixture. A Scala conformance test replays the fixture against the real code.
- `cd formal && lake build --wfail` checks every proof; warnings, including `sorry`, fail the build.
- The `Formal` workflow fails when a committed fixture is stale.

## When changing X, update the model first

For every row: update the Lean model first, regenerate the fixture with `cd formal && lake exe <traces>`, and run the conformance test.

| When changing | Model | Regenerate | Run |
| --- | --- | --- | --- |
| Activity state transitions (`ItemStore`, `TaskQueue`, `BatchExecutor.recoverItems`) | `WvletFormal/Activity/` | `activity-traces` | `./sbt "runnerJVM/testOnly *ActivityModelConformanceTest"` |
| `unify`, `findCommonType`, `canCoerce`, or `widen` in `TypeInference` | `WvletFormal/Typer/Coercion.lean` | `typer-traces` | `./sbt "langJVM/testOnly *TypeCoercionConformanceTest"` |
| `arithmeticType`, `equalityType`, `comparisonType`, or `logicalType` in `TyperRules` | `WvletFormal/Typer/Operators.lean` | `operator-traces` | `./sbt "langJVM/testOnly *OperatorTypingConformanceTest"` |
| `ModelOrder` (edges through ephemeral models, selectors, `buildOrder`, `findCycle`) | `WvletFormal/ModelBuild/` | `model-build-traces` | `./sbt "langJVM/testOnly *ModelBuildConformanceTest"` |
| `SQLBlock.accepts`, or which plan node maps to which `FoldOp` in `SqlGenerator.relation` | `WvletFormal/SqlFold/` | `sql-fold-traces` | `./sbt "langJVM/testOnly *SqlFoldConformanceTest"` |
| A rule in `RewriteExpr` | `WvletFormal/Rewrite/` | `rewrite-expr-traces` | `./sbt "langJVM/testOnly *RewriteExprConformanceTest"` |
| How a name resolves (`Scope` add/enter, `GlobalSymbolIndex.visibleEntries` or `findFirstAnywhere`, `Context.findSymbolByName`, the identifier rule in `TyperRules`, `RelationRefResolver.resolveTableRef`) | `WvletFormal/NameLookup/` and [the name-lookup ADR](../adr/2026-11-18-name-lookup-precedence.md) | `name-lookup-traces` | `./sbt "langJVM/testOnly *NameLookupConformanceTest"` |
| Where the engine's `PushDownFilters` moves a conjunct over a join (the `Join` case of its walk), and which LEFT joins it turns into inner joins (`outer_to_inner`, `rejectsNull`, the `Join` and `InnerJoins` cases) | `WvletFormal/Pushdown/` | `pushdown-filter-traces` | `./sbt "engineJVM/testOnly *PushDownFiltersConformanceTest"` |
| How the engine binds NOT IN (the NOT IN conjunct in `RuntimeBinder`, the null-aware residual in `Planner.correlatedJoin`, `Binder.bindIn` over a subquery, `InList`) | `WvletFormal/NotIn/` | `not-in-traces` | `./sbt "engineJVM/testOnly *NotInConformanceTest"` |
| When `ReorderJoins` moves a LEFT join above the inner joins of its preserved input (`commute_left_joins`, in `ReorderJoins.order`) | `WvletFormal/LeftJoinCommute/` | `left-join-commute-traces` | `./sbt "engineJVM/testOnly *LeftJoinCommuteConformanceTest"` |
| Which joins carry conjuncts across their keys in `PushDownFilters` (`join_key_filters`), and which join inputs take runtime join filters (`Lowering.filteredProbe`, `runtime_join_filters` / `row_join_filters`) and which nested join inputs below the probe side pass them on (`Lowering.passesFilter`, `build_side_join_filters`; proved in `Probe.lean`, checked by a table test in `EngineRuntimeJoinFiltersTest`, not replayed from the fixture) | `WvletFormal/JoinKeyFilter/` | `join-key-filter-traces` | `./sbt "engineJVM/testOnly *JoinKeyFilterConformanceTest"` |
| `FactorOr` (`Normalize`), the filters an `or` implies on inner join inputs (`or_implied_filters` in `PushDownFilters`), or a `VExpr.sig` they match conjuncts by | `WvletFormal/FactorOr/` | `factor-or-traces` | `./sbt "engineJVM/testOnly *FactorOrConformanceTest"` |
| How the engine decorrelates EXISTS / IN (`Planner.correlatedJoin` for EXISTS and IN, `planCorrelated`'s conjunct split, `Planner.decorrelateExists`) | `WvletFormal/Decorrelate/` | `decorrelate-traces` | `./sbt "engineJVM/testOnly *DecorrelateConformanceTest"` |
| How the engine decorrelates a correlated scalar subquery (`Planner.decorrelateScalar`, `emptyGroupValue`) | `WvletFormal/Scalar/` | `scalar-subquery-traces` | `./sbt "engineJVM/testOnly *ScalarSubqueryConformanceTest"` |
| Where the engine reduces inner joins to semi joins (`ReduceSemiJoins`, its duplicate-insensitive contexts, or the shared CTE readers it trusts) | `WvletFormal/SemiReduce/` | `semi-reduce-traces` | `./sbt "engineJVM/testOnly *SemiReduceConformanceTest"` |
| Which self semi joins on a key with a different value become `group by k having min(x) <> max(x)` (`SelfJoinAggregate`: the matched shape, NULL handling, the keys-only form in a duplicate-insensitive context) | `WvletFormal/SelfJoinAggregate/` | `self-join-aggregate-traces` | `./sbt "engineJVM/testOnly *SelfJoinAggregateConformanceTest"` |

## Cross-model rules

- Conditions with the same `VExpr.sig` must have the same value; `FactorOr` depends on it.
- The decorrelate fixture also replays the pushdown model's semi and anti join cases. A change to how `PushDownFilters` moves a conjunct over a semi or anti join also runs `lake exe decorrelate-traces` and `DecorrelateConformanceTest`.
- New 3VL-sensitive engine rules build their models on `formal/WvletFormal/Algebra/` (bag algebra with SQL three-valued logic).
