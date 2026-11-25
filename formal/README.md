# Formal models (Lean 4)

Machine-checked models of Wvlet behavior where example-based tests sample poorly:

- The activity item state machine: the fenced `ItemStore` operations, `TaskQueue` restarts and
  lease expiry, and `BatchExecutor` restart reconciliation.
- The compiler's common-type and coercion rules (`TypeInference` in wvlet-lang): `unify`,
  `findCommonType`, `canCoerce`, and `widen`.
- The typer's operator result types (`TyperRules` in wvlet-lang): sign prefixes, arithmetic,
  `=` / `!=`, ordering comparisons, and `and` / `or`.
- `wvlet build` ordering (`ModelOrder` in wvlet-lang): edges through ephemeral models, selector
  closures, the build order, and cycle detection.
- SQL generation's SELECT-block folding (`SQLBlock.accepts` in `SqlGenerator`): when a pipeline
  operator can join the current SELECT block, and when it must open a subquery.
- The expression rewrite rules (`RewriteExpr` in wvlet-lang): string `+`, `s"..."`
  interpolation, and `if(...)`, over SQL values with NULL and three-valued conditions.
- Name-lookup precedence in the Typer (`Scope`, `GlobalSymbolIndex`, `Context.findSymbolByName`,
  `TyperRules.identifierRules`, `RelationRefResolver.resolveTableRef`): which binding a bare
  identifier or a relation name resolves to. The spec itself is
  [adr/2026-11-18-name-lookup-precedence.md](../adr/2026-11-18-name-lookup-precedence.md).
- A bag algebra with SQL three-valued logic for the engine's optimizer rules, and the first rule
  on it: where `PushDownFilters` (wvlet-engine) moves a WHERE conjunct over a join, and when a
  NULL-rejecting conjunct turns a LEFT join into an inner join (`outer_to_inner`).
- NOT IN over a subquery as a null-aware anti join (wvlet-engine's `RuntimeBinder`,
  `Planner.correlatedJoin`, and the `InList` value set): each binding returns SQL's rows of
  `x NOT IN (subquery)`, NULLs on either side and an empty subquery included.
- Carrying filters across hash join keys (`join_key_filters` in the engine's `PushDownFilters`):
  which joins let a conjunct over one input's keys filter the other input, with three-valued
  conjuncts and NULL keys.
- OR factoring (wvlet-engine's `Normalize`: `FactorOr`): taking the conjuncts common to every
  branch out of an `or` keeps the condition's value under three-valued logic, provided conjuncts
  with the same signature have the same value.
  An `or` over several join inputs also implies, on each input, the `or` of each branch's
  conjuncts over that input alone (`or_implied_filters`); adding it beside the `or` keeps a
  WHERE's rows.
- EXISTS / IN decorrelation (wvlet-engine's `Planner.correlatedJoin` and
  `Planner.decorrelateExists`): the semi and anti joins of correlated `[NOT] EXISTS` and `IN`
  conjuncts, and the EXISTS mark (a left join with the distinct correlation keys) under OR and
  NOT, return SQL's rows, NULLs and duplicates included.
- Correlated scalar subqueries (wvlet-engine's `Planner.decorrelateScalar`): the body grouped by
  the correlation keys, left-joined to the outer input, with the value over no rows (`count` = 0)
  for the unmatched outer rows, gives every outer row SQL's value, NULL keys included.

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
lake exe operator-traces    # regenerate the operator typing conformance fixture
lake exe model-build-traces # regenerate the model build conformance fixture
lake exe sql-fold-traces    # regenerate the SQL block folding conformance fixture
lake exe rewrite-expr-traces # regenerate the expression rewrite conformance fixture
lake exe name-lookup-traces # regenerate the name lookup conformance fixture
lake exe left-join-commute-traces # regenerate the LEFT join commute conformance fixture
lake exe pushdown-filter-traces # regenerate the filter pushdown conformance fixture
lake exe not-in-traces      # regenerate the NOT IN conformance fixture
lake exe join-key-filter-traces # regenerate the join-key filter conformance fixture
lake exe factor-or-traces   # regenerate the OR factoring conformance fixture
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
| `WvletFormal/Typer/Operators.lean` | `arithmeticType`, `equalityType`, `comparisonType`, `logicalType` over the coercion model's `Ty` plus known and unresolved decimals, unresolved types, and function references | every operator ignores operand order (`arith_comm`, `compare_comm`, `logical_comm`); `arith_numeric`: on numeric and NULL operands arithmetic is `unify`, hence their least upper bound (`arith_numeric_isLub`); an error operand yields an error (`arith_error_left`, `compare_error_left`, `logical_error_left`, and their right-hand versions); `arith_noType_iff`: arithmetic is untyped exactly while an operand is untyped and none is an error (likewise `logical_deferred`), and a comparison is boolean or an error, never untyped (`compare_cases`); `arith_decimal`: a decimal result keeps the larger scale and, below 38 digits, the larger integer part |
| `WvletFormal/Typer/OperatorTraces.lean`, `OperatorTraces.lean` | — | exports the operator typing conformance fixture |
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
| `WvletFormal/NameLookup/Scope.lean` | `Scope.add`, `enter`, `newChildScope`, `newFreshScope`, `lookupSymbol`; scope programs over handles | `lookup_add_self`: the newest `add` wins; `lookup_add_ne` / `lookup_add_comm`: a binding never changes another name, so entry order of different names is irrelevant; `enter_bound`: `enter` never shadows; `lookup_child_add`: an inner `add` shadows the outer binding; `step_frame`: an operation changes only its own handle, so a child never sees later parent bindings |
| `WvletFormal/NameLookup/Global.lean` | `visibleEntries` (visible packages, buckets, stable sort by basename), `findFirstAnywhere` | `mem_visiblePackages` / `mem_entriesIn`: exactly presets, the default package, the own package, and every prefix of an import are visible; `visibleEntries_head`: the winner has the smallest basename; `visibleEntries_unique_min`: a strictly smallest basename wins whatever the registration order |
| `WvletFormal/NameLookup/Resolve.lean` | `findSymbolByName` tiers, the identifier rule, relation-name resolution over symbol kinds | `findSymbol_local` / `findSymbol_global` / `findSymbol_anywhere`: scope, then visible globals, then the import-by-name fallback; `findSymbol_add` / `identify_after_add`: re-running lookup after a pass adds a local binding returns it (multi-pass typing); `identify_column_iff`: a column wins exactly over nothing or a function definition that is not completing; `resolveRelation_table_iff`: tables are the last tier |
| `WvletFormal/NameLookup/Traces.lean`, `NameLookupTraces.lean` | — | exports the name lookup conformance fixture |
| `WvletFormal/Algebra/Bag.lean` | rows as maps from column ids to `Rewrite.Value`, relations as lists (bags), conditions in Kleene logic (`Rewrite.Tri`), WHERE (keeps TRUE rows only), and inner, left, right, full, semi, and anti joins with a general ON condition and NULL padding; shared by the optimizer rule models | `Cond.eval_congr`: a condition sees only the columns it reads; `Cond.holds_and`: `p and q` is TRUE exactly when both are |
| `WvletFormal/Pushdown/Rule.lean` | `place`: where `PushDownFilters` puts a conjunct over a join (the preserved input of a LEFT or RIGHT join and the left input of a semi or anti join when it reads only that input, the inner join's conditions, an inner join made of a LEFT join under a NULL-rejecting conjunct, or above); `rewrite`: the plan it builds; `rejectsNull`: conditions never TRUE when the right columns are NULL (comparisons, IN and NOT IN over at least one value, `is not null`, `and` with one such side, `or` of two) | `rewrite_sound`: every placement keeps the rows of the filter over the join, for every pair of inputs and ON condition, NULLs included; `inner_left`: pushing into one input of an inner join (as `ReorderJoins` may) keeps them too; `rejectsNull_holds`: a NULL-rejecting condition is never TRUE on a padded row; `tree_left_inner`: a LEFT join input of an inner join tree whose condition rejects its right columns can be an inner join. The engine also accepts a column under a cast as an operand (a cast of NULL is NULL), which the model's terms don't have |
| `WvletFormal/Pushdown/Necessity.lean` | — | for each refusal (the null-supplying input of a LEFT or RIGHT join, either input of a FULL join, the ON condition of a LEFT join), inputs where moving the conjunct anyway changes the rows, also for a conjunct that is never TRUE on NULLs; `left_inner_needs_rejecting`: under `c is null`, the LEFT join can't be inner |
| `WvletFormal/Pushdown/Traces.lean`, `PushdownFilterTraces.lean` | — | exports the filter pushdown conformance fixture |
| `WvletFormal/LeftJoinCommute/Rule.lean` | `commutes`: whether `ReorderJoins` may move a LEFT join above an inner join (its condition reads no column of the LEFT join's right input) | `commute`: `(A ⟕ₚ B) ⋈_q C` is a permutation of `(A ⋈_q C) ⟕ₚ B` (the same bag) when `q` reads no column of `B`, for inputs whose rows are NULL outside their columns; `flatMap_map_comm`: either loop order of a nested flat map gives the same bag |
| `WvletFormal/LeftJoinCommute/Traces.lean`, `LeftJoinCommuteTraces.lean` | — | exports the LEFT join commute conformance fixture |
| `WvletFormal/Algebra/Bag.lean` (IN and NOT IN) | `inTri`: `v IN (vs)`, the Kleene OR of `v = y`, FALSE over no values; `notIn`: SQL's rows of `x NOT IN (subquery)`; `nullAwareAntiJoin`: the left rows that every right row compares FALSE with | `notIn_tri_true`: NOT IN is TRUE exactly when every `v = y` is FALSE; `nullAwareAntiJoin_eq`: the null-aware anti join is NOT IN |
| `WvletFormal/NotIn/Rule.lean` | `uncorrelated`: the plan of an uncorrelated NOT IN conjunct (every row over no values, none over a NULL value, else an anti join on `x = y` and `x is not null`); `correlated`: the anti join on the correlation and `x = y or x is null or y is null`; `inList`: the `InList` value set | `uncorrelated_sound`, `correlated_sound`, `inList_sound`: each equals SQL's NOT IN (or IN) |
| `WvletFormal/NotIn/Necessity.lean` | — | inputs where a plain anti join on `x = y` keeps a NULL target or keeps rows over a NULL value, where `x is not null` without the NULL-value check keeps rows, and where dropping NULL targets over an empty subquery (the engine before this model) or an unknown NULL target over an empty set loses rows |
| `WvletFormal/NotIn/Traces.lean`, `NotInTraces.lean` | — | exports the NOT IN conformance fixture |
| `WvletFormal/JoinKeyFilter/Model.lean` | hash joins over bags of rows with a nullable key and an arbitrary residual (inner, LEFT, RIGHT, semi, anti); filter conjuncts as three-valued functions of the key | — |
| `WvletFormal/JoinKeyFilter/Sound.lean` | — | `sound_left`, `sound_right`, `sound_inner_right`: a conjunct over the preserved input's key above the join gives the same rows as filtering both inputs by it over their own keys (`join_key_filters` in the engine's `PushDownFilters`) |
| `WvletFormal/JoinKeyFilter/Probe.lean` | — | `probe_left`, `probe_left_of_right`, `probe_right`: a runtime join filter on the probe input keeps the join's rows exactly where the join drops unmatched probe rows (`Lowering.filteredProbe`); `through_left`, `through_right_inner`, `through_right_of_right`: below it, a nested join's left input (inner, LEFT, semi, anti) or right input (inner, RIGHT) may take the filter on either build side (`Lowering.scanColumns`, `build_side_join_filters`), and `through_right_of_left_unsound`, `through_left_of_right_unsound` show the padded inputs may not |
| `WvletFormal/JoinKeyFilter/Findings.lean` | — | `carry_from_null_supplying_side_unsound`: carrying a conjunct from the NULL-supplying input of a LEFT join changes the result |
| `WvletFormal/JoinKeyFilter/Traces.lean`, `JoinKeyFilterTraces.lean` | — | exports the join-key filter conformance fixture |
| `WvletFormal/Algebra/Bag.lean` (`inList`) | `a IN (v1, ..., vn)` over constants, NULL among them, valued by `inTri` | — |
| `WvletFormal/FactorOr/Rule.lean` | `factor`: the conjuncts common to every branch of an `or` (matched by a key, taken from the first branch), then the `or` of the other conjuncts, or nothing more when a branch has no other conjunct; `factorConjuncts`: each conjunct of a filter condition replaced by its factored conjuncts | `factor_sound`, `factorConjuncts_sound`: the factored condition has the value of the original on every row where conjuncts with the same key have the same value (`FaithfulOn`), NULLs included, by distributivity and absorption in Kleene logic; `filter_sound_id`: with the condition itself as the key, a WHERE keeps its rows |
| `WvletFormal/FactorOr/Necessity.lean` | `oldSig`: `InList.sig` before the fix (non-NULL values joined with commas); `oldLitSig`: `Const.sig` before the signature audit (the text `'null'` printed as a NULL); `oldNameKey`: the planner's `exprName` key before this model replayed it (a column without its qualifier) | `oldSig_drops_null_value`, `oldSig_joins_values`: the old signature factors `a not in (1, 3)` with `a not in (1, 3, null)`, and `s in ('x,y')` with `s in ('x', 'y')`, and keeps rows SQL drops; `oldLitSig_reads_text_as_null`: the old literal signature factors `s = 'null'` with `s = null` and keeps rows SQL drops; `oldExprName_drops_qualifier`: the planner's old `exprName` key reads `r.v` as `l.v`, factors `l.v and a = 1` out of `(l.v and a = 1) or (r.v and a = 1)`, and drops a row SQL keeps; `complement_unsound`: `(p and q) or (not p and q) = q` holds in two-valued logic only; `not_common_unsound`: a conjunct of one branch cannot be taken out |
| `WvletFormal/FactorOr/Implied.lean` | `implied`: the `or`, over the branches, of each branch's conjuncts that read only an input's columns, when every branch has one and the `or` reads other columns too; `impliedCond`: the same for a conjunct that is an `or` | `implied_true`, `impliedCond_true`: the implied filter is TRUE wherever the `or` is; `filter_implied`: a WHERE over the `or` and its implied filter keeps the `or`'s rows, NULLs included (a filter rewrite, not a value rewrite); `implied_reads`: the implied filter reads only the input's columns |
| `WvletFormal/FactorOr/Traces.lean`, `FactorOrTraces.lean` | — | exports the OR factoring conformance fixture |
| `WvletFormal/Decorrelate/Rule.lean` | SQL's `[NOT] EXISTS` and `IN` over a correlated subquery (`existsSql`, `inSql`); `planCorrelated`'s split of the subquery's WHERE (`locals` filter the body, `corrs` go on the join); `planExists` and `planIn`: the semi and anti joins of `correlatedJoin`; `markJoin`: `decorrelateExists`' left join with the distinct correlation keys and its `key is not null` flag | `planExists_sound`, `planIn_sound`: the joins return SQL's rows of the WHERE conjunct, for every pair of inputs and conjunct list, NULLs and duplicates included; `markJoin_sound`: the mark join keeps every outer row once, flagged by SQL's EXISTS (never unknown), when the correlated conditions are key equalities; `markFilter_sound`: so any condition over the flag (EXISTS under OR, NOT, or CASE) keeps SQL's rows |
| `WvletFormal/Decorrelate/Necessity.lean` | — | EXISTS as an inner join duplicates outer rows, and removing the duplicates also removes duplicate outer rows SQL keeps; the mark's left join over the body instead of its distinct keys duplicates outer rows; a semi join under OR drops rows; NOT EXISTS is not NOT IN (a NULL outer key, a NULL inner key) |
| `WvletFormal/Decorrelate/Traces.lean`, `DecorrelateTraces.lean` | — | exports the decorrelation conformance fixture, with the pushdown model's semi and anti join cases |
| `WvletFormal/Scalar/Rule.lean` | SQL's value of a correlated scalar subquery (`scalarSql`: an aggregate, any function of its rows, over the inner rows that make the WHERE TRUE); `planScalar`: `decorrelateScalar`'s GROUP BY of the filtered body by the inner keys (NULL keys form a group), the left join on the keys, and the `CASE WHEN key IS NULL THEN empty` projection, skipped when the value over no rows is NULL | `planScalar_sound`: the plan keeps every outer row once, in order, with SQL's value, for every aggregate, NULL keys, duplicate outer rows, and groups without rows included; `scalarFilter_sound`: so any condition over the value keeps SQL's rows |
| `WvletFormal/Scalar/Necessity.lean` | — | without the projection, `count` of an outer row without a group is NULL, not 0; an inner join drops those rows; telling unmatched rows by a NULL value (`coalesce`) replaces a matched group's NULL (`nullif(count(*), 1)`); matching NULL keys as GROUP BY groups them gives a NULL outer key the NULL group's count |
| `WvletFormal/Scalar/Traces.lean`, `ScalarSubqueryTraces.lean` | — | exports the scalar subquery conformance fixture |
| `WvletFormal/SemiReduce/Rule.lean` | `projHas`: a row's projection to columns `K` occurs in a relation; the semi-join reduction of `ReduceSemiJoins` (an inner join whose needed columns `K` are the left input's, read where only the set of rows matters) | `innerJoin_proj_iff_semiJoin`: `X ⋈ Y` and `X ⋉ Y` on the same ON condition have the same rows projected to `K`, as sets, a NULL condition dropping the pair in both; `semiJoin_reduce`, `antiJoin_reduce`, `notIn_reduce`: so IN / EXISTS, NOT EXISTS, and NOT IN (whose NULL handling reads only the set of values, `inTri_congr`) over either input keep the same rows |
| `WvletFormal/SemiReduce/Traces.lean`, `SemiReduceTraces.lean` | — | exports the semi-join reduction conformance fixture |
| `WvletFormal/SelfJoinAggregate/Rule.lean` | `multi`: a key's group holds two different non-NULL values; `having`: `group by k having min(x) <> max(x)` without NULL keys; `Lineage` / `Covers`: the left rows agree with rows of the table / read all of it; the self join `l ⋉ T on l.k = T.k and l.x <> T.x` of `SelfJoinAggregate` | `semiJoin_eq`: when every left row is a row of T, the self join keeps exactly the left rows with a non-NULL x that the semi join on k with `having` keeps, in order, duplicates included; `keys_eq`: when the left side reads all of T, the self join's distinct keys are `having`'s. Gap: that `min(x) <> max(x)` says the same as `multi` is not modeled |
| `WvletFormal/SelfJoinAggregate/Traces.lean`, `SelfJoinAggregateTraces.lean` | — | exports the self-join aggregation conformance fixture |

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
- `lake exe operator-traces` writes
  `wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/typer/operator-traces.json`: every
  arithmetic operator, `=`, the ordering comparisons, and `and` / `or` over every pair of an
  operand sample (each `Ty` constructor, short and date as `other` types, four decimals including
  one past the precision cap, an unresolved decimal, an unresolved type, and a function
  reference). `OperatorTypingConformanceTest` (langJVM) replays it against `TyperRules`.
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
- `lake exe name-lookup-traces` writes
  `wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/lookup/name-lookup-traces.json`: every
  scope program of up to three operations over three handles and two names; global lookups over
  one or two units (two basenames; preset, default, `p`, and `q.r` packages) from five package and
  import contexts, with and without a local binding; and every symbol kind, completing or not,
  with and without a same-named column (identifiers) or table (relation names).
  `NameLookupConformanceTest` (langJVM) replays it against the real `Scope`, `GlobalSymbolIndex`,
  `findSymbolByName`, `identifierRules`, and `resolveTableRef`, running the completing cases inside
  the symbol's completer.
- `lake exe pushdown-filter-traces` writes
  `wvlet-engine/.jvm/src/test/resources/wvlet/lang/engine/runtime/pushdown-filter-traces.json`:
  inner, left, right, and full joins of two inline tables (NULL keys and values, a duplicate
  row) under `a = c` and `a = c and b < d`, each with one of 11 WHERE conjuncts over the left
  input, the right input, both, or no column, with the model's placement and rows.
  `PushDownFiltersConformanceTest` (engineJVM) runs `Normalize` and `PushDownFilters` on each
  query's bound plan (`select` before `where`, so the conjunct is a filter there) to check
  where the conjunct went, and runs the query to compare rows.
- `lake exe not-in-traces` writes
  `wvlet-engine/.jvm/src/test/resources/wvlet/lang/engine/runtime/not-in-traces.json`: a left
  table with NULL and duplicate targets against six subqueries (values without a NULL, with a
  NULL, only a NULL, none, duplicates, and no match), with SQL's rows of `a not in` and `a in` as
  WHERE conjuncts and the value of both per row, plus a NOT IN correlated by `k = b`.
  `NotInConformanceTest` (engineJVM) runs each query with decorrelation on and off (the anti
  join and the value set), the select items in Wvlet and SQL, compares rows with the model and
  DuckDB, and checks the uncorrelated plan: no join over an empty subquery or one with a NULL,
  an anti join otherwise.
- `lake exe factor-or-traces` writes
  `wvlet-engine/.jvm/src/test/resources/wvlet/lang/engine/runtime/factor-or-traces.json`: 21 WHERE
  conditions that are an `or` (a common conjunct, absorption, no common conjunct, three branches,
  duplicate and reordered conjuncts, a complement, IN lists with and without a NULL value,
  `'x,y'` against `'x', 'y'`, `s = 'null'` against `s = null::string`, `b and a = 1` against
  `c and a = 1`, `'x  y'` against `'x y'`, and branches over both join inputs, with NULLs that
  make a branch unknown) over an inline table with NULLs in every column,
  with whether the model factors each one, into how many conjuncts, the factored condition, the
  rows, and the condition over a join of the table's two halves on the row id (`b` and `c` as
  `l.v` and `r.v`, alone and with a TRUE `exists` in each branch), with the filters its factored
  conjuncts imply on the left (`l.a`, `l.v`) and right (`r.v`, `r.s`) inputs.
  `FactorOrConformanceTest` (engineJVM) runs `Normalize` on each bound condition to check that it
  factors as the model does, and runs the condition (FactorOr on and off) and its factored form
  to compare rows with the model and DuckDB. It runs the join form as an inner join ON and a left
  join ON (factored by `Normalize`), and as a WHERE and a left join ON beside an `exists`, a WHERE
  with an `exists` in each branch, and a correlated EXISTS body (factored by `Planner.factorOr`),
  against the model's rows and DuckDB, with `or_implied_filters` on and off. It checks that
  `PushDownFilters` adds to the inner join's conditions exactly the implied filters the model
  gives, each reading only its input. It also replays collisions outside the model's
  conditions (a literal that prints like two `coalesce` arguments or a list, and a LIKE with an
  ESCAPE character) against DuckDB.
- `lake exe decorrelate-traces` writes
  `wvlet-engine/.jvm/src/test/resources/wvlet/lang/engine/runtime/decorrelate-traces.json`: an
  outer table with NULL and duplicate rows against an inner table with NULL keys and duplicate
  matches, with the rows of `exists` and `not exists` over 8 subquery WHEREs (a key, a key and a
  residual, a key and a local filter, only a residual, a key over another column, two keys, a
  key and a negation, and an OR of two correlations), of 5 `in` subqueries (uncorrelated,
  correlated by a residual, by a key, and with a local filter), and of 4 conditions with an
  EXISTS under OR and NOT over 4 key correlations; plus the pushdown model's semi and anti join
  cases (24: both ON conditions of the pushdown fixture with each conjunct over the outer input
  or no column), with placement and rows. `DecorrelateConformanceTest` (engineJVM) runs each
  query, compares rows with the model and DuckDB, checks the join kinds (a semi or anti join per
  conjunct, a left join per mark), and runs `Normalize` and `PushDownFilters` on the pushdown
  cases to check that the conjunct moved into the outer input.
- `lake exe scalar-subquery-traces` writes
  `wvlet-engine/.jvm/src/test/resources/wvlet/lang/engine/runtime/scalar-subquery-traces.json`: an
  outer table with a NULL key, duplicate rows, and a key without a group against an inner table
  with NULL keys, a group whose values are all NULL, and duplicate rows, with the value per outer
  row of 7 aggregates (`count(*)`, `count(d)`, `sum`, `max`, `count(*) + 1`,
  `coalesce(sum(d), 0)`, and a CASE over `count(*)` that is NULL on matched groups) over 4
  correlations (a key, a key with a local filter on NULLs or on values, and two keys).
  `ScalarSubqueryConformanceTest` (engineJVM) reads each row's value back through WHERE conditions
  (`is null`, `= v`, and one under OR), with decorrelation on and off, compares rows with the model
  and DuckDB, and checks the plan: one left join over an aggregation, with the
  `CASE WHEN key IS NULL` above it exactly when the value over no rows is not NULL.
- `lake exe semi-reduce-traces` writes
  `wvlet-engine/.jvm/src/test/resources/wvlet/lang/engine/runtime/semi-reduce-traces.json`: an
  outer table with a NULL key and inputs `l` and `r` with NULL keys and values and duplicate rows,
  with the rows that IN, NOT IN, EXISTS, and NOT EXISTS keep over `select lk from l, r where lk =
  rk` and 3 extra conditions across the inputs (`<>`, `<`, and an OR with `is null`).
  `SemiReduceConformanceTest` (engineJVM) runs each query with `reduce_semi_joins` on and off,
  compares rows with the model and DuckDB, and checks that the rule turned the subquery's inner
  join into a semi join.
- `lake exe self-join-aggregate-traces` writes
  `wvlet-engine/.jvm/src/test/resources/wvlet/lang/engine/runtime/self-join-aggregate-traces.json`:
  a table `t(k, x, id)` with NULL keys, NULL values, and repeated values, with the rows that
  EXISTS, IN, and `select distinct t1.k` keep over `t as t1 ⋉ t as t2 on t1.k = t2.k and t1.x <>
  t2.x`, its left side whole or filtered. `SelfJoinAggregateConformanceTest` (engineJVM) runs each
  query with `self_join_aggregate` on and off, compares rows with the model and DuckDB, and checks
  that the rule turned the whole-table self join into an aggregation and left the filtered ones.
- The `Formal` workflow checks the proofs and fails when a committed fixture is stale.

When you change an activity state transition, a coercion rule in `TypeInference`, an operator
typing rule in `TyperRules`, the build
ordering in `ModelOrder`, a folding rule in `SQLBlock.accepts`, a rule in `RewriteExpr`, a name-lookup rule, where `PushDownFilters` moves a conjunct over a join, how the engine binds NOT IN, how
`FactorOr` factors an `or` (or a `sig` it compares conjuncts by), or how the engine decorrelates
EXISTS, IN, and scalar subqueries, update the model first, fix the proofs, regenerate the fixture, and then change the Scala code until the
conformance test passes.

## Known gaps of the typer model

The model proves today's behavior. These are left for follow-ups:

- Type variables are out of scope: `unify(TypeVar a, TypeVar b)` returns `b`, so it is not
  commutative on type variables.
- Decimals have no common type with other numerics or across precisions, unlike arithmetic typing
  (`decimal + int` is a decimal). Short, byte, real, varchar, and char have no rule either.
- `=` and `!=` are boolean for any operands, errors included: an error operand does not
  propagate through them.
- Ordering comparisons and `and` / `or` reject NULL operands (`x < null` and `null and true` are
  type errors), unlike arithmetic, where NULL takes the other operand's type.
- Decimal results follow the "holds both operands" rule, not each engine's SQL precision rules
  (for example `p1 + p2` digits for multiplication).

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

## Known gaps of the name lookup model

- Tie order is replayed but not proved: within one file, and between same-named files of
  different folders, the newest registration wins; the proofs cover only strictly smaller
  basenames. `findFirstAnywhere` breaks basename ties in hash-map order, so the fixture accepts any
  tied candidate.
- Relation symbols are assumed well formed: `resolveTableRef` also leaves `from x` unresolved
  for a model whose type is not a relation, a table value whose expression is not a row array, or
  an alias whose tree is not a relation, which the model does not distinguish.
- CTE and flow-stage names (`Typer.prepare`) precede `resolveTableRef`; the model proves that tier
  (`resolveRelation_local`) but the fixture replays only `resolveTableRef`.
- Out of scope: GenSQL's second resolution (val substitution that ignores columns, and a lookup
  that falls back to any package), qualified expression names (`t.col`, type members, mixin
  method order), and function-call resolution through `FunctionInliner`.

## Known gaps of the filter pushdown model

- Only the join case of `PushDownFilters` is modeled: the conjunct sits directly over one join.
  Projections, aggregations, windows, set operations, and the walk that moves a conjunct through
  several nodes are out of scope.
- The equalities to constants the pass infers inside inner join trees (`a = b` and `a = 1` add
  `b = 1`) are not modeled; the placement check tolerates the extra conditions.
- The model treats an outer join's ON condition as one condition; the pass also moves its
  equalities between the inputs into hash keys. The row replay checks that NULL keys never
  match, but the key extraction itself is not proved.
- Semi and anti joins are replayed by `DecorrelateConformanceTest`, since queries reach them only
  through IN and EXISTS subqueries; the replay writes them with EXISTS. Right semi and anti joins, the `Join` nodes `ReorderJoins` builds,
  and ASOF joins are out of scope.
- The engine never turns an outer join into an inner join under a NULL-rejecting conjunct, so
  the model has no such rule.
- A TRUE conjunct that reads no column is dropped (by the binder, or by the pass itself), so
  for those cases the fixture checks that no filter is left; a FALSE one stays and is checked.
- The binder binds a WHERE directly over an inner join into the join's conditions, so the
  fixture's queries project before filtering: every conjunct then reaches the pass as a filter.

## Known gaps of the NOT IN model

- The subquery's value is a column, and the target a term over the left input; the model has
  no casts, so `inKeys`' common-type casts are trusted.
- Tuple NOT IN (`(a, b) not in (subquery)`, `Planner.bindTupleInSubquery`) and NOT IN over a
  value list (`a not in (1, null)`, which binds to the same `InList`) are not replayed.
- The correlated case is replayed for one equality correlation only; the residual conditions
  `Planner.correlatedJoin` adds for other correlated predicates are a general `corr` in the
  proof.
- The uncorrelated binding's plan-time evaluation of the subquery is trusted: the model starts
  from the subquery's rows.

## Known gaps of the OR factoring model

- The engine's key is `VExpr.sig`, a string; the model trusts that conditions with the same
  signature have the same value (`FaithfulOn`). The fixture replays the collisions found in
  `InList.sig` (a NULL value, and values joined with commas) and in `Const.sig` (the text
  `'null'` against a NULL). The signatures that print user text (`Const`, `InList`, `LikeExpr`)
  quote a string whose text could read as other syntax (`VExpr.valueSig`), and `LikeExpr.sig`
  includes the ESCAPE character; `FactorOrConformanceTest` replays the `coalesce` and LIKE cases
  the model cannot state. Function names (`RowFn`) are not user text.
- Expressions are deterministic and total in the model: a volatile function (`random()`) under
  two branches would share a signature but not a value, and the model has no errors (division by
  zero, failed casts) whose evaluation order factoring could change.
- The binder factors conditions it hands to the shared planner code (with subqueries) on
  compiler expressions (`Planner.factorOr`), keyed by their SQL text with the whitespace inside
  literals kept. Its key used to be `exprName`, which names `l.v` and `r.v` alike
  (`oldExprName_drops_qualifier`). The replay runs each traced condition over a join, where `b`
  and `c` are such columns, through this copy of the rule. Lateral unnest ON conditions and ASOF
  join conditions use the same key but are not replayed.
- The engine keeps a bound `between` as one conjunct; the model splits every `and`, which has the
  same value.

## Known gaps of the decorrelation model

- Correlated scalar subqueries have their own model (below).
- The subquery is `from rs where ...` (with `select c` for IN): `planCorrelated` rejects other
  shapes, and the model has no projections, so an IN value is an inner column, and casts to a
  common key type are trusted.
- Which conjuncts are local is decided by the columns they read; the engine decides by binding
  each conjunct over the subquery's input, with inner names resolving first. The fixture's
  tables have distinct column names, so name shadowing is not replayed.
- The mark needs every correlated condition to be an equality (the engine rejects others); the
  model takes the keys as `outer = inner` pairs and does not replay the rejection.
- EXISTS in a select item parses in neither Wvlet nor SQL, so the mark's flag is replayed through
  WHERE conditions only.
- Uncorrelated EXISTS is decided at plan time and uncorrelated IN is the same semi join; one
  uncorrelated IN is replayed, and the NOT IN model covers the plan-time value sets.

## Known gaps of the scalar subquery model

- The fixture's values are replayed as select items of a plain projection (and through WHERE
  conditions). A select item of an aggregation (`select a, count(*), (select ...) ... group by a`)
  or next to `unnest` still fails to bind (`ColumnNotFound`), and is not replayed.
- The aggregate is a function of the group's rows, so the model trusts the engine's aggregate
  functions and `emptyGroupValue`'s plan-time evaluation over no rows; the fixture replays seven
  aggregates and expressions.
- The correlations are `outer = inner` equalities on columns; the planner rejects other
  correlations, and the model does not replay the rejection. Casts to a common key type are
  trusted.
- The local/correlated split of the subquery's WHERE is `Decorrelate.holds_split`'s; the model
  starts from the key pairs and the local conjuncts.
