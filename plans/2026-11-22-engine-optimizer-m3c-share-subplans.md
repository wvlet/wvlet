# Engine optimizer M3c, PR 13: Share subplans by their bound structure, not their SQL text

## Goals

- Framework pass 5 (ShareSubplans), first part: decide which uncorrelated scalar subqueries read the same input from the input's bound runtime plan, compared structurally by column ids, instead of from the SQL text the compiler's code generator prints for it (#2243).
- Keep the plans the grouping produces: a group of two or more is still evaluated as one `Project(input, all items)`, so golden plans stay unchanged.

## Background

- `Planner.prefetchScalarSubqueries` (#2243) runs before planning. It collects the scalar subqueries that are a `Project` with one aggregate select item and groups them by `GenSQL.generateSQLFromRelation(p.child)`. Each group of two or more is planned and evaluated once, and the results fill `scalarResults`, which `bindScalarSubquery` reads. TPC-DS q09 reads `store_sales` 5 times instead of 15 this way.
- SQL text is the wrong identity on both ends. It tells apart inputs that bind to the same plan (`where t.a >= 2` and `where a >= 2`, or two reads of the same Parquet file), and it depends on the SQL generator, which the engine otherwise doesn't need (pain point 4 of the framework plan: rules compare by ids, not printed SQL).
- Scalar subqueries are not IR nodes: the binder evaluates uncorrelated ones at plan time into constants. The sharing decision therefore stays a planner step before binding, but its key now comes from the IR.

## Design

Lane: Express. This is an internal planner change with an unchanged plan shape. The direction and gates came with the launch prompt.

- **Structural key (`ShareSubplans.key`, new, in `wvlet.lang.engine.runtime`).** It canonicalizes a bound `RuntimePlan` by renumbering its column ids in order of definition (children first), with the expressions remapped. Two plans get equal keys when they are equal up to the choice of ids. Column names, types, and qualifiers are part of the key, so the merged items, which are bound against the first subquery's input, resolve the same way for every member. Equality is conservative:
  - Expressions compare by case-class equality after remapping. A row function compares its function by reference, and a constant compares its value with `==`, so values that can't be compared for sure never merge.
  - Literal counts (`Filter.literalCounts`, `JoinCondition.literalCounts`) are dropped from the key. They only steer join-order estimates.
  - Expressions also compare by their signatures, since `==` on boxed values equates some distinct constants (`0.0` and `-0.0`).
  - A plan with a `Legacy` leaf, `Values` rows, or a `CTERef` has no key.
  - An input with subqueries has no key and is not bound for one: binding evaluates its subqueries, so the discarded key binding would run them once more, and alone, before they are grouped. Their operators, batches, and CTE objects only compare by reference.
  - Scans compare their sources by `TableSource.key` (new). It defaults to the source itself, and file sources (Parquet, JSON, CSV) override it with their path, because each file reference creates a new source.
- **Planner.** `prefetchScalarSubqueries` binds each candidate's input with the runtime binder (on both planners, since the key is only compared) and groups by `ShareSubplans.key`. The skips stay as they are: inputs that read a CTE, inputs that fail to bind (correlated ones fail on outer names), and the row-count and fallback guards. The hidden-name counter is restored after binding the keys, so hidden column names in golden plans don't shift.
- **Toggle.** Unchanged: `prefetch_scalar_subqueries` turns off the whole merge.
- **Tests.** `EngineScanPushdownTest`: subqueries whose inputs differ only in qualification read the input once; subqueries over different filters don't merge. A unit test for `ShareSubplans.key`: equal plans under different ids get equal keys, and different constants, columns, or sources get different keys.

## Out of scope (recorded on the item, folded into M4)

- **CTE sharing by reference count on the IR.** The framework plan also gives ShareSubplans the decision to share a CTE, or any repeated subplan, by counting its reads. Today `countCTERefs` counts CTE names on the compiler's relations before binding. Counting on the IR needs the whole query bound before any part is lowered, and today readers inside `Legacy` leaves and nested plans are operators by then (notes from #2273). It folds into M4, together with taking the CTE reader union and the reader filters from the `CTERef`s of the logical plan.
- **Scalar subqueries over CTEs.** They stay skipped. Their inputs bind only in the CTE's scope, which is known only while planning.
