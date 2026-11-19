# ADR: Lean 4 models with trace conformance, starting with activity items

Date: 2026-11-18 · PR #2176

## Context

Quality in Wvlet rests on UniTest, the spec-driven `.wv`/`.sql` runs, and the typer coverage ratchet. These check examples. Some rules, though, must hold for every interleaving. The activity orchestration rules (ADR 2026-10-23 on activity attempt recovery, and the one on reruns and cancel) are of that kind: stale results are fenced by attempt id, final outcomes are never overwritten, and a restart never re-executes in-flight work. Tests sample those interleavings, but they cannot cover them all. Lean cannot verify Scala directly, so a proof only says something about the code as long as the model and the code agree.

## Decision

- Formal models live in `formal/`, a standalone Lean 4 Lake project pinned by `lean-toolchain`, with no Mathlib. It is outside sbt, so contributors who don't touch it never need Lean.
- The first model covers the activity item state machine: the fenced `ItemStore` operations, `TaskQueue` recovery and lease expiry, and `BatchExecutor` restart reconciliation. Its transition functions mirror the Scala `match` structure.
- Each model exports its expected behavior as a JSON fixture (`lake exe activity-traces`). A Scala test (`ActivityModelConformanceTest`) replays the fixture against every implementation, so the existing `CI` workflow detects drift without installing Lean.
- To make restart reconciliation replayable without threads or timers, it is extracted from `BatchExecutor.execute` into `recoverItems`. Its behavior is unchanged.
- The `Formal` workflow builds with `--wfail`, so any `sorry` or warning fails, and it fails when the committed fixture is stale. It starts as a non-required check.

## Consequences

- A change to an activity transition now starts in the model: update the proofs, regenerate the fixture, then change the Scala code until the conformance test passes.
- The proofs cover the model plus the traced behavior, not the Scala code itself. Store traces are exhaustive up to two operations from every state. Restart cases are exhaustive over state, resumability, worker, lease, and rerun policy.
- The model makes the transition table explicit, and it is wider than the old `ItemState` doc comment. It includes `queued → cancelling` and late results that resolve a `cancelling` or `uncertain` item. The stores accept any target state in `reportResult`, so they do not enforce the table themselves.
- Later models follow the same pattern: the typer's common-type laws (`typer-traces`) and the `wvlet build` ordering (`model-build-traces`), where `ModelOrder` was extracted from `ModelGraph` as a pure refactor so the replay needs no parsed models. Proving the build order exposed three cases where a cycle went unreported (a self-reference, a self-reference through an ephemeral model, and a cycle partly outside the selection), which led to the project-wide cycle rule in the model-build-semantics ADR.
- SQL generation's SELECT-block folding (`sql-fold-traces`) is the first compiler-rewrite model. Folding decisions moved into one `SQLBlock.accepts`, so the replay drives the shipped decision directly. Proving `fold_sound` exposed folds that changed results: a filter, group by, `select distinct`, or aggregation after a limit; a projection over a filter on one of its aliases; DISTINCT moving to a later projection; a count and a later limit swapping. It also exposed ORDER BY landing under GROUP BY or DISTINCT. The fixes follow the model's rule: an operator joins a block only when no later clause there sits in an earlier SQL slot, unless the two commute. SQL `LIMIT l OFFSET o` now parses as `Limit(Offset(…))`, so every plan node reads in pipeline order.
- The expression rewrite rules (`rewrite-expr-traces`) are the first model with SQL values and NULL; its `Tri` (three-valued logic) is kept separate for later optimizer rules. The evaluator is parameterized by what each engine's `concat` does, so a rule must be correct on every engine. Proving the rules exposed engine-dependent results: `'a' + x` and `s"...${x}"` compiled to `concat`, which skips a NULL on DuckDB but returns NULL on Trino, Snowflake, and Hive. A non-string operand was not cast (Trino rejects it), a string on the right of `+` was never rewritten, and `s"${x}"` stayed `x`. Wvlet now defines NULL as `''` in string `+` and interpolation rather than inherit SQL's inconsistent NULL rules, and the rules pass `concat` only non-NULL strings (`coalesce(cast(x as varchar), '')`), on which every engine agrees.
- TLA+ was not chosen: it model-checks bounded state spaces but does not give general proofs, and Lean also scales to compiler properties (rewrite soundness, coercion laws), which are the natural next targets.
