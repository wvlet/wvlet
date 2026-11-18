# ADR: Lean 4 models with trace conformance, starting with activity items

Date: 2026-11-18 · PR #TBD

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
- TLA+ was not chosen: it model-checks bounded state spaces but does not give general proofs, and Lean also scales to compiler properties (rewrite soundness, coercion laws), which are the natural next targets.
