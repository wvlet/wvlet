# Introduce Lean 4 for formal verification, starting with the activity item state machine

## Goals

- Add a small, maintained Lean 4 project to the repo so that core invariants are stated as theorems and checked by a machine, not only by example-based tests.
- Prove properties where a bug is costly and tests sample poorly: concurrency/recovery rules that hold across every interleaving, not just the ones a test author thought of.
- Keep the Lean model tied to the Scala code, so a proof says something about the shipped behavior rather than a drifting paper model.
- Establish the pattern (layout, CI, conformance bridge, ADR) so later targets, such as compiler rewrites, can follow it.

## Background

- Nothing in the repo uses formal methods today; quality relies on UniTest plus spec-driven `.wv`/`.sql` runs and the typer coverage ratchet.
- The activity orchestration added in Oct 2026 has the sharpest invariants in the code base (ADR 2026-10-23 activity attempt recovery):
  - `ItemState`: `queued → running → {succeeded, failed, cancelled, uncertain}`, `running → cancelling → {cancelled, uncertain}`, `queued → cancelled`.
  - `reportResult` accepts only the current `attemptId` on a non-final item; `uncertain` is terminal for scheduling but not final.
  - A restart never re-executes an in-flight attempt; uncertain items are never retried unless `retry_uncertain` is set.
- These rules are enforced in two `ItemStore` implementations (in-memory, SQLite) plus `TaskQueue`/`BatchExecutor`; they are exactly the "holds for all interleavings" kind of claim Lean is good at.
- Lean cannot verify Scala directly. The realistic options are (a) a model proved in Lean and (b) a conformance check that the Scala code matches the model.

## Design

Lane: Standard — new toolchain, new CI job, and a new contributor-facing workflow; scope needs the user's judgment.

- **Layout:** a standalone Lake project under `formal/` (Lean 4 + Mathlib-free to keep build time low), pinned via `lean-toolchain`. It does not touch the sbt build; contributors who don't edit it never need Lean installed.
- **First model — activity items:**
  - Model `ItemState`, an item record (state, attemptId, lease), and the events as the code implements them, not as the `ItemState` doc comment lists them: enqueue, claim, heartbeat, lease expiry, reportResult (including late results on `cancelling`/`uncertain`), cancel request on `queued` or `running`, queue recovery, batch restart, and an explicit rerun (`retry_failed`, `rerunCancelled`, `retry_uncertain`) that resets an item to `queued` with a new attemptId.
  - Define the transition function as Lean code that mirrors the Scala `match` structure, so reviewers can read them side by side.
  - Theorems:
    - `reportResult` never changes a final item, and a stale `attemptId` never changes any item.
    - A restart (queue recovery or batch restart) never creates a new attempt for an in-flight item.
    - A terminal item returns to `queued` only through a rerun event whose policy flag is set.
- **Conformance bridge (model ↔ Scala), in two layers:**
  - Lean emits event traces with expected outcomes as a checked-in JSON fixture via `lake exe`.
  - Store layer: replay the fencing traces (claim, markCancelling, reportResult, refreshLease) against both `InMemoryItemStore` and `SQLiteItemStore`.
  - Queue/batch layer: replay lease, recovery, and rerun traces through `TaskQueue` (which already takes an injectable clock) and `BatchExecutor`. This needs a small code change: batch restart reconciliation is extracted from `BatchExecutor.execute` into `recoverItems(…, now)`, so traces run without threads, timers, or a clock seam.
  - The stores accept any target state in `reportResult`, so the PR does not claim "only documented transitions are reachable" for the Scala code; adding that validation is a follow-up.
- **CI:** a new `formal.yml` workflow that installs Lean through a pinned `elan-init.sh`, triggered on changes to `formal/**` or the activity sources. It builds the proofs (any `sorry` fails), regenerates the fixture, and fails on drift. The Scala replay tests run in the existing `test.yml` against the checked-in JSON, so sbt never needs Lean. `formal.yml` starts out non-required and becomes required once it's stable.
- **Docs:** ADR recording why Lean (vs. TLA+ etc.) and the model-plus-conformance pattern; a short `formal/README.md`; CLAUDE.md commands for building and regenerating fixtures.
- **Out of scope for this PR:** compiler/SQL-generator proofs. Natural next targets once the pattern settles: expression simplification and predicate pushdown soundness, SQL three-valued-logic rewrites, and type coercion lattice laws.

## Alternatives and Why Not?

- **TLA+/TLC or Apalache:** great for model checking the distributed protocol, but bounded state exploration and no general proofs; Lean was the requested tool and also scales to compiler properties later.
- **Verify Scala directly (Stainless):** targets a Scala subset that doesn't cover Scala 3 cross-platform code with uni dependencies.
- **Start with the compiler:** higher payoff long-term, but the model is much larger (AST, types, SQL semantics); a first PR would be mostly scaffolding with weak proofs.
- **Lean model without a conformance bridge:** cheaper, but the proof stops describing the code after the first refactor.
