# Prove model build ordering in Lean and replay it against ModelGraph

## Goals

- Show with machine-checked proofs that `wvlet build` runs every selected model after the selected models it reads, builds each selected model exactly once, selects what the dbt-style selectors promise, and reports dependency cycles.
- Tie the proofs to the shipped code with the trace-conformance pattern from #2176 and #2177: a Lean-generated fixture, a Scala replay test, and a freshness check in the `Formal` workflow.
- Fix the cycle bugs this work found (below), so the proven property "a cycle is always reported" holds.

## Background

- `ModelGraph` (in `ModelBuild.scala`) has three steps. `graph` derives each materialized model's upstream edges, passing through ephemeral models. `select` resolves `m`, `+m`, `m+`, and `+m+` with a DFS `closure` and returns the result in definition order. `buildOrder` places models in rounds: each round is every remaining model whose selected upstreams are already built, kept in input order. It throws `RECURSIVE_MODEL_REFERENCE` when a round would be empty.
- Probes against `ModelBuildLauncher.plan` (a throwaway test, not committed) found three cases where a cycle is **not** reported:
  - **Self-reference is dropped.** `model ma (table) = from ma`, directly or through an ephemeral model (`ma -> e -> ma`), gets no edge, because `upstreamsOf` marks the model itself as visited. The plan succeeds, and the save statement reads `ma`'s own persisted table: it fails on a first build and reads stale data later.
  - **A partial selection hides a cycle.** With `ma <-> mb`, `wvlet build` and `wvlet build +ma` report the cycle, but `wvlet build ma` plans `ma` alone, reading `mb`'s persisted (possibly stale) table. `buildOrder` only looks at edges inside the selection.
  - The compile step does not catch these. `GenSQL`'s recursion check only fires on inlining, and a build replaces materialized references with table scans first.
- **An empty selection builds nothing.** `-s ""` or `-s ","` passes a non-empty selector list that splits to no names. The build then does nothing, with no error. It does not fall back to building every model.
- The error message also lists every blocked model ("among: a, b, c"), including models downstream of the cycle, not the cycle itself.
- The activity and typer models already set the conventions: no Mathlib, `lake build --wfail`, one `lake exe` per fixture, and a Scala test that needs no Lean.

## Design

Lane: Standard — it changes user-visible `wvlet build` behavior (a project that has a cycle now fails even for a selection that avoids it) and adds a new model.

- **Cycle rule (behavior change):** `graph` rejects any cycle among materialized models, self-loops included, whatever the selection. It throws `RECURSIVE_MODEL_REFERENCE` with the path, e.g. `ma -> mb -> ma`, matching `GenSQL`'s message. This is how dbt behaves: an invalid project graph fails before anything runs.
  - Self-edges are found by reaching the model's own name while walking through its ephemeral references. A cycle made only of ephemeral models is left to `GenSQL`, as today.
  - `buildOrder` keeps its own check as a backstop for graphs built by hand.
  - Selectors that split to no names are rejected as a usage error, instead of building nothing.
  - The rule is recorded in the model-build-semantics ADR and the model-pipelines docs page.
- **Pure extraction (refactor, no behavior change):** move edge derivation, `closure`, selector resolution, `buildOrder`, and cycle search into functions over names, `List[String]` and `Map[String, List[String]]`. `ModelGraph` and `ModelBuildPlanner.graph` delegate to them. The replay test can then drive the code without building `ModelDef` ASTs.
- **Lean model (`WvletFormal/ModelBuild/`):** models are `Nat`s in definition order, and `upstreams` is a function. The model mirrors `closure`, `select`, `buildOrder`, edge derivation through ephemerals, and cycle detection, each with explicit fuel bounded by the node count.
- **Theorems to prove** (no `sorry`):
  - `closure` (a stack-based DFS) returns exactly the nodes reachable from the start node. It terminates within 1 + edge-count pops: each pop either visits a new node or discards one, and only new visits push.
  - `select` models the comma split and the `+` parsing (a bare `+` is an unknown model). It returns the union of each selector's closure (the model, plus ancestors for `+m`, plus descendants for `m+`), in definition order, without duplicates.
  - `buildOrder` succeeds exactly when the input has no cycle among its own edges. On success the output is a permutation of the input, and each model comes after every input model it reads.
  - Determinism: the order is level order. Each model sits in the round after its last upstream in the input, and models in the same round keep input order. Since `select` returns definition order, a build orders by level, then definition order. The fixture pins the exact order.
  - Cycle detection reports a cycle exactly when one exists, and the reported path is a real cycle in the graph.
- **Conformance:** `lake exe model-build-traces` enumerates every graph on up to 3 models, plus small ephemeral-chain cases, and records the edges, selections, build order or cycle, and graph validity. `ModelBuildConformanceTest` (langJVM) replays the fixture, stored at `wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/pipeline/`, against the extracted functions. The `Formal` workflow checks its freshness. `ModelBuild.scala` and the fixture directory join both of the workflow's path lists.
- **Tests and docs:** `ModelBuildTest` gains end-to-end cases for the self-reference cycle, the ephemeral self-loop, the partial-selection cycle, and the empty selection. `formal/README.md` describes the new model. CLAUDE.md gets the "update the Lean model first" rule for ModelBuild ordering.

## Alternatives and Why Not?

- **Report cycles only when they touch the selection's closure.** This keeps `wvlet build ma` working in a broken project, but the project still has a model that cannot be built from scratch. It also makes the rule depend on the selection.
- **Prove properties of `buildOrder` only, without fixing the bugs.** The proven "cycles are reported" property would then hold only inside the selection, and the self-loop hole would stay.
- **Replay against `ModelGraph` with real `ModelDef`s.** That needs parsing per trace and no longer exercises a pure function. The end-to-end `ModelBuildTest` cases already cover the wiring.
