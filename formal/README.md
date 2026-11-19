# Formal models (Lean 4)

Machine-checked models of Wvlet behavior where example-based tests sample poorly:

- The activity item state machine: the fenced `ItemStore` operations, `TaskQueue` restarts and
  lease expiry, and `BatchExecutor` restart reconciliation.
- The compiler's common-type and coercion rules (`TypeInference` in wvlet-lang): `unify`,
  `findCommonType`, `canCoerce`, and `widen`.
- `wvlet build` ordering (`ModelOrder` in wvlet-lang): edges through ephemeral models, selector
  closures, the build order, and cycle detection.

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
| `WvletFormal/ModelBuild/Order.lean` | `buildOrder` rounds, `findCycle` | `buildOrder_spec`: the order is a permutation of the selection in which every model follows the selected models it reads, and it fails exactly when the selection has a cycle, reporting a real one; `rounds_level`: a model is built in the round right after the last round of its selected upstream models, in input order within a round; `findCycle_spec`: a cycle is reported exactly when one exists; `buildOrder_ok_of_acyclic`: after `findCycle` passes, every selection orders |
| `WvletFormal/ModelBuild/Traces.lean`, `ModelBuildTraces.lean` | — | exports the model build conformance fixture |

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
- The `Formal` workflow checks the proofs and fails when a committed fixture is stale.

When you change an activity state transition, a coercion rule in `TypeInference`, or the build
ordering in `ModelOrder`, update the model first, fix the proofs, regenerate the fixture, and then change the Scala code until the
conformance test passes.

## Known gaps of the typer model

The model proves today's behavior. These are left for follow-ups:

- Type variables are out of scope: `unify(TypeVar a, TypeVar b)` returns `b`, so it is not
  commutative on type variables.
- Decimals have no common type with other numerics or across precisions, unlike arithmetic typing
  (`decimal + int` is a decimal). Short, byte, real, varchar, and char have no rule either.
- The arithmetic result table in `TyperRules` is not yet tied to `unify`.

## Known gaps of the model build model

- Selector parsing (comma splitting, trimming, `+` stripping) is replayed by the fixture but not
  proved; the proofs start from parsed selectors.
- Models are names: the model trusts that `ModelBuildPlanner.graph` extracts each model's direct
  references from its plan. A cycle made only of ephemeral models is left to SQL generation, which
  reports it as a recursive model reference.
