# Specify and prove name-lookup precedence in Lean

## Goals

- Write down, for the first time, which binding a name resolves to in Wvlet: input columns, scope-local definitions, relation aliases, imports, and globals.
- Prove that the lookup code follows that precedence for every configuration in a bounded domain, not just the cases the tests sample.
- Keep the proofs tied to the shipped code: Lean exports a fixture, and a Scala test replays it through the real `Scope`, `GlobalSymbolIndex`, and identifier resolution.
- Surface the surprising rules as decisions for you. This PR changes no resolution behavior.

## Background

- The Typer's resolution is spread over four places, and no doc or ADR states the combined order:
  - `Scope` holds local bindings. `add` always shadows. `enter` binds a name only when no binding is visible yet, so it never shadows. A child scope copies its parent's entries when it is created and never sees entries added to the parent later.
  - `GlobalSymbolIndex.visibleEntries` holds top-level definitions. It returns those in visible packages (stdlib presets, the default package, the unit's own package, and each imported package plus its enclosing packages), sorted by source file basename. Within one file, and for equal basenames, the newest registration comes first.
  - `Context.findSymbolByName` checks the scope, then the visible globals, then (only after an `import x` of the bare name) the first `x` in any package. It also records and warns about duplicate definitions, a side effect.
  - `TyperRules.identifierRules` takes that symbol, but an input column wins over it when the symbol is a function definition whose completion is not running.
- Names are split by kind: `TermName` and `TypeName` never compare equal. A bare identifier looks up a term (val, def, model, flow, partial query, `as` alias), so type definitions never compete with it.
- Model parameters are not scope bindings in the Typer. `typeModelDef` puts them into the input type, so they sit in the column tier. The tests that pin "a parameter shadows a val" (`spec/basic/model/model-shadowing.wv`, `flow.md`) pass because of later stages: GenSQL substitutes scope-bound vals, and `FlowParams.substitute` rewrites flow parameters in the AST.
- Relation names take a parallel path in `Typer.prepare` and `RelationRefResolver`: CTE and flow-stage names come first (a later CTE with the same name replaces an earlier one), then `findSymbolByName`, then table declarations and the catalog. A qualified name (`a.b`) skips the symbol lookup.
- Other pinned cases: a for-loop variable shadows an outer val and nesting shadows again (`spec/basic/for-loop.wv`); columns, including grouped virtual columns, shadow imported engine functions (`EngineNativeFunctionTest`); duplicate top-level names resolve to the file that sorts first (#93).
- Earlier formal models (activity, coercion, model build, SQL folding) live under `formal/` and use the same pattern: Lean exports a JSON fixture, a Scala conformance test replays it, and the `Formal` workflow fails on a stale fixture. Other sessions are adding models in parallel, so this one gets its own Lean directory (`NameLookup/`), executable, and fixture path.

## Design

Lane: Standard. It writes the language's name-resolution rules down as a spec and asks you to rule on several behaviors.

Precedence spec for the Typer, as the code behaves today:

- **Bare identifier in an expression**, highest precedence first:
  1. A term binding in the current scope: the latest `add` wins, and an `enter` counts only when the name was not yet bound. In the Typer, `add` binds for-loop variables. `enter` binds models and tables pre-scanned in a package or loop body.
  2. The visible top-level term from the file whose basename sorts first; within that file, the newest definition.
  3. After `import x`, the first `x` in any package, by basename.
  4. A column of the input relation, including virtual columns such as grouped aggregates, and model parameters.
  - Exception: if the binding from steps 1–3 is a function definition that is not completing, a column with the same name wins over it.
  - When nothing matches, the identifier is unresolved.
- **Relation name** (`from x`): local CTE or flow-stage name, then steps 1–3, then table declarations or the catalog. A model, table val, or alias symbol becomes its relation. A plain val stops the lookup and leaves `x` unresolved. A completing symbol is left for recursion detection. Any other term symbol falls through to tables.
- **Functions and values share the term namespace.** A val and a def with the same name compete under the rules above. Calls `f(...)` resolve through `FunctionInliner` and the builtin table, which this PR leaves out of scope.
- **Re-running lookup is consistent.** The result is a function of the scope, the index, the imports, the input type, and each symbol's kind and completing flag. So when a later pass adds a local binding, the next lookup returns it in place of the global one attached earlier. That is what the multi-pass comment in `identifierRules` relies on.
- **Out of scope, listed as known gaps:** GenSQL's own substitution of scope vals (it ignores columns and falls back to any package), qualified expression names (`t.col`, type members and mixin method order), and function-call resolution.

Proofs and replay:

- Lean model of the four layers (scope operations, global visibility and ordering, the three-tier `findSymbolByName`, the identifier and relation decisions), each mirroring the Scala `match` structure. Completing status is a model input.
- Theorems:
  - Lookup returns the highest-precedence binding under the spec.
  - An inner `add` shadows the outer binding; `enter` never shadows.
  - Lookup is deterministic. Global resolution depends on the registration order only among entries with the same basename.
  - Adding a binding for one name never changes the lookup of another name.
  - After adding a local binding, the next lookup returns it whatever the global index holds.
- `lake exe name-lookup-traces` exports bounded, exhaustive configurations with their expected resolutions:
  - scope operation sequences over a few names
  - global entries across files, packages, and imports
  - every symbol kind × completing × column case
- A new `NameLookupConformanceTest` (langJVM) builds real `Scope`s and `GlobalSymbolIndex` entries and replays the fixture.
- No refactor turned out to be needed: the replay builds real `Scope`s, index entries, `Context`s, and symbols, and drives the shipped `findSymbolByName`, `identifierRules`, and `resolveTableRef` directly.
- The `Formal` workflow and `formal/README.md` gain the new model, and the ADR's Consequences list records the spec. `lake build --wfail`, no `sorry`, no Mathlib.

Questions for you (code stays as is unless you say otherwise):

- **Q1, column vs definition asymmetry.** A column shadows a same-named function definition, but a val, model, or alias shadows the column. So `val age = 3` changes the type of `select age`. For the same reason, a model parameter named like a top-level val is typed as the val, while GenSQL later substitutes the parameter. Keep this, or let the column tier win for every bare identifier in the Typer (a behavior change, in a follow-up)?
- **Q2, `enter` never shadows.** A model or table that the Typer `enter`s inside a for-loop body resolves to a same-named outer one. GenSQL also `enter`s vals, so re-entering a val name keeps the first binding. `add` (loop variables, model arguments in GenSQL) does shadow. Keep this as the documented split between "declare if absent" and "shadow"?
- **Q3, globals are ordered by basename only.** The unit's own package, imported packages, the default package, and stdlib presets rank equally. An explicit `import p` does not make `p`'s definition beat a default-package definition from an earlier-sorted file. Two files named `a.wv` in different folders tie, and then registration order decides. Keep this as documented behavior, or plan a follow-up that sorts by relative path and ranks own package over imports over the default package over presets?
- **Q4, smaller items, defaulting to "keep and document":**
  - When a later pass picks the column, the symbol an earlier pass attached stays on the identifier. The theorem then covers the lookup result, not the tree.
  - `from x` with a plain `val x = 3` leaves `x` unresolved instead of falling through to a table named `x`.
  - `Scope.cloneScope` is unused and drops the scope's own entries. Remove it here, a behavior-neutral deletion?

## Alternatives and Why Not?

- **Model `Typer.scala` end to end.** About 1,200 lines of mutable passes. The lookup policy is the part with a precedence claim worth proving, and it is pure once extracted.
- **Include GenSQL's substitution in the same model.** It is a second resolver with different rules (Q1). Specifying the Typer first gives a baseline to align GenSQL with later.
- **Example tests only.** They pin single cases but cannot show that precedence holds for every combination of scope, imports, and files, which is where the asymmetries hide.
- **Replay through full compilations of generated `.wv` sources.** Slower and indirect. Building `Scope` and the index directly exercises the same code without the parser in the loop.
