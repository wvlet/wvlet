# ADR: Name-lookup precedence in the Typer

Date: 2026-11-18 · PR #2186

## Context

The Typer resolves names in four places, and no doc stated the combined order:

- `Scope` holds local bindings.
- `GlobalSymbolIndex` holds top-level definitions.
- `Context.findSymbolByName` combines the two with imports.
- `TyperRules.identifierRules` weighs the found symbol against the columns of the input relation.

Relation names (`from x`) take a parallel path through `Typer.prepare` and `RelationRefResolver`. Tests pinned single cases: the loop variable shadowing in `spec/basic/for-loop.wv`, columns shadowing engine functions in `EngineNativeFunctionTest`, and duplicate names across files (#93). Some behaviors that follow from the code were surprising enough to need an explicit decision.

## Decision

The precedence below is today's behavior. It is now specified and proved in `formal/WvletFormal/NameLookup/` (see the formal-verification ADR) and replayed by `NameLookupConformanceTest`.

- Names are split by kind: a bare identifier looks up a term name (val, def, model, flow, partial query, `as` alias). Type definitions live under type names and never compete with it.
- A bare identifier in an expression resolves to, highest precedence first:
  1. A binding in the current scope. `Scope.add` shadows, and the newest `add` wins. `Scope.enter` binds only a name that is not bound yet, so it never shadows. A child scope sees a snapshot of its parent taken at creation.
  2. The visible top-level definition whose source file basename sorts first. Visible means a stdlib preset, the default package, the unit's own package, or an imported package or one of its enclosing packages. Within one file, the newest definition wins.
  3. After `import x` of the bare name, a definition named `x` in any package, the first by basename.
  4. A column of the input relation, including virtual columns such as grouped aggregates. In the Typer, a model's parameters are input columns too.
  - Exception: a column shadows a function definition (`MethodSymbolInfo`) found by steps 1–3, unless the function's completion is still running. Any other definition shadows the column.
- A relation name `from x` resolves to, highest precedence first:
  1. A CTE or flow-stage name of the enclosing query; a later CTE replaces an earlier one with the same name.
  2. The symbol from steps 1–3 above. A model becomes its scan, a table value constant its rows, an `as` alias its query (each assuming a well-formed definition: a relation-typed model, a literal row array, an aliased relation; otherwise `x` stays unresolved).
  3. Table declarations or the catalog.
  - A plain val, or a symbol whose completion is running (a recursive reference), leaves `x` unresolved.
  - A qualified name `a.b` skips the symbol lookup.
- Kept as specified, rather than changed:
  - The column-versus-definition asymmetry: `val age = 3` changes the type of `select age`.
  - `enter` never shadows: a model or table re-declared inside a for-loop body resolves to the outer one.
  - Globals rank by basename alone. The unit's own package, imports, the default package, and presets rank equally. Same-named files in different folders fall back to registration order.
  - A later pass that picks the column leaves the symbol an earlier pass attached to the identifier.
  - `from x` with a plain `val x` stays unresolved rather than falling through to a table.
- `Scope.cloneScope` was removed: it was unused and dropped the scope's own bindings, which its name did not suggest.

## Consequences

- A change to any rule above starts in the Lean model: update the proofs, regenerate `name-lookup-traces`, then change the Scala code until the conformance test passes.
- Lookup is deterministic, and the proofs show where order matters. Re-running a lookup after a pass adds a local binding returns that binding (`findSymbol_add`), which the multi-pass typing relies on. A global winner with a strictly smallest basename does not depend on the order units were registered in (`visibleEntries_unique_min`).
- GenSQL resolves names a second time with its own rules. It substitutes scope-bound vals without checking columns, which is how a model parameter shadows a top-level val at run time, and it falls back to any package. That path, qualified expression names (`t.col`, type members, and mixin method order), and function-call resolution remain outside the model.
