
Summary
- Today, types for plans/expressions come from two places: (a) node fields (notably `ValDef.dataType` and schemas on scan/value nodes), and (b) `symbol.symbolInfo.dataType` attached to trees. This can drift and is ambiguous for contributors.
- Goal is to make symbols the single authoritative source for types where it matters (definitions and stable sources), while keeping relationType computation functional for intermediate operators. This matches Scala 3’s denotation-driven design.

Motivation / Goals
- Single source of truth for types at definitions and named entities.
- Eliminate drift between node fields and symbol info.
- Clarify API: readers consult `node.symbol.dataType` when a symbol exists; intermediate plans continue deriving `relationType` from children.
- Align with Scala 3 compiler’s symbols/denotations model.

Non-Goals
- Do not remove `schema` from leaf source nodes (e.g., scans/files) — those nodes originate schemas.
- Do not rework all intermediate operators to store types on nodes; they continue to compute `relationType` from children.
- Do not overhaul expression typing in one pass.

Scope
- In-scope: `ValDef`, `ModelDef`, and leaf source relations (`Values`, `TableScan`, `FileScan`, `ModelScan`, `IncrementalTableScan`).
- Out-of-scope (for now): assigning symbols to every intermediate `Relation`/`Expression` node.

Current State (where DataType/RelationType fields are read)
- `ValDef.dataType` is parsed and read by the typer and codegen; `SymbolLabeler` already seeds `ValSymbolInfo.tpe` from it.
- `ModelDef.givenRelationType` is optional; `relationType` falls back to `child.relationType`. `ModelSymbolInfo.tpe` is created from this.
- Leaf sources carry `schema: RelationType` and compute `relationType` from it (possibly with column projection). Resolved by catalog/inspection passes.

Design (Target)
- Symbols are authoritative for definitions and named sources:
  - `ValDef`: read/write type via `v.symbol.dataType`.
  - `ModelDef`: continue as-is; treat `m.symbol.dataType` as the source of truth.
  - Source relations (`Values`/`*Scan`): keep `schema` on the node, but seed `node.symbol.dataType = schema` so downstream readers can consistently use symbol views.
- Intermediate operators: keep computing `relationType` from inputs (no new fields). Optionally expose `node.symbol.dataType` when a symbol exists, otherwise fall back to `relationType`.

Plan
1) Definitions first (easy win)
   - Add a typed accessor (helper/extension) `tpe(node) = node.symbol.dataType` with fallback to current field where needed.
   - Update reads in:
     - `TypeResolver`: pattern-match on `v.symbol.dataType` for table value constants; avoid reading `v.dataType` directly.
     - `WvletGenerator`: print val types from `v.symbol.dataType`, fallback to parsed type for BC.
     - Tests: assert on `valDef.symbol.dataType` instead of `valDef.dataType`.
   - Keep `ValDef.dataType` for parsing; deprecate direct reads in internal code.

2) Symbolize source relations (moderate)
   - In `SymbolLabeler` or a new light phase (e.g., `SourceSymbolizer` after labeler): assign symbols to `Values`, `TableScan`, `FileScan`, `ModelScan`, `IncrementalTableScan` and set `symbolInfo.dataType = node.schema` (or computed schema for `Values`).
   - Add an invariant check in tests: when a plan has a symbol, `plan.symbol.dataType == plan.relationType`.
   - Gradually migrate internal reads that want a uniform API to consult `node.symbol.dataType` when present.

3) Cleanup + guardrails
   - Deprecate internal usage of `ValDef.dataType`; plan removal in a future minor once call sites are switched.
   - Add a lint/check in specs to flag direct `ValDef.dataType` reads in compiler/optimizer packages.
   - Document the rule: “Definitions and sources → types on SymbolInfo; operators → compute relationType; expressions keep structural `def dataType` until a later phase.”

Acceptance Criteria
- No functional regressions in `./sbt test`.
- `ValDef` typing flows entirely through `symbol.dataType`; codegen and TypeResolver no longer rely on `v.dataType` (except in parsing and initial seeding).
- Source relations have symbols with `dataType` matching their `relationType`.
- New helper to get a node’s type consistently is available and used in updated sites.

Risks / Considerations
- Rewrites must preserve symbols. Our `SyntaxTreeNode.copyInstance` already copies metadata and updates symbol.tree.
- Avoid double sources of truth by treating schema fields as inputs and symbol info as the canonical view post-labeling.
- Performance: symbol lookups are O(1); minimal impact.

Effort Estimate
- Step 1 (definitions): 0.5–1 day including tests and codegen.
- Step 2 (sources): 1–2 days to assign symbols + invariants.
- Step 3 (cleanup): <0.5 day.

Task Checklist
- [ ] Add typed accessor for `symbol.dataType` (with safe fallback where appropriate).
- [ ] Switch `TypeResolver` table-value-constant logic to `v.symbol.dataType`.
- [ ] Switch `WvletGenerator` to prefer symbol types for `val` printing.
- [ ] Assign symbols to `Values`, `TableScan`, `FileScan`, `ModelScan`, `IncrementalTableScan` and seed `symbolInfo.dataType`.
- [ ] Add invariant tests equating `plan.symbol.dataType` and `plan.relationType` on symbolized nodes.
- [ ] Update parser tests to assert symbol types instead of node fields.
- [ ] Add developer note in CONTRIBUTING/docs about symbol‑centric typing.
- [ ] Deprecate internal reads of `ValDef.dataType`; plan removal milestone.

References
- Mirrors Scala 3’s denotation-driven design: trees are immutable; types live on symbols and evolve by phase.
- Relevant files to touch (paths):
  - `wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/TypeResolver.scala`
  - `wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/SymbolLabeler.scala` (or new phase)
  - `wvlet-lang/src/main/scala/wvlet/lang/compiler/codegen/WvletGenerator.scala`
  - `wvlet-lang/src/main/scala/wvlet/lang/model/plan/plan.scala` (`ValDef`)
  - `wvlet-lang/src/test/scala/...` parser/typing specs

