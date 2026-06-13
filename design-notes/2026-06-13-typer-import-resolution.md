# Typer Design for Full Compilation-Time Typing with Import Statements

**Date**: 2026-06-13
**Related Issues**: [#392](https://github.com/wvlet/wvlet/issues/392) (new typer), [#93](https://github.com/wvlet/wvlet/issues/93) (package scope management)
**Builds on**: `design-notes/2025-01-15-new-typer-architecture.md`, `plans/2025-01-24-typer-integration.md`

## Goal

Make the new `Typer` (`wvlet.lang.compiler.typer.Typer`) able to perform *full-fledged*
compilation-time typing of a multi-file program, where names defined in one `.wv`/`.sql` file
become visible in another **only** through `import` statements (plus the always-visible package
scope and standard library). This requires the typer to:

1. Build a real package/namespace symbol tree, not a flat list of "known symbols".
2. Resolve each `import` form to a concrete set of bindings and install them into the typing scope.
3. Type imported compilation units **on demand** (lazily), in dependency order, with cycle
   detection — without re-typing the whole program eagerly.
4. Produce precise diagnostics for unresolved / ambiguous / cyclic imports.

This note specifies the data model, the resolution algorithm, the phase changes, and a staged
implementation plan.

## Current State and Why It Is Not Enough

### What exists today

- **AST** (`model/plan/plan.scala`):
  `Import(importRef: NameExpr, alias: Option[NameExpr], fromSource: Option[StringLiteral], span)`.
  Covers `import a`, `import a.b`, `import a.*`, `import a.b as c`, and
  `import a.* from "github.com/..."`.
- **SymbolLabeler** creates an `Import` symbol with `ImportType(i)` and calls `ctx.withImport(i)`.
- **Typer** pre-scan and main pass both just thread the import through `ctx.withImport(i)`.
- **Context.findSymbolByName** (the only resolver) does:
  1. `scope.lookupSymbol(name)`,
  2. if an `importDefs` entry's `importRef.leafName == name`, scan **all** contexts'
     `knownSymbols` for any symbol whose `name == name`,
  3. else scan all *global* contexts' `knownSymbols`.

### Problems

1. **Imports are not actually resolved.** Step 2 above matches a symbol by *leaf name only*,
   across *every* compilation unit, regardless of which package/file the import points at.
   `import a.foo` will happily bind to a `foo` defined in unrelated unit `z`. There is no notion
   of "the package `a`" being a thing that can be looked up.
2. **No namespace tree.** `PackageSymbolInfo` carries a `declScope`, and `registerPackageSymbol`
   already nests packages under `RootPackage`, but nothing *resolves a qualified name* against that
   tree. `knownSymbols` is a flat per-unit list, so `a.b.c` cannot be walked member-by-member.
3. **Imports don't survive scope nesting.** `Context.newContext` resets `importDefs = Nil`, and
   `findSymbolByName` only inspects the current context's `importDefs` (not the `outer` chain).
   So an `import` at the top of a file is invisible inside a `model`/`type` body — exactly where
   user code references imported types (e.g. `model weblogs: td_sdk_log = ...`).
4. **No laziness / ordering.** Typing a unit that imports another assumes the other unit's symbols
   already carry resolved types. With per-unit phase execution there is no guarantee the imported
   unit was typed first, and no mechanism to trigger it. `SymbolLoader extends LazyType` exists for
   exactly this but is unused. `ImportInfo.scala` is an empty stub.
5. **No diagnostics.** Unresolved imports silently fall through to "unresolved identifier" at use
   sites; ambiguous and cyclic imports are not detected at all.
6. **Wildcard / alias / remote** imports are parsed but not honored by resolution.

## Design Overview

Four cooperating pieces:

```
                       ┌─────────────────────────────────────────┐
   SymbolLabeler  ───► │ Namespace tree (RootPackage → packages   │
   (per unit)          │ → TypeDef/ModelDef/Def/Val members),     │
                       │ each member a Symbol with a SymbolLoader  │
                       │ completer (lazy type)                     │
                       └─────────────────────────────────────────┘
                                        ▲
                                        │ resolve qualified names, members
   Typer.preScan  ───► ImportResolver ──┘   produces ImportInfo per Import
   (per unit)          installs ImportInfo bindings into the unit's
                       package Scope (so they survive nested contexts)
                                        │
                                        ▼
   Typer.typePlan ───► Context.lookup uses: local scope → installed import
                       bindings → enclosing package members → preset/std lib
                                        │
                                        ▼  (member type requested)
                       SymbolLoader.load triggers on-demand typing of the
                       defining unit (dependency-ordered, cycle-guarded)
```

Key idea: **imports are resolved to bindings during pre-scan and entered into the package scope as
real `ScopeEntry`s**, so the existing scope-chain lookup transparently sees them in every nested
context. Cross-unit *types* are computed lazily through `SymbolLoader`, so a unit's symbols can be
*labeled* eagerly but *typed* only when first referenced.

## Part 1 — A Real Namespace Tree

### 1.1 GlobalSymbolTable

Today symbols live in per-unit `knownSymbols: List[Symbol]` and in the ad-hoc package `declScope`s
threaded off `RootPackage`. Promote this into an explicit, queryable structure on `GlobalContext`:

```scala
// new: wvlet.lang.compiler.GlobalSymbolTable
class GlobalSymbolTable(rootPackage: Symbol):
  /** Resolve a possibly-qualified name starting from the root package. */
  def lookup(qualified: Name*): Option[Symbol]

  /** Members directly declared in a package/type symbol. */
  def membersOf(owner: Symbol): List[Symbol]

  /** The package symbol for a dotted package path, creating intermediate
    * packages if necessary (used by SymbolLabeler). */
  def packageFor(path: Seq[TermName]): Symbol
```

`registerPackageSymbol` (already in `SymbolLabeler`) becomes the writer for `packageFor`. The
existing `PackageSymbolInfo.declScope` remains the per-package symbol table; `GlobalSymbolTable`
is the thin index that lets us walk `a` → `a.b` → `a.b.c` deterministically instead of scanning
every unit.

This directly fixes problems #1 and #2: an import target is resolved by walking the package tree,
not by global leaf-name scanning.

### 1.2 Member symbols carry lazy completers

When `SymbolLabeler` registers a `TypeDef`/`ModelDef`/`def`/`val`, it sets a `SymbolInfo` whose
`tpe` is a `SymbolLoader` (a `LazyType`) instead of computing the concrete type immediately:

```scala
final class DefSymbolLoader(unit: CompilationUnit, tree: SyntaxTreeNode) extends SymbolLoader:
  def load(root: SymbolInfo)(using Context): Unit =
    // Ensure the defining unit is typed up to the Typer phase, then copy
    // the computed DataType into `root` (which replaces the lazy tpe).
    Typer.ensureTyped(unit)
    root.withType(tree.tpe)
```

`SymbolInfo.dataType`/`tpe` is changed to *force* a `SymbolLoader` on first access (the standard
"completer" pattern, mirroring Scala 3's `SymDenotation.info`). This is what makes cross-unit typing
both lazy and demand-driven (problem #4).

## Part 2 — ImportInfo and Import Resolution

### 2.1 ImportInfo model (fills the empty stub)

```scala
// wvlet.lang.compiler.analyzer.ImportInfo  (replace the empty class)
/** The resolved result of a single `import` statement. */
sealed trait ImportInfo:
  def importNode: Import

object ImportInfo:
  /** import a.b  /  import a.b as c   → one name bound to one symbol */
  case class Single(importNode: Import, boundName: TermName, target: Symbol) extends ImportInfo

  /** import a.*  → every public member of package/type `a` */
  case class Wildcard(importNode: Import, pkg: Symbol) extends ImportInfo

  /** import ... from "github.com/..."  → bindings resolved against a fetched
    * VirtualFile/archive; otherwise identical to Single/Wildcard. */
  case class Remote(importNode: Import, source: String, delegate: ImportInfo) extends ImportInfo

  /** Could not be resolved; carries the reason for diagnostics. */
  case class Unresolved(importNode: Import, reason: String) extends ImportInfo
```

### 2.2 ImportResolver

A new object in the typer package, invoked from `Typer.preScan`:

```scala
object ImportResolver:
  /** Resolve an import to its bindings and enter them into `pkgScope`. */
  def resolveAndInstall(i: Import, pkgScope: Scope)(using ctx: Context): ImportInfo
```

Algorithm for a non-remote import:

1. Split `i.importRef` into its dotted parts (`a.b.c` or `a.*`).
2. For a wildcard `a.*`: resolve prefix `a` via `GlobalSymbolTable.lookup` to a package or type
   symbol; produce `Wildcard`. For each public member, `pkgScope.add(member.name, member)` unless a
   name collision exists (record an ambiguity error if two wildcard imports bring the same name).
3. For `a.b`: resolve `a` to a package, then look up member `b` in it. Bind under `b`’s leaf name,
   or under the alias if `i.alias` is set. Produce `Single`. `pkgScope.add(boundName, target)`.
4. If any step fails, produce `Unresolved` and add a `TyperError` (see Part 4).

Because bindings are written into the **package scope** (which child scopes inherit via
`Scope.newChildScope` copying `outer.getAllEntries`), imports become visible inside `model`/`type`
bodies. This fixes problem #3 without threading `importDefs` through every `newContext`.

Resolution results are cached per `(unit, ImportNode)` so re-running the phase (REPL/incremental) is
idempotent.

### 2.3 Remote imports (`from "github.com/..."`)

`Remote` delegates fetching to a `VirtualFile` provider (the existing `GitHubArchive` path, today a
`???`). The fetched archive is registered as additional `CompilationUnit`s in `GlobalContext`,
parsed + labeled, then resolved exactly like a local import. Fetches are content-addressed and
cached on disk under `workEnv` so repeated compiles are offline-friendly. Network failure yields
`Unresolved` with a clear message rather than aborting the whole compile. This can land **after**
the local-import work; the data model already accommodates it.

## Part 3 — Lazy, Dependency-Ordered Typing

### 3.1 ensureTyped + cycle guard

```scala
object Typer extends Phase("typer"):
  // re-entrant, memoized per unit
  def ensureTyped(unit: CompilationUnit)(using ctx: Context): Unit =
    if unit.isFinished(Typer) then ()
    else if typingInProgress.contains(unit) then
      // import cycle: stop recursion, leave already-labeled (lazy) symbols in place
      ctx.addTyperError(CyclicImport(unit, typingInProgress.toList))
    else
      typingInProgress += unit
      try run(unit, ctx.withCompilationUnit(unit)) finally typingInProgress -= unit
```

- **Pre-scan/labeling stays eager and cheap** (no type computation), so any unit's *names* are
  resolvable as soon as it is parsed.
- **Type computation is pulled** through `SymbolLoader.load` → `ensureTyped`. The first reference
  to an imported `model`/`type` triggers typing of its defining unit; subsequent references hit the
  now-resolved `SymbolInfo`.
- **Cycles** (A imports B, B imports A) are detected by the in-progress set. A cycle is only a hard
  error if it is also a *value/type* dependency cycle; pure name visibility cycles resolve fine
  because labeling already happened. The guard degrades to "use the partially-typed symbol" and
  records a `CyclicImport` diagnostic rather than looping.

### 3.2 Interaction with the phase runner

`Compiler.compileInternal` runs each phase over all units. `Typer.run` per unit becomes the entry
that `ensureTyped` funnels through. Because labeling for *all* units precedes the typer phase group
(SymbolLabeler runs over the whole unit list first), forward and cross imports are always
name-resolvable before any typing begins. No global topological sort is required up front — demand
ordering plus the cycle guard subsumes it. (A topo-sort can be added later as an optimization to
reduce re-entrancy depth; `ModelDependencyAnalyzer` already computes a DAG we can reuse.)

## Part 4 — Diagnostics

Add to `TyperError`:

```scala
case class UnresolvedImport(i: Import, reason: String) extends TyperError
case class AmbiguousImport(name: Name, candidates: List[Symbol], span: Span) extends TyperError
case class CyclicImport(unit: CompilationUnit, chain: List[CompilationUnit]) extends TyperError
```

And matching `StatusCode`s (per CLAUDE.md error-handling guidance) so the CLI/REPL report them with
source locations. Unresolved imports are reported at the `import` site, not deferred to use sites,
which is a concrete UX improvement over today.

## Part 5 — Replacing `Context.findSymbolByName`

The new lookup, used by `TyperRules.identifierRules` and qualified-name resolution:

```scala
def lookupName(name: Name): LookupResult =
  scope.lookupSymbol(name)               // local + inherited (incl. installed imports)
    .orElse(enclosingPackageMember(name)) // same-package siblings
    .orElse(global.symbolTable.lookup(name)) // preset / std lib at root
    .map(LookupResult.Found(_))
    .getOrElse(LookupResult.NotFound)

/** Qualified `a.b.c`: resolve `a` then walk members. */
def lookupQualified(parts: Seq[Name]): LookupResult
```

The old global-leaf-name scan (step 2/3 of today's `findSymbolByName`) is removed. Names are visible
**only** via: the local scope chain, installed imports, the enclosing package, and the root/preset
namespace — which is the intended import semantics.

## Affected Files

| File | Change |
|------|--------|
| `compiler/analyzer/ImportInfo.scala` | Replace empty stub with the `ImportInfo` ADT |
| `compiler/typer/ImportResolver.scala` | **New**: resolve + install imports |
| `compiler/GlobalSymbolTable.scala` | **New**: queryable namespace tree |
| `compiler/SymbolLoader.scala` | Add `DefSymbolLoader` (lazy per-def completer) |
| `compiler/Symbolnfo.scala` | Force `SymbolLoader` on `tpe`/`dataType` access |
| `compiler/analyzer/SymbolLabeler.scala` | Register members into `GlobalSymbolTable`; attach lazy completers |
| `compiler/typer/Typer.scala` | Call `ImportResolver` in `preScan`; add `ensureTyped` + cycle guard |
| `compiler/typer/TyperRules.scala` | Identifier/DotRef use `lookupName`/`lookupQualified` |
| `compiler/typer/TyperError.scala` | Add import diagnostics |
| `compiler/Context.scala` | Add `lookupName`/`lookupQualified`; deprecate `findSymbolByName` |
| `api/.../StatusCode` | Add import-related error codes |

All changes are behind the existing `Compiler.withNewTyper(useNewTyper = true)` switch, so the
default (legacy `TypeResolver`) path is untouched until parity is reached.

## Staged Implementation Plan

1. **Namespace tree** — `GlobalSymbolTable`, populate from `SymbolLabeler`, add
   `lookupQualified`. No behavior change yet (legacy resolver still default).
2. **ImportInfo + ImportResolver (local imports)** — `Single`, `Wildcard`, `as` alias; install into
   package scope; new `lookupName`. Wire into `Typer.preScan`. Unit tests resolving across two
   in-memory units.
3. **Lazy completion** — `DefSymbolLoader`, force-on-access in `SymbolInfo`, `ensureTyped` +
   cycle guard. Enables true cross-unit typing.
4. **Diagnostics** — unresolved/ambiguous/cyclic import errors + `StatusCode`s; negative specs.
5. **Remote imports** — implement `GitHubArchive` fetch + cache behind `ImportInfo.Remote`.
6. **Parity & cutover** — extend `TyperValidationTest` to cover import scenarios; once green across
   `spec/`, flip `withNewTyper` on by default and retire the legacy import path.

## Test Strategy

- **Unit**: `ImportResolverTest` — each import form resolves to the right `ImportInfo` and installs
  the expected scope entries; ambiguity and unresolved cases produce the right errors.
- **Spec-driven**: reuse/extend existing import specs — `spec/basic/q5.wv` (single/wildcard/alias/
  remote), `spec/cdp_behavior/behavior.wv` and `spec/cdp_simple/behavior.wv`
  (`import td_sdk_log` then `model weblogs: td_sdk_log = ...`, which exercises import visibility
  *inside* a model body — the case that fails today). Add `test _.columns should contain ...`
  assertions that only pass once the imported type resolves.
- **Negative**: `spec/neg/` cases for unresolved import, ambiguous wildcard, and an import cycle.
- **Validation**: `TyperValidationTest` compares Typer vs TypeResolver output on every `spec/` unit
  that uses imports, gating the eventual default cutover.

## Open Questions

1. **Visibility/exports** — do we want an explicit `export`/visibility modifier, or is "every
   top-level definition is importable" sufficient? (Assumed: all top-level defs importable.)
2. **Same-package implicit visibility** — files sharing a `package` currently see each other's names
   without an import. Keep this (Part 5 `enclosingPackageMember`) or require explicit imports even
   within a package? (Assumed: keep implicit same-package visibility.)
3. **Shadowing rules** — precedence among local def, single import, wildcard import, and package
   member. Proposed: local > single-import > package-member > wildcard-import > preset, with
   wildcard-vs-wildcard collisions being an error only when actually referenced.
