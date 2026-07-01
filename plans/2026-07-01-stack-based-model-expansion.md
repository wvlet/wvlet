# Stack-based model expansion in GenSQL (via the Context outer chain)

## Context

When converting Wvlet's LogicalPlan to SQL, `GenSQL.expand` (`wvlet-lang/src/main/scala/wvlet/lang/compiler/codegen/GenSQL.scala:297-406`) inlines referenced `model` bodies and substitutes model arguments. The current implementation doesn't use any stack discipline:

1. **Wrong-scope substitution**: after all models are inlined (`transformUp { case m: ModelScan => ... }`), a trailing global pass `.transformOnce { transformExpr(r, ctx) }` re-evaluates identifiers over the *entire* tree — including already-inlined model bodies — using the **outer** context. An outer `val` sharing a name with a model parameter/column can be wrongly substituted into a model body.
2. **Verbatim arg binding**: `transformModelScan` binds `arg.value` verbatim into the callee scope (TODO at line 373). If a model passes its own parameter (or a caller-local binding) as an argument to a nested model, the nested expansion resolves it in the wrong frame. Concrete broken case: `val n = 1` + `from m(n)` where `m(id)` uses `` s`tbl_${id}` `` backquote interpolation — produces `tbl_n` instead of `tbl_1`.
3. **No cycle detection**: `model a = { from a }` or mutual recursion → `StackOverflowError` instead of a user-facing error.

Fix: invert to **substitute-first, then inline**, and use the existing dotc-style `Context` outer chain as the expansion stack — `ctx.newContext(modelSym)` already sets `outer = this, owner = modelSym` (`Context.scala:205-215`), so the chain of enclosing contexts *is* the active expansion path. No new frame structure is introduced; cycle detection, depth, and the cycle-path message all derive from the chain, and the same mechanism generalizes to future function/macro expansion.

## Design

Logic changes live in `GenSQL.scala` plus a small dotc-parity helper on `Context`. Public API (`expand(relation, ctx)`) keeps its exact signature.

### 1. New status code

`wvlet-api/src/main/scala/wvlet/lang/api/StatusCode.scala` — add after `PARTIAL_QUERY_INVALID_BODY`:

```scala
case RECURSIVE_MODEL_REFERENCE extends StatusCode(StatusType.UserError)
```

Used for both cycle detection and the max-depth safety net. `isUserError` is what `RunnerSpecNeg.handleError` accepts (`wvlet-runner/src/test/scala/wvlet/lang/runner/RunnerSpec.scala:105-111`).

### 2. Context helper (dotc parity)

`wvlet-lang/src/main/scala/wvlet/lang/compiler/Context.scala`:

```scala
/** Iterate this context and its enclosing (outer) contexts, innermost first */
def outersIterator: Iterator[Context] =
  Iterator.iterate(this)(_.outer).takeWhile(_ ne Context.NoContext)
```

### 3. Restructure `GenSQL.expand`

```scala
private inline val maxModelExpansionDepth = 100

def expand(relation: Relation, ctx: Context): Relation = expandRelation(relation)(using ctx)

private def expandRelation(relation: Relation)(using ctx: Context): Relation =
  // 1) Substitute identifiers bound in the current context (vals, model args) and evaluate
  //    backquote interpolations exactly once for this region. Because ModelScan is a LeafPlan,
  //    unexpanded model bodies are not in the tree yet, and ModelScan.modelArgs ARE in the tree —
  //    so arg values get resolved in the caller's context before binding.
  val substituted = substituteContextBindings(relation, ctx)
  // 2) Inline model scans; each expansion pushes a new Context via ctx.newContext(modelSym)
  substituted.transformUp { case m: ModelScan =>
    lookupType(Name.termName(m.name.name), ctx) match
      case Some(sym) => TypeResolver.resolve(expandModelScan(m, sym), ctx)
      case None      => warn(s"unknown model: ${m.name}"); m
  }.asInstanceOf[Relation]
```

- `substituteContextBindings` = current nested `transformExpr` (lines 300-350) hoisted to a private method **verbatim** — keep the `tableRefQualifiers` DotRef-qualifier skip logic and the `BackquoteInterpolatedIdentifier`/`Identifier` rules unchanged.
- **Delete** the trailing `.transformOnce { transformExpr(r, ctx) }` pass (defect-1 fix). Top-level vals are still substituted because `substituteContextBindings` runs at the start of `expandRelation` on the top-level region.
- `transformModelScan` → `expandModelScan(m, sym)(using ctx)`:
  - **Cycle check first**: if `ctx.outersIterator.exists(_.owner eq sym)` (including `ctx` itself), the model is already being expanded on the active path. Cycle path from the chain:
    `val path = (ctx.outersIterator.toList.reverse.collect { case c if c.owner.isModelDef => c.owner.name.name } :+ m.name.name).mkString(" -> ")` → throw `StatusCode.RECURSIVE_MODEL_REFERENCE.newException(s"Recursive model reference detected: ${path}", ctx.sourceLocationAt(m.span))` (yields `a -> b -> a`). `Symbol.isModelDef` exists (`Symbol.scala:96`).
  - **Depth guard**: if `ctx.outersIterator.count(_.owner.isModelDef) >= maxModelExpansionDepth`, same status code, "exceeded the maximum depth" message.
  - Then as today: `newCtx = ctx.newContext(sym)` (pushes the frame), bind each arg as `ValSymbolInfo(expr = arg.value)` into `newCtx.scope` — arg values are already caller-resolved by step 1, so remove the stale TODOs (lines 356, 373) and the dead `given Context = ctx` (line 366).
  - Recurse `expandRelation(md.child.body)(using newCtx)` — replaces the separate `transformExpr(md.child.body, newCtx)` + `expand(...)` calls, since `expandRelation` substitutes first.
- Bindings live where they always did — in the `Context`/`Scope` chain (child scope snapshots outer entries and prepends new ones, so arg bindings shadow same-named outer vals). No parallel data structure.
- `ctx.sourceLocationAt(span)` exists (`Context.scala:130`).
- Caveat checked: model-symbol owners appear on the outer chain *only* while their expansion is active (the chain unwinds when `expandRelation(...)(using newCtx)` returns), so a diamond reference (`model c = { from a join a }`) is NOT a false-positive cycle.

### 4. New spec files

- `spec/basic/model/model-nested-args.wv` — chain `model filter_via2(b) -> filter_via(bound = b) -> filter_min(min_x = bound)` over a VALUES-based model; `test _.rows` assertions; plus passing a top-level `val` as a model arg. Include a backquote-interpolation variant where the arg comes from a `val` instead of a literal (the concrete currently-broken case; see `spec/basic/backquote-interpolation.wv` for the existing pattern).
- `spec/basic/model/model-shadowing.wv` — outer `val bound = 100` plus `model adults(bound: int)` called with `bound = 18`; assert rows reflect 18 not 100.
- `spec/neg/recursive-model.wv` — `model model_self = { from model_self }` + `from model_self`.
- `spec/neg/recursive-model-mutual.wv` — `model_a -> model_b -> model_a` (separate file so one failure doesn't mask the other).

Note: `RunnerSpecNeg` passes on *any* user-error `WvletLangException`, so the neg specs prove "no StackOverflowError"; the specific status code is asserted in the unit test.

### 5. Unit test

`wvlet-lang/src/test/scala/wvlet/lang/compiler/codegen/ModelExpansionTest.scala` (UniTest, following `TyperValidationTest` compile pattern):

```scala
private def generateSQL(wv: String): String =
  val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
  val unit     = CompilationUnit.fromWvletString(wv)
  val result   = compiler.compileSingleUnit(unit)   // CompileResult.context is available
  GenSQL.generateSQL(unit)(using result.context)
```

Tests: (1) self-recursive model → `WvletLangException` with `StatusCode.RECURSIVE_MODEL_REFERENCE`, message contains cycle path; (2) mutual recursion → same; (3) nested param pass-through → generated SQL contains resolved literal, not a param name; (4) shadowing → SQL contains `18`, not `100`.

## Files to modify

- `wvlet-lang/src/main/scala/wvlet/lang/compiler/codegen/GenSQL.scala` — main restructure
- `wvlet-lang/src/main/scala/wvlet/lang/compiler/Context.scala` — `outersIterator` helper
- `wvlet-api/src/main/scala/wvlet/lang/api/StatusCode.scala` — new status code
- `wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/TypeResolver.scala` — only if Risk 1 requires a compile-time guard in `resolveModelScan`
- New: 4 spec files + `ModelExpansionTest.scala`

## Risks

1. **Recursive models may overflow during compile (TypeResolver), before GenSQL.** Run the neg spec against unmodified code first to locate where the overflow happens; if compile-time, add a matching guard in `resolveModelScan` (`TypeResolver.scala:373-388`).
2. **Ordering change**: substitution now precedes expansion at top level. Watch `spec/basic/backquote-interpolation.wv`, `spec/basic/model/*.wv`, and TPC-H for regressions.
3. **`tableRefQualifiers` becomes per-region** (computed pre-expansion per frame) — strictly more precise, but run `spec/basic` + `spec/sql` suites to confirm no behavior change.
4. **Dynamic scoping remains** (model bodies still see caller vals via scope snapshot). Pre-existing behavior, out of scope; note as follow-up in PR description.
5. **Owner-chain assumptions**: cycle detection relies on model symbols appearing as `Context.owner` only during active expansion in the GenSQL path. Verified for `newContext`/`withImport` call sites; the unit tests (diamond-safe positive specs + cycle negatives) guard this.

## Verification

```bash
./sbt "langJVM/testOnly *ModelExpansionTest"
./sbt "runner/testOnly *RunnerSpecNeg*"
./sbt "runner/testOnly *RunnerSpecBasic*"
./sbt "runner/testOnly *RunnerSpecTPCH*"
./sbt "langJVM/test"
./sbt "projectJS/Test/compile" && ./sbt "projectNative/Test/compile"   # cross-platform compile check
./sbt scalafmtAll
```

Sequencing: status code → write neg specs and run against unmodified code (confirm overflow location, Risk 1) → Context helper + GenSQL restructure → positive specs + unit test → full verification.

## Workflow

Per CLAUDE.md: create branch `fix/<timestamp>-stack-based-model-expansion` off origin/main in this worktree, copy this plan to `./plans/2026-07-01-stack-based-model-expansion.md`, implement, then PR via `gh pr create` and iterate on Gemini review + CI. No merge without explicit approval.
