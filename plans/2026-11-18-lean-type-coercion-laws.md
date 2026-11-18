# Lean model of the typer's common-type and coercion rules

## Goals

- Take the `formal/` Lean project (PR #2176) past the activity state machine to a compiler property, small enough to prove fully in one PR (`lake build --wfail`, no `sorry`, no Mathlib).
- Tie the proofs to the shipped compiler code the same way the activity model is tied: Lean exports a JSON fixture, a Scala test replays it against the real compiler functions, and the `Formal` workflow fails on a stale fixture.
- Pick the first target from the three candidates in the request, based on what actually exists in `wvlet-lang`, and record the other two as follow-ups.

## Background

What each candidate looks like in `wvlet-lang` today:

- **(a) Expression simplification under three-valued logic: no such code exists.**
  - No phase folds constants or rewrites `AND`/`OR`/`NOT`. `RewriteExpr` handles only string concatenation, `s"..."` interpolation, and `if(...)` → `IfExpr`. `ExpressionEvaluator` only turns native-function results into literals.
  - The only NULL-sensitive rewrite is in codegen: `x = null` → `x is null` and `x != null` → `x is not null` (`SqlGenerator`, `WvletGenerator`). That is a deliberate choice of meaning, not an equivalence we could prove.
- **(b) Predicate pushdown and relation rewrites: no optimizer exists.**
  - The closest code is `SqlGenerator` joining stacked filter conditions with `Expression.concatWithAnd` into a single `WHERE`. That rewrite is sound under 3VL, because `WHERE` keeps a row only when `p AND q` is TRUE, which happens exactly when both are TRUE.
  - It lives inside SQL text generation, though. Replaying it would mean comparing generated SQL strings or running queries, which makes for a weak conformance check.
  - The other relation rewrites are `TrinoRewritePivot`, `HiveRewriteUnnest`, `Incrementalize`, and `RemoveUnusedQueries`. The first two translate dialects, so proving them would need a semantic model of the source and target SQL. The last two have no equivalence law simple enough for a first model.
- **(c) Type coercion and least-upper-bound laws: real, self-contained code.**
  - `typer/TypeInference.scala` (232 lines) holds the pure functions `findCommonType`, `unify`, `canCoerce`, and `widen`. They take no `Context`, have no side effects, and work over `Type` values that are easy to build in a test.
  - `findCommonType` is what types `CASE`/`WHEN` results (`TyperRules.caseExprRules`). `unify`, `canCoerce`, and `widen` are currently called only from `TyperTest`, but their docs say they are meant for binary operations, function arguments, and generic instantiation.
  - Reading the code turned up behavior that the laws will either pin down or expose:
    - `canCoerce` is not the order that `unify` is a least upper bound of. `canCoerce` lets Int and Boolean both become String, yet `unify(Int, Boolean)` is an error. Leastness holds only for the numeric widening order, minus the string-concatenation rule.
    - `unify(TypeVar a, TypeVar b)` returns `b`, so it is not commutative on type variables.
    - Neither `unify` nor `findCommonType` handles `DecimalType`, while arithmetic typing (`TyperRules`) keeps the decimal (`decimal + int → decimal`). So `CASE WHEN … THEN 1.5::decimal ELSE 1 END` is a type error, but `1.5::decimal + 1` is not. `DecimalType(p, s)` also has no common type across precisions.
    - No typer code handles `ShortType`, `ByteType`, `RealType`, `VarcharType`, or `CharType` (DataType.scala). So `unify(short, int)` and `unify(varchar(n), string)` are errors, which affects `CASE` over catalog columns.
    - `ErrorType` carries a message, and the message depends on argument order. So "commutative" has to mean "same type, or both errors".

## Design

Lane: Standard. It adds a new formal model and extends the `Formal` CI workflow, and the user asked to approve the direction first.

- **Target (c): the common-type laws of `TypeInference`.** It is the only candidate that exists as real, pure, self-contained code, and it feeds `CASE` typing today.
- **Model domain:** Null, NoType, Int, Long, Float, Double, Boolean, String, Short (standing in for the unhandled numeric types), one "other" type (e.g. Date), two Decimals with different precisions, varchar(10), plus a single `error` value.
  - Error messages are abstracted away. The replay maps the model's `error` to one fixed `ErrorType` instance and compares results by error-ness only.
  - The laws are claimed for this domain only. The other DataTypes fall into the same catch-all branches as Short and Date. `TypeVar` stays out of this first model, because it is a placeholder that the laws don't fit (see follow-ups).
- **Lean mirrors the Scala `match` order**, as the activity model does, so each case can be checked against the code by eye.
- **Theorems (all fully proved):**
  - `unify` is commutative, associative, and idempotent. NoType is its identity, Null is the identity on every type except NoType, and error absorbs.
  - `findCommonType xs = xs.foldl unify NoType` for lists without NoType, which is how `CASE` calls it (it filters NoType out first). So `CASE` typing doesn't depend on branch order and agrees with pairwise unification. With NoType present the two differ: `[Null, NoType]` gives NoType vs Null.
  - Upper bound: whenever `unify t u = r` is not an error, `canCoerce t r` and `canCoerce u r`.
  - Least: for any other `r'` that both widen to, without the string rule, `r` widens to `r'`. The theorem states the string-rule caveat precisely instead of hiding it.
  - `widen t u` is `u` exactly when `canCoerce t u` holds, and an error otherwise.
- **Decimal and unhandled numerics:** the model records today's behavior (no common type with the other numerics) and proves the laws with it. Fixing it in Scala would change `CASE` typing, which this PR does not do. It is listed as a follow-up.
- **Conformance:**
  - A second `lake exe` (e.g. `typer-traces`) writes a fixture under the wvlet-lang JVM test resources. It holds every `unify`, `canCoerce`, and `widen` pair over the domain, and `findCommonType` over every list up to length 3, about 1–2k cases.
  - A new `TypeCoercionConformanceTest` in `langJVM`, shaped like `ActivityModelConformanceTest`, replays the fixture against the real `TypeInference`. It compares error-ness rather than messages.
- **CI and docs:**
  - The `Formal` workflow also runs the new exe and checks the new fixture's freshness.
  - Its path filter gains `TypeInference.scala` and the new fixture directory.
  - `formal/README.md` gets a layout row and a sync rule. CLAUDE.md gets a line: change `TypeInference` by updating the model first.
- **ADR:** none needed. The pattern (model, JSON fixture, Scala replay, freshness check) is unchanged, and the 2026-11-18 ADR already names coercion laws as a next target. The PR #2176 ADR stays untouched; `formal/README.md` records the second model.
- **No Scala behavior change in this PR.** If a law turns out to fail on current behavior, I'll stop and bring it to you rather than quietly changing the typer.

## Follow-ups

- **Type variables in `unify`:** commutativity up to renaming, once generic instantiation actually calls `unify`.
- **Decimal and other numeric/string types:** give `unify`/`findCommonType` common types for decimals across precisions, Short/Byte/Real, and Varchar/Char, in line with arithmetic typing. Then extend the model.
- **Arithmetic result typing:** prove that the `ArithmeticBinaryExpr` table in `TyperRules` agrees with `unify` on numeric operands.
- **(b) Filter merging:** prove `filter p (filter q R) = filter (p AND q) R` under 3VL, which would pay off once a real plan-level rewrite or optimizer exists.
- **(a) 3VL simplification:** prove a Kleene-logic rewrite set ahead of adding constant folding, so the rules are proved sound before they ship.

## Alternatives and Why Not?

- **Start with 3VL simplification (a).** There is no Scala code to tie the proofs to. It would be a model without conformance, which goes against the ADR's rule.
- **Start with filter merging (b).** The property is trivial, and the code sits in SQL text generation, so the replay would compare strings rather than semantics.
- **Prove the laws on Scala-generated cases with no Lean (property tests).** That samples the domain, while the Lean proof covers the model completely and the fixture pins the code to it exhaustively over a finite domain.
