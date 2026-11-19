# Prove the typer's operator typing rules in Lean

## Goals

- Machine-check the result types that `TyperRules.binaryOpRules` assigns to sign prefixes, arithmetic (`+ - * / %`), equality (`=`, `!=`), ordering comparisons (`< <= > >=`), and `and` / `or`.
- Tie the arithmetic result table to `unify` from the coercion model (#2177), closing the formal/README.md follow-up "Tie the TyperRules arithmetic result table to unify".
- Replay the model against the shipped rules through a Lean-exported fixture, so the table cannot drift from the proofs.
- Fix the operand combinations where today's rule order breaks the laws, if the fixes stay small.

## Background

- Operator result types feed `collectTypeMismatches`: an arithmetic or conditional node whose type is an `ErrorType` over concrete scalar operands becomes a TypeMismatch diagnostic (a warning by default, fatal under `failOnTypeErrors`). A wrong result type is therefore either a false diagnostic or a missed one.
- A node with a resolved result type counts as typed, so a rule that commits a concrete type while an operand is still untyped is not revisited when the operand later turns into an error.
- `ErrorType` and `NoType` are both unresolved (`isResolved = false`), and they are `Type`s, not `DataType`s. The deferral guards (`!isResolved`) decide whether errors reach the propagation cases below them.
- Findings from reading the rules (each will be replayed by the fixture):
  - **Confirmed: decimal arithmetic depends on operand order.** `decimal(10,2) + decimal(20,4)` types as `decimal(10,2)` and the reverse as `decimal(20,4)`; neither holds both operands. Decimal with int/long/float/double keeps the decimal and is symmetric.
  - **Confirmed: string `+` swallows errors and deferral.** The string rule precedes deferral and error propagation, so `'a' + <error>` is `string` (the error, and its diagnostic, are lost) and `'a' + <untyped>` commits to `string` early.
  - **Not confirmed: numeric promotion vs `unify`.** On int/long/float/double the arithmetic table equals `unify` exactly (int/long + float is float in both).
  - **New: comparisons swallow errors.** In `< <= > >=` the `!isResolved` deferral guard matches `ErrorType` first, so the error-propagation cases are dead code and `<error> < 1` is `boolean`. `=` and `!=` are always `boolean`, errors included.
  - **New: NULL operands are type errors in arithmetic.** `1 + null` falls through to the mismatch case and reports a TypeMismatch on concrete scalars, while `unify(int, null)` is `int` and SQL evaluates it to NULL.
  - **New: unresolved decimals commit early.** The decimal cases precede the `!isResolved` guard, so `decimal(p, s) + int` with unbound `p`, `s` returns that decimal instead of NoType.
  - **New: NoType beats errors.** In arithmetic and `and` / `or`, `<error> + <untyped>` is NoType, whereas `unify` lets the error absorb NoType.

## Design

Lane: Standard — the fixes change compiler diagnostics, and the user asked to gate on the direction.

- New Lean module `WvletFormal/Typer/Operators.lean`, separate from the other in-flight models. It imports `Coercion.lean` and reuses its `Ty` and `unify`; no second type lattice.
- Operand domain: `Ty` plus what the operator rules distinguish beyond it: `decimal p s`, an unresolved decimal (unbound precision or scale), an unresolved data type (unknown column, unbound type parameter), and a function reference. `other n` stands for the remaining types no rule mentions (short, date, varchar, …).
- The model is a function per operator family that mirrors the Scala `match`, case by case, so rule order is part of what is proved.
- Theorems:
  - Commutativity of the result type for every binary operator (the table ignores operand order, so `-`, `/`, `%` are symmetric too).
  - Numeric arithmetic is `unify`: on int/long/float/double (and NULL, after the fix) the result is `unify l r`, hence the least upper bound of the operands.
  - Error propagation: an error operand yields an error, for every operator family.
  - Deferral: an untyped or unresolved operand of arithmetic, `and`, or `or` (and no error operand) yields NoType. Comparisons are boolean-valued whatever their operands, so for them deferral means "never an error, always boolean"; their result does not commit to anything an operand could change.
  - Decimal results hold both operands (after the fix): scale and integer digits bound both sides when the sum is at most 38; past the cap the scale is kept and integer digits are what is given up.
  - Rule-order independence is the conjunction of the above: each law is stated over the whole function, so no earlier case can shadow it.
- Refactor (no behaviour change, its own commit): extract pure functions from `binaryOpRules` (`arithmeticType`, `comparisonType`, `equalityType`, `logicalType`) taking an operator and two `Type`s, so the test replays them without building expressions.
- Conformance: `lake exe operator-traces` writes `operator-traces.json` next to `typer-traces.json` (every operator × every pair of a finite sample: the `Ty` constructors, two `other` types, three decimals including one with a large precision near the cap, one unresolved decimal, one unresolved type, one function reference); `OperatorTypingConformanceTest` (langJVM) binds each model type to a concrete Scala type and compares errors by error-ness. The `Formal` workflow gets a freshness step and watches `TyperRules.scala`.
- Proposed fixes (one commit after the refactor, so the fixture diff shows each change):
  - Errors first, then deferral, then the string rule, in arithmetic, ordering comparisons, and `and` / `or`.
  - `=` / `!=` propagating an error operand is a separate item: today they are always boolean, and they are the most common predicate.
  - NULL with a numeric or decimal operand types as the other operand (as `unify` does for numerics); `null + null` stays NULL.
  - An unresolved decimal operand defers to NoType, like any other unresolved type.
  - `decimal(p1,s1) ⊕ decimal(p2,s2)` types as the smallest decimal holding both: scale `max(s1,s2)`, integer digits `max(p1-s1, p2-s2)`, precision capped at 38. Engines still compute their own SQL result precision; this only stops the Wvlet type from depending on order.
- Coverage ratchet: today a comparison or `'a' + …` over an unresolved identifier (typed `ErrorType`) counts as typed in `TyperCoverageCheck`; propagating the error makes it untyped. Run the check before and after the fix commit. The thresholds are never lowered, so a fix that drops coverage below them moves to a follow-up and the model records today's behaviour for that operator.
- Docs: formal/README.md layout table, commands, and known gaps; the ADR index gets no new record (the setup ADR already covers this pattern).

## Alternatives and Why Not?

- **Model and prove today's behaviour only, with all fixes as follow-ups.** Keeps the PR behaviour-neutral, but the error, deferral, and commutativity laws would have to be stated with exceptions that just restate the bugs.
- **Extend `Ty` in `Coercion.lean` with decimals and unresolved types.** Reopens #2177's proofs and fixture for a distinction `unify` does not make.
- **SQL-standard decimal precision growth (`p1 + p2` for `*`, etc.).** Engine-specific, and nothing in Wvlet consumes the precision yet; the "holds both" rule fixes the order dependence with far less surface.
