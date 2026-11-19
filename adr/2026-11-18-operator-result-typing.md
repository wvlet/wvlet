# ADR: Operator result types check errors, then deferral, then value rules

Date: 2026-11-18 · PR #2185

## Context

`TyperRules` types sign prefixes, arithmetic, `=` / `!=`, ordering comparisons, and `and` / `or`. Each rule was one `match` whose cases mixed value rules (numeric promotion, decimals, string `+`) with deferral (an operand not typed yet) and error propagation, so case order decided the outcome. The Lean model in `formal/WvletFormal/Typer/Operators.lean` showed where that order broke the intended laws: string `+` and comparisons hid error operands, NoType beat errors, `1 + null` was a type mismatch, unresolved decimals committed early, and `decimal(10,2) + decimal(20,4)` typed differently from its reverse.

The typer depends on these results: a node with a resolved type counts as typed and is not revisited, and an `ErrorType` over concrete scalars becomes a TypeMismatch diagnostic (fatal under `failOnTypeErrors`).

## Decision

- Every operator family runs the same stages: an error operand propagates; then an operand that is not typed yet (`isDeferredOperand`: unresolved types, including NoType and type variables, and function references) defers the result; only then do the value rules apply.
- Deferral gives NoType for arithmetic and `and` / `or`. Comparisons are boolean-valued whatever the operands, so they give `boolean`.
- On numeric and NULL operands, arithmetic is `TypeInference.unify`, so arithmetic and `CASE` / `findCommonType` promote numbers the same way.
- Two decimals type as the smallest decimal holding both: the larger scale, the larger integer part, capped at 38 digits (integer digits are what the cap gives up). A decimal with a number or NULL keeps the decimal. Engines still compute their own SQL result precision; Wvlet's type only has to be independent of operand order and hold both operands.
- `=` / `!=` stay boolean for any operands, errors included, for now (a follow-up).

## Consequences

- The laws are proved in Lean (commutativity, `unify` on numerics, error propagation, deferral, decimal bounds) and replayed against the Scala functions by `OperatorTypingConformanceTest` from `lake exe operator-traces`. A rule change starts in the Lean model.
- Errors under string `+` and comparisons now reach the enclosing expressions instead of turning into `string` or `boolean`; diagnostics are still reported only where the error originates.
- `TyperCoverageCheck` was unchanged by the fixes (5860/6504 typed expressions before and after).
