# Key the planner's OR factoring by the full expression

## Goals

- Make the compiler-side `Planner.factorOr` take a conjunct out of an `or` only when the conjunct really is in every branch, so join and subquery conditions keep SQL's rows.
- Replay the factoring paths the FactorOr conformance test skipped (outer-join ON, inner-join ON, WHERE over a left join through `Planner.conjuncts`, a correlated EXISTS body) against the Lean model and DuckDB. Today it covers only WHERE through `Normalize`.

## Background

- Two copies of the same `factor` rule exist. `Normalize` factors bound conditions keyed by `VExpr.sig`; PR 11 and PR 14 checked that key. `Planner.factorOr` factors compiler expressions before binding, keyed by `exprName`. That function names output columns, and nothing replays it.
- `exprName` drops a column's qualifier: `l.b` and `r.b` both key as `b`. So the planner treats `(l.b and l.k = 1) or (r.b and l.k = 1)` as having `l.b` in common and rewrites it to `l.b and l.k = 1`.
- Reproduced: a WHERE over a left join that also has an EXISTS conjunct goes through `Planner.conjuncts` and returns different rows from DuckDB. Plain ON or WHERE conditions bind unfactored and `Normalize` factors them, so they come out right today. The same key is also used for lateral unnest ON, ASOF join conditions, and the bodies of correlated subqueries.
- `printSql` (the generator's text) doubles quotes (`'it''s'`), keeps `'null'` apart from NULL, and prints LIKE's ESCAPE. But it collapses whitespace over the whole text, so `'x  y'` and `'x y'` would print alike.

## Design

Lane: Express. A bug fix with a clear diagnosis, plus model and test work. No always-gate area.

- Engine: `Planner.factorOr` keys conjuncts by the SQL generator's text without collapsing whitespace, not `exprName`. It keeps qualifiers, literal quoting and whitespace, and ESCAPE. A key that differs where it doesn't need to only skips a factoring, which is always safe.
- Lean: a necessity lemma (`oldExprName_drops_qualifier`): a key that treats two columns as one factors `(b and a = 1) or (c and a = 1)` to `b and a = 1` and drops a row SQL keeps.
- Traces: render a TRUE comparison of a boolean column as the bare column (`b`, as people write it), add the case above and a whitespace case (`'x  y'` against `'x y'`), and export each condition a second time over a join. In that version `b` and `c` are columns of two inputs with the same name (`l.v`, `r.v`), so the old key would confuse them in every case.
- Replay: run each traced join condition in each context, checked against the model's rows (or the rows they imply for an outer join) and DuckDB, with FactorOr on and off:
  - through `Normalize`: a plain inner-join ON and outer-join ON;
  - through `Planner.factorOr`: a WHERE over a left join with an EXISTS conjunct, an outer-join ON with an EXISTS conjunct, an inner-join WHERE with an always-TRUE `exists` in every branch, and a correlated EXISTS body.
- Lateral unnest ON and ASOF conditions use the same key but stay unreplayed; README records them as gaps.
- README: close the two known gaps the replay now covers.

## Alternatives and Why Not?

- **Make `exprName` keep qualifiers.** It names output columns (`select l.b` is named `b`), so that would change column names.
- **Drop `Planner.factorOr` and leave all factoring to `Normalize`.** Join-key extraction in `joinOn` and subquery planning need the factored conjuncts before binding. That is a bigger planner change.
