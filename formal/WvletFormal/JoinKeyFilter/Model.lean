/-
Licensed under the Apache License, Version 2.0.

A model of carrying a filter across the keys of a hash join (`join_key_filters` in the engine's
`PushDownFilters`).

Rows are lists, so the model keeps duplicates and order (bags). A row has one join key, a SQL value
that may be NULL, and an id. A hash join matches two rows when their keys are equal and not NULL and
an arbitrary residual condition holds. A filter conjunct is any function of a key value to a
three-valued truth (TRUE, FALSE, UNKNOWN); a filter keeps the rows where it is TRUE.

The rule: a conjunct over the key of the preserved input, pushed into that input, is also applied
to the other input over that input's key. `Sound.lean` proves that it keeps the filtered join's
rows for inner, LEFT, RIGHT, semi, and anti joins.

The same model covers runtime join filters (`runtime_join_filters`, `row_join_filters` in the
engine's `Lowering`): `Probe.lean` proves which join inputs may drop rows whose key is no key of
the other input.
-/

namespace Wvlet.JoinKeyFilter

/-- SQL three-valued truth. -/
inductive Truth where
  | tt
  | ff
  | unk
  deriving DecidableEq, Repr

/-- A key value: `none` is SQL NULL. -/
abbrev Val := Option Int

/-- A filter conjunct over one key column: any deterministic function of the key's value. -/
abbrev Pred := Val → Truth

/-- Whether a filter keeps a row whose key is `v`: only a TRUE conjunct passes. -/
def holds (p : Pred) (v : Val) : Bool := p v == .tt

/-- Hash join key equality: never true for a NULL key. -/
def keyEq : Val → Val → Bool
  | some a, some b => a == b
  | _, _ => false

/-- A row of a join input. -/
structure Row where
  key : Val
  id : Nat
  deriving DecidableEq, Repr

/-- The join kinds the rule applies to. -/
inductive Kind where
  | inner
  | left
  | right
  | semi
  | anti
  deriving DecidableEq, Repr

/-- An output row: the left row and the right row, either one missing for an outer join's padded
rows (a semi or anti join has no right row). -/
abbrev Out := Option Row × Option Row

/-- Residual join condition over a left row and a right row. -/
abbrev Residual := Row → Row → Bool

/-- The rows of `rs` that match `l`. -/
def matching (θ : Residual) (rs : List Row) (l : Row) : List Row :=
  rs.filter fun r => keyEq l.key r.key && θ l r

/-- The output rows for one left row `l`, given the right rows that match it. -/
def rowsOf : Kind → Row → List Row → List Out
  | .inner, l, ms => ms.map fun r => (some l, some r)
  | .left, l, [] => [(some l, none)]
  | .left, l, ms@(_ :: _) => ms.map fun r => (some l, some r)
  | .semi, _, [] => []
  | .semi, l, _ :: _ => [(some l, none)]
  | .anti, l, [] => [(some l, none)]
  | .anti, _, _ :: _ => []
  -- A RIGHT join is a LEFT join with the inputs swapped (see `join`)
  | .right, _, _ => []

/-- Swap the sides of an output row. -/
def swap (o : Out) : Out := (o.2, o.1)

/-- A join of `ls` and `rs`. A RIGHT join is the LEFT join of `rs` and `ls`, sides swapped. -/
def join (θ : Residual) : Kind → List Row → List Row → List Out
  | .right, ls, rs => ((rs.flatMap fun r => rowsOf .left r (matching (fun a b => θ b a) ls r))).map swap
  | k, ls, rs => ls.flatMap fun l => rowsOf k l (matching θ rs l)

/-- The key of an output row's left row, NULL for a padded one. -/
def leftKey (o : Out) : Val := o.1.bind Row.key

/-- The key of an output row's right row, NULL for a padded one. -/
def rightKey (o : Out) : Val := o.2.bind Row.key

/-- Rows of an input for which the conjunct is TRUE. -/
def keep (p : Pred) (rs : List Row) : List Row := rs.filter fun r => holds p r.key

end Wvlet.JoinKeyFilter
