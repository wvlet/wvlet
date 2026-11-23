/-
Licensed under the Apache License, Version 2.0.

Why `place` keeps a conjunct above: for each refusal, inputs where moving the conjunct anyway
changes the rows. The inputs are one row per side, left column 0 (`a`) and right column 1 (`c`),
joined on `a = c`.
-/
import WvletFormal.Pushdown.Rule

namespace Wvlet.Pushdown.Necessity

open Wvlet.Algebra
open Wvlet.Rewrite (Value)

private def L : List Col := [0]
private def a : Term := .col 0
private def c : Term := .col 1
private def on : Cond := .eq a c

/-- A row with `a` (column 0) and `c` (column 1). -/
private def row (va vc : Value) : Row := fun i => if i = 0 then va else if i = 1 then vc else .null

private def ls : Rel := [row (.int 1) .null]
private def rs : Rel := [row .null (.int 1)]

/-- Below a LEFT join, `c is null` on the right input drops the match, and the left row comes
back padded with NULLs, which satisfies the conjunct: one row instead of none. -/
theorem left_null_side :
    (leftJoin L on ls (filter (.isNull c) rs)).length = 1 ∧
      (filter (.isNull c) (leftJoin L on ls rs)).length = 0 := by
  decide

/-- Even a conjunct that is never TRUE on NULLs (`c = 2`) can't move into the null-supplying
input: the left row then survives padded instead of being dropped. -/
theorem left_null_side_rejecting :
    (leftJoin L on ls (filter (.eq c (.lit (.int 2))) rs)).length = 1 ∧
      (filter (.eq c (.lit (.int 2))) (leftJoin L on ls rs)).length = 0 := by
  decide

/-- The RIGHT join counterpart: a conjunct over the left input. -/
theorem right_null_side :
    (rightJoin L on (filter (.eq a (.lit (.int 2))) ls) rs).length = 1 ∧
      (filter (.eq a (.lit (.int 2))) (rightJoin L on ls rs)).length = 0 := by
  decide

/-- Below a FULL join, a conjunct over the left input that drops the left row turns the right
row's match into a padded right row. -/
theorem full_left :
    (fullJoin L on (filter (.eq a (.lit (.int 2))) ls) rs).length = 1 ∧
      (filter (.eq a (.lit (.int 2))) (fullJoin L on ls rs)).length = 0 := by
  decide

/-- And symmetrically for a conjunct over the right input. -/
theorem full_right :
    (fullJoin L on ls (filter (.eq c (.lit (.int 2))) rs)).length = 1 ∧
      (filter (.eq c (.lit (.int 2))) (fullJoin L on ls rs)).length = 0 := by
  decide

/-- Moving a conjunct over the null-supplying input into the ON condition of a LEFT join is no
better: the unmatched left row is kept padded. -/
theorem left_on :
    (leftJoin L (.and on (.eq c (.lit (.int 2)))) ls rs).length = 1 ∧
      (filter (.eq c (.lit (.int 2))) (leftJoin L on ls rs)).length = 0 := by
  decide

end Wvlet.Pushdown.Necessity
