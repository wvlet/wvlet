/-
Licensed under the Apache License, Version 2.0.

Why NOT IN needs a null-aware anti join: inputs where the plain `NOT EXISTS` anti join on
`x = y`, or a binding without one of the engine's checks, returns rows other than SQL's. The
target is left column 0 (`a`); the subquery's value is right column 1 (`c`).
-/
import WvletFormal.NotIn.Rule

namespace Wvlet.NotIn.Necessity

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

private def L : List Col := [0]
private def a : Term := .col 0

private def left (v : Value) : Row := fun i => if i = 0 then v else .null
private def right (v : Value) : Row := fun i => if i = 1 then v else .null

/-- A plain anti join keeps a NULL target, which no value compares FALSE with:
`NULL NOT IN (1)` is unknown. -/
theorem anti_keeps_null_target :
    (antiJoin L (keyEq a 1) [left .null] [right (.int 1)]).length = 1 ∧
      (notIn a 1 [left .null] [right (.int 1)]).length = 0 := by
  decide

/-- A plain anti join keeps a row that matches no value when the subquery has a NULL:
`2 NOT IN (1, NULL)` is unknown. -/
theorem anti_keeps_with_null_value :
    (antiJoin L (keyEq a 1) [left (.int 2)] [right (.int 1), right .null]).length = 1 ∧
      (notIn a 1 [left (.int 2)] [right (.int 1), right .null]).length = 0 := by
  decide

/-- Dropping NULL targets after the anti join is still not enough without the check for a NULL
value in the subquery. -/
theorem anti_not_null_keeps_with_null_value :
    (filter (.not (.isNull a))
        (antiJoin L (keyEq a 1) [left (.int 2)] [right (.int 1), right .null])).length = 1 ∧
      (notIn a 1 [left (.int 2)] [right (.int 1), right .null]).length = 0 := by
  decide

/-- Dropping NULL targets over an empty subquery is wrong: `NULL NOT IN (empty)` is TRUE. The
engine did this before the empty check in `uncorrelated`. -/
theorem not_null_drops_over_empty :
    (filter (.not (.isNull a)) (antiJoin L (keyEq a 1) [left .null] [])).length = 0 ∧
      (notIn a 1 [left .null] []).length = 1 := by
  decide

/-- `InList` before the fix: a NULL target is unknown even over an empty set. -/
def inListUnknownOnNull (v : Value) (vs : List Value) : Tri :=
  if v = .null then .unknown else inList v vs

/-- `NULL NOT IN (empty)` is TRUE, not unknown. -/
theorem null_over_empty_set :
    (inListUnknownOnNull .null []).not = .unknown ∧ (inTri .null []).not = .true := by
  decide

end Wvlet.NotIn.Necessity
