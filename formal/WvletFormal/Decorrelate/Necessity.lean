/-
Licensed under the Apache License, Version 2.0.

Why the decorrelated plans have their shapes: inputs where a simpler plan returns rows other than
SQL's. The outer row has column 0 (`b`); the subquery's rows have column 1 (`k`), correlated by
`k = b`.
-/
import WvletFormal.Decorrelate.Rule

namespace Wvlet.Decorrelate.Necessity

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

private def L : List Col := [0]
private def b : Term := .col 0
private def k : Term := .col 1

/-- The correlation `k = b`. -/
private def corr : Cond := .eq k b

private def outer (v : Value) : Row := fun i => if i = 0 then v else .null
private def inner (v : Value) : Row := fun i => if i = 1 then v else .null

/-- EXISTS as an inner join returns an outer row once per match: SQL returns it once. -/
theorem inner_join_duplicates :
    (innerJoin L corr [outer (.int 1)] [inner (.int 1), inner (.int 1)]).length = 2 ∧
      (whereExists L corr false [outer (.int 1)] [inner (.int 1), inner (.int 1)]).length = 1 := by
  decide

/-- Removing the duplicates of that inner join also removes duplicate outer rows, which SQL
keeps. -/
theorem inner_join_distinct_loses_duplicates :
    (dedup ((innerJoin L corr [outer (.int 1), outer (.int 1)] [inner (.int 1)]).map (· 0))).length
        = 1 ∧
      (whereExists L corr false [outer (.int 1), outer (.int 1)] [inner (.int 1)]).length = 2 := by
  decide

/-- The mark's left join needs the distinct keys of the body: over the body itself, an outer row
with two matches appears twice. -/
theorem mark_without_distinct_duplicates :
    (leftJoin L corr [outer (.int 1)] [inner (.int 1), inner (.int 1)]).length = 2 ∧
      (markJoin [(b, k)] [inner (.int 1), inner (.int 1)] [outer (.int 1)]).length = 1 := by
  decide

/-- Under OR, EXISTS cannot be a semi join, whose output has only the rows with a match:
`b is null or exists (...)` keeps an outer row without one. -/
theorem semi_join_under_or_drops :
    (semiJoin L corr [outer .null] [inner (.int 1)]).length = 0 ∧
      ([outer .null].filter fun x =>
        ((Cond.isNull b).eval x).or (if existsSql L corr x [inner (.int 1)] then .true else .false)
          == .true).length = 1 := by
  decide

/-- NOT EXISTS is not NOT IN: `not exists (from r where k = b)` keeps a NULL `b` and keeps `b`
when the keys have a NULL, both of which `b not in (from r select k)` drops. -/
theorem not_exists_is_not_not_in :
    (whereExists L corr true [outer .null] [inner (.int 1)]).length = 1 ∧
      (notIn b 1 [outer .null] [inner (.int 1)]).length = 0 ∧
      (whereExists L corr true [outer (.int 2)] [inner (.int 1), inner .null]).length = 1 ∧
      (notIn b 1 [outer (.int 2)] [inner (.int 1), inner .null]).length = 0 := by
  decide

end Wvlet.Decorrelate.Necessity
