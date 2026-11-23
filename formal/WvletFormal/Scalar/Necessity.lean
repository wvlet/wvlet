/-
Licensed under the Apache License, Version 2.0.

Why `decorrelateScalar`'s plan has its shape: inputs where a simpler plan gives an outer row a
value other than SQL's, or loses or duplicates outer rows. The outer row has column 0 (`b`); the
subquery's rows have column 1 (`k`), correlated by `k = b`.
-/
import WvletFormal.Scalar.Rule

namespace Wvlet.Scalar.Necessity

open Wvlet.Algebra
open Wvlet.Decorrelate
open Wvlet.Rewrite (Value Tri)

private def L : List Col := [0]
private def b : Term := .col 0
private def k : Term := .col 1
private def ks : Keys := [(b, k)]

/-- The correlation `k = b`. -/
private def corr : Cond := .eq b k

private def outer (v : Value) : Row := fun i => if i = 0 then v else .null
private def inner (v : Value) : Row := fun i => if i = 1 then v else .null

/-- `count(*)` -/
private def count : Agg := fun r => .int r.length

/-- `nullif(count(*), 1)`: NULL for a group of one row, but not NULL over no rows. -/
private def nullifCount : Agg := fun r => if r.length = 1 then .null else .int r.length

/-- SQL's values for the outer rows. -/
private def sql (agg : Agg) (ls rs : Rel) : List Value := ls.map fun x => scalarSql L corr agg x rs

private def values (r : List (Row × Value)) : List Value := r.map (·.2)

/-- Without the projection, an outer row without a group gets NULL for `count`, not 0. -/
theorem no_fix_count_null :
    values ((leftJoined ks count [inner (.int 1)] [outer (.int 2)]).map noFix) = [.null] ∧
      sql count [outer (.int 2)] [inner (.int 1)] = [.int 0] := by
  decide

/-- An inner join with the groups drops the outer rows without one, which SQL keeps with the
value over no rows (NULL for `max`, 0 for `count`). -/
theorem inner_join_drops :
    (innerJoin L corr [outer (.int 1), outer (.int 2), outer .null] [inner (.int 1)]).length = 1 ∧
      (sql count [outer (.int 1), outer (.int 2), outer .null] [inner (.int 1)]).length = 3 := by
  decide

/-- Detecting the unmatched rows by a NULL value (`coalesce(v, empty)`) instead of a NULL key
replaces the NULL value of a matched group: `nullif(count(*), 1)` over one row is NULL. -/
theorem coalesce_value_unsound :
    ((leftJoined ks nullifCount [inner (.int 1)] [outer (.int 1)]).map fun (x, _, v) =>
        (x, if v = Value.null then nullifCount [] else v)).map (·.2) = [.int 0] ∧
      sql nullifCount [outer (.int 1)] [inner (.int 1)] = [Value.null] := by
  decide

/-- Matching NULL keys (`is not distinct from`, as GROUP BY compares them) gives a NULL outer key
the NULL group's count, while SQL's `k = b` is never TRUE for it. -/
theorem null_safe_match_unsound :
    count (group ks [inner .null, inner .null] [.null]) = .int 2 ∧
      sql count [outer .null] [inner .null, inner .null] = [.int 0] := by
  decide

end Wvlet.Scalar.Necessity
