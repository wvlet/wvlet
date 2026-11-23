/-
Licensed under the Apache License, Version 2.0.

Conformance traces for NOT IN, exported as JSON so that a Scala test can replay them against the
engine. One left table with NULL and duplicate targets meets one right table whose rows are
grouped by `k`; the subquery of group `g` is `c` of the rows with `k = g`. The groups are values
without a NULL, values with a NULL, only a NULL, none (group 4, an empty subquery), duplicates, and
values that match no target. Each uncorrelated case gives SQL's rows of `a not in` and `a in` as a
WHERE, and the value of both per left row; the correlated case correlates the subquery by
`k = b`. The rows come from SQL's semantics (`notIn`, `notInCorrelated`), which the engine's
bindings are proved to equal (`Rule.lean`).

Columns: `a`, `b` on the left (ids 0, 1), `k`, `c` on the right (ids 2, 3).
-/
import WvletFormal.NotIn.Necessity

namespace Wvlet.NotIn.Traces

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

def L : List Col := [0, 1]

def jvalue : Value → String
  | .null => "null"
  | .int n => toString n
  | .str s => jstr s
  | .bool b => if b then "true" else "false"

def jtri (t : Tri) : String := jvalue t.toValue

/-- A row of the given values for columns 0, 1, ... -/
def mkRow (vs : List Value) : Row := fun i => vs.getD i .null

private def v (n : Int) : Value := .int n

/-- Left rows `(a, b)`; `b` names the group of the correlated case. -/
def leftRows : List (List Value) :=
  [ [v 1, v 1], [v 2, v 1], [.null, v 1], [v 1, v 1]
  , [v 1, v 2], [v 3, v 2]
  , [v 1, v 3]
  , [.null, v 4], [v 7, v 4]
  , [v 2, v 5], [v 3, v 5]
  , [.null, v 6], [v 1, v 6] ]

/-- Right rows `(k, c)`, at columns 2 and 3. No row has `k = 4`. -/
def rightRows : List (List Value) :=
  [ [v 1, v 1], [v 1, v 3]
  , [v 2, v 1], [v 2, .null]
  , [v 3, .null]
  , [v 5, v 2], [v 5, v 2], [v 5, v 1]
  , [v 6, v 5], [v 6, v 6] ]

def ls : Rel := leftRows.map mkRow
def rs : Rel := rightRows.map fun r => mkRow ([.null, .null] ++ r)

def groups : List Int := [1, 2, 3, 4, 5, 6]

private def a : Term := .col 0

/-- The subquery of group `g`. -/
def sub (g : Int) : Rel := filter (.eq (.col 2) (.lit (v g))) rs

def subValues (g : Int) : List Value := (sub g).map (· 3)

def inRows (g : Int) : Rel := ls.filter fun x => inTri (x 0) (subValues g) == .true

def jrow (x : Row) : String := jarr ([0, 1].map fun i => jvalue (x i))

def jcase (g : Int) : String :=
  jobj [ ("group", toString g)
       , ("values", jarr ((subValues g).map jvalue))
       , ("notInRows", jarr ((notIn a 3 ls (sub g)).map jrow))
       , ("inRows", jarr ((inRows g).map jrow))
       , ("items", jarr (ls.map fun x =>
            jarr [jvalue (x 0), jvalue (x 1), jtri (inTri (x 0) (subValues g)),
              jtri (inTri (x 0) (subValues g)).not])) ]

/-- The correlation `k = b`. -/
def corr : Cond := .eq (.col 2) (.col 1)

def fixture : String :=
  jobj [ ("left", jarr (leftRows.map fun r => jarr (r.map jvalue)))
       , ("right", jarr (rightRows.map fun r => jarr (r.map jvalue)))
       , ("cases", "[\n  " ++ ",\n  ".intercalate (groups.map jcase) ++ "\n]")
       , ("correlatedNotInRows", jarr ((notInCorrelated L corr a 3 ls rs).map jrow)) ] ++ "\n"

end Wvlet.NotIn.Traces
