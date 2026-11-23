/-
Licensed under the Apache License, Version 2.0.

Conformance traces for correlated scalar subqueries, exported as JSON so that a Scala test can
replay them against the engine. An outer table with NULL keys, duplicate rows, and keys without a
group meets an inner table with NULL keys, NULL values, a group whose values are all NULL, and
duplicate rows, through subqueries with `count(*)`, `count(d)`, `sum`, `max`, and expressions
over them whose value over no rows is or is not NULL, correlated by one key, one key and a local
filter, or two keys. Each case gives the value of the engine's plan (`planScalar`) per outer row,
which `Rule.lean` proves equal to SQL's.

Columns: `a`, `b` on the outer table (ids 0, 1), `c`, `d` on the inner table (ids 2, 3).
-/
import WvletFormal.Scalar.Necessity
import WvletFormal.Decorrelate.Traces

namespace Wvlet.Scalar.Traces

open Wvlet.Algebra
open Wvlet.Decorrelate
open Wvlet.Rewrite (Value Tri)
open Wvlet.Pushdown.Traces (jvalue mkRow)
open Wvlet.Decorrelate.Traces (jwhere)

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

def L : List Col := [0, 1]

private def a : Term := .col 0
private def b : Term := .col 1
private def c : Term := .col 2
private def d : Term := .col 3

private def v (n : Int) : Value := .int n

def leftRows : List (List Value) :=
  [[v 1, v 10], [v 1, v 10], [v 2, .null], [.null, v 30], [v 3, v 40], [v 4, v 5], [v 6, v 6]]

/-- Inner rows, at columns 2 and 3: key 3's only value is NULL, NULL keys have rows, and no row
has key 6. -/
def rightRows : List (List Value) :=
  [[v 1, v 100], [v 1, .null], [v 1, v 100], [v 2, v 5], [v 4, v 5], [.null, v 30], [.null, v 7],
    [v 3, .null], [v 5, v 40]]

def ls : Rel := leftRows.map mkRow
def rs : Rel := rightRows.map fun r => mkRow ([.null, .null] ++ r)

/-- The integer values of `d` in a group, NULLs skipped. -/
private def ds (r : Rel) : List Int := r.filterMap fun y =>
  match y 3 with
  | .int n => some n
  | _ => none

private def maxOf : List Int → Value
  | [] => .null
  | n :: ns => .int (ns.foldl max n)

/-- Aggregates in SQL, with their value over a group's rows. -/
def aggs : List (String × Agg) :=
  [ ("count(*)", fun r => .int r.length)
  , ("count(d)", fun r => .int (ds r).length)
  , ("sum(d)", fun r => if (ds r).isEmpty then .null else .int (ds r).sum)
  , ("max(d)", fun r => maxOf (ds r))
  , ("count(*) + 1", fun r => .int (r.length + 1))
  , ("coalesce(sum(d), 0)", fun r => .int (ds r).sum)
  , ("case when count(*) > 0 then null else 1 end", fun r => if r.length > 0 then .null else .int 1) ]

/-- Correlations: the keys (`outer = inner`) and the subquery's local conjuncts. -/
def wheres : List (Keys × List Cond) :=
  [ ([(a, c)], [])
  , ([(a, c)], [.not (.isNull d)])
  , ([(a, c)], [.lt d (.lit (v 100))])
  , ([(a, c), (b, d)], []) ]

/-- The subquery's WHERE: `inner = outer` per key, then the local conjuncts. -/
def subWhere (ks : Keys) (lws : List Cond) : List Cond :=
  ks.map (fun (o, i) => Cond.eq i o) ++ lws

def jrow (x : Row) (val : Value) : String := jarr ((L.map fun i => jvalue (x i)) ++ [jvalue val])

def jcase (agg : String × Agg) (w : Keys × List Cond) : String :=
  let (ks, lws) := w
  let rows := planScalar ks agg.2 (filter (andAll lws) rs) ls
  jobj [ ("agg", jstr agg.1)
       , ("where", jwhere (subWhere ks lws))
       , ("emptyIsNull", if agg.2 [] = .null then "true" else "false")
       , ("rows", jarr (rows.map fun (x, val) => jrow x val)) ]

def fixture : String :=
  let cases := wheres.flatMap fun w => aggs.map fun agg => jcase agg w
  jobj [ ("left", jarr (leftRows.map fun r => jarr (r.map jvalue)))
       , ("right", jarr (rightRows.map fun r => jarr (r.map jvalue)))
       , ("cases", "[\n  " ++ ",\n  ".intercalate cases ++ "\n]") ] ++ "\n"

end Wvlet.Scalar.Traces
