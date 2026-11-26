/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the aggregate windows model, exported as JSON so that a Scala test can
replay them against the engine's `AggregateWindows`. The table `i(g, x, id)` has a NULL key, a NULL
value, repeated values, and keys E lacks; `e(e, w)` repeats a key, and has a key I lacks and a NULL
key. Each case is an aggregate `f` of the query

  `select i.id, e.w, (select f(i2.x) from i as i2 where i2.g = i.g) from i, e where i.g = e.e`

`rows` are computed over the rewritten plan (f over O's partition of the row's key, a window),
`join_rows` over the decorrelated one (f over I's group of the key, joined back). They agree for
min, max, and avg (`mem_iff`, `avg_eq`), and differ for sum, which the rule leaves.
-/
import WvletFormal.AggregateWindows.Rule

namespace Wvlet.AggregateWindows.Traces

open Wvlet.Rewrite (Value Tri)
open Wvlet.Algebra

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

-- Columns: I's g, x, id (the joined row's left columns), then E's e, w
def g : Col := 0
def x : Col := 1
def id : Col := 2
def e : Col := 10
def w : Col := 11
def L : List Col := [g, x, id]

def v : Option Int → Value
  | none => .null
  | some n => .int n

/-- The rows of i: (g, x, id). -/
def is_ : List (Option Int × Option Int × Int) :=
  [(none, some 5, 0), (some 1, some 1, 1), (some 1, some 3, 2), (some 1, none, 3),
   (some 2, some 4, 4), (some 2, some 6, 5), (some 3, some 7, 6), (some 4, some 2, 7)]

/-- The rows of e: (e, w). -/
def es_ : List (Option Int × Int) :=
  [(some 1, 10), (some 1, 11), (some 2, 20), (some 5, 50), (none, 60)]

def iRel : Rel := is_.map fun (a, b, n) => fun c =>
  if c = g then v a else if c = x then v b else if c = id then .int n else .null

def eRel : Rel := es_.map fun (a, n) => fun c =>
  if c = e then v a else if c = w then .int n else .null

def oRel : Rel := joined L g e iRel eRel

def ints (vs : List Value) : List Int := vs.filterMap fun | .int n => some n | _ => none

/-- The aggregate over values, NULLs skipped, as JSON: NULL over no values. -/
def agg (f : String) (vs : List Value) : String :=
  match ints vs with
  | [] => "null"
  | n :: ns =>
    if f == "min" then toString (ns.foldl min n)
    else if f == "max" then toString (ns.foldl max n)
    else if f == "sum" then toString (ns.foldl (· + ·) n)
    else
      -- avg, as the exact fraction [numerator, denominator]
      jarr [toString (ns.foldl (· + ·) n), toString (ns.length + 1)]

def jval : Value → String
  | .int n => toString n
  | _ => "null"

/-- Each row of O with f over the values of its key in `over` (O itself, or I). -/
def rowsOf (f : String) (over : Rel) : String :=
  jarr (oRel.map fun z => jarr [jval (z id), jval (z w), agg f ((part g (z g) over).map (· x))])

def caseJson (f : String) : String :=
  jobj [("agg", jstr f), ("rows", rowsOf f oRel), ("join_rows", rowsOf f iRel)]

def jopt : Option Int → String
  | none => "null"
  | some n => toString n

/-- The fixture document. -/
def fixture : String :=
  let cases := ["min", "max", "avg", "sum"].map caseJson
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe aggregate-windows-traces (do not edit by hand)\",\n" ++
  "  \"i\": " ++ jarr (is_.map fun (a, b, n) => jarr [jopt a, jopt b, toString n]) ++ ",\n" ++
  "  \"e\": " ++ jarr (es_.map fun (a, n) => jarr [jopt a, toString n]) ++ ",\n" ++
  "  \"cases\": [\n" ++ ",\n".intercalate (cases.map ("    " ++ ·)) ++ "\n  ]\n" ++
  "}\n"

end Wvlet.AggregateWindows.Traces
