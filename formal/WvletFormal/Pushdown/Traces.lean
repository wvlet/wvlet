/-
Licensed under the Apache License, Version 2.0.

Conformance traces for filter pushdown through joins, exported as JSON so that a Scala test can
replay them against the shipped `PushDownFilters` pass. Two inline tables with NULL keys, NULL
values, and a duplicate row are joined by every kind the fixture covers, under two ON conditions,
with one WHERE conjunct per case. Each case gives the model's placement (`place`) and the rows of
the query (`rewrite_sound`: the same rows wherever the conjunct goes).

Columns: `a`, `b` on the left (ids 0, 1), `c`, `d` on the right (ids 2, 3).
-/
import WvletFormal.Pushdown.Necessity

namespace Wvlet.Pushdown.Traces

open Wvlet.Algebra
open Wvlet.Rewrite (Value)

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

def colName : Col → String
  | 0 => "a"
  | 1 => "b"
  | 2 => "c"
  | _ => "d"

def L : List Col := [0, 1]
def R : List Col := [2, 3]
def outCols : List Col := [0, 1, 2, 3]

def jvalue : Value → String
  | .null => "null"
  | .int n => toString n
  | .str s => jstr s
  | .bool b => if b then "true" else "false"

def termWv : Term → String
  | .col c => colName c
  | .lit v => jvalue v

/-- The condition in Wvlet syntax. -/
def condWv : Cond → String
  | .eq a b => s!"{termWv a} = {termWv b}"
  | .lt a b => s!"{termWv a} < {termWv b}"
  | .isNull a => s!"{termWv a} is null"
  | .not (.isNull a) => s!"{termWv a} is not null"
  | .not p => s!"not ({condWv p})"
  | .and p q => s!"({condWv p}) and ({condWv q})"
  | .or p q => s!"({condWv p}) or ({condWv q})"

private def int (n : Int) : Term := .lit (.int n)
private def a : Term := .col 0
private def b : Term := .col 1
private def c : Term := .col 2
private def d : Term := .col 3

/-- A row of the given values for columns 0, 1, ... -/
def mkRow (vs : List Value) : Row := fun i => vs.getD i .null

private def v (n : Int) : Value := .int n

def leftRows : List (List Value) :=
  [[v 1, v 10], [v 1, v 10], [v 2, .null], [.null, v 30], [v 3, v 40]]

/-- Right rows, at columns 2 and 3. -/
def rightRows : List (List Value) :=
  [[v 1, v 100], [v 1, .null], [.null, v 300], [v 4, v 400], [v 2, v 5]]

def ls : Rel := leftRows.map mkRow
def rs : Rel := rightRows.map fun r => mkRow ([.null, .null] ++ r)

def ons : List Cond := [.eq a c, .and (.eq a c) (.lt b d)]

/-- One conjunct per case: over the left input, the right input, both, and none, with NULL
tests, negations, and disjunctions. A conjunct that reads no column (TRUE or FALSE) may be
folded before the pass, which the fixture marks with `constant`. -/
def conjuncts : List Cond :=
  [ .eq a (int 1)
  , .isNull b
  , .not (.isNull b)
  , .or (.eq a (int 2)) (.isNull a)
  , .isNull c
  , .not (.isNull d)
  , .not (.eq d (int 100))
  , .eq a c
  , .or (.lt b d) (.isNull c)
  , .lt (int 1) (int 2)
  , .lt (int 2) (int 1)
  ]

def kinds : List JoinKind := [.inner, .left, .right, .full]

def kindName : JoinKind → String
  | .inner => "inner"
  | .left => "left"
  | .right => "right"
  | .full => "full"
  | .semi => "semi"
  | .anti => "anti"

def placeName : Place → String
  | .left => "left"
  | .right => "right"
  | .cond => "cond"
  | .above => "above"

def jrow (x : Row) : String := jarr (outCols.map fun i => jvalue (x i))

def jcase (k : JoinKind) (on p : Cond) : String :=
  jobj [ ("kind", jstr (kindName k))
       , ("on", jstr (condWv on))
       , ("where", jstr (condWv p))
       , ("place", jstr (placeName (place k L R p)))
       , ("constant", if p.reads.isEmpty then "true" else "false")
       , ("rows", jarr ((rewrite k L R on p ls rs).map jrow)) ]

def fixture : String :=
  let cases := kinds.flatMap fun k => ons.flatMap fun on => conjuncts.map fun p => jcase k on p
  jobj [ ("left", jarr (leftRows.map fun r => jarr (r.map jvalue)))
       , ("right", jarr (rightRows.map fun r => jarr (r.map jvalue)))
       , ("cases", "[\n  " ++ ",\n  ".intercalate cases ++ "\n]") ] ++ "\n"

end Wvlet.Pushdown.Traces
