/-
Licensed under the Apache License, Version 2.0.

Conformance traces for filter pushdown through joins, exported as JSON so that a Scala test can
replay them against the shipped `PushDownFilters` pass. Two inline tables with NULL keys, NULL
values, and a duplicate row are joined by every kind the fixture covers, under two ON conditions,
with one WHERE conjunct per case. Each case gives the model's placement (`place`) and the rows of
the query (`rewrite_sound`: the same rows wherever the conjunct goes).

Columns: `a`, `b` on the left (ids 0, 1), `c`, `d` on the right (ids 2, 3).

The `tree` cases join the LEFT join with a third table `t(e)` (id 4) in an inner join tree
(`from l left join r on ..., t where q`): the model makes the LEFT join inner when `q` rejects NULLs
on `c`, `d` (`tree_left_inner`), and otherwise keeps it.
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
  | 3 => "d"
  | _ => "e"

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
  | .inList a vs => s!"{termWv a} in ({", ".intercalate (vs.map jvalue)})"
  | .not p => s!"not ({condWv p})"
  | .and p q => s!"({condWv p}) and ({condWv q})"
  | .or p q => s!"({condWv p}) or ({condWv q})"

private def int (n : Int) : Term := .lit (.int n)
private def a : Term := .col 0
private def b : Term := .col 1
private def c : Term := .col 2
private def d : Term := .col 3
private def e : Term := .col 4

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
tests, negations, and disjunctions. A TRUE conjunct that reads no column is dropped (by the
binder or the pass's own `filter`), which the fixture marks with `trueConstant`. -/
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
  | .inner => "inner"
  | .above => "above"

def jrow (x : Row) : String := jarr (outCols.map fun i => jvalue (x i))

/-- The third table of the `tree` cases, at column 4. -/
def thirdRows : List (List Value) := [[v 1], [v 5], [.null]]

def ts : Rel := thirdRows.map fun r => mkRow ([.null, .null, .null, .null] ++ r)

def treeCols : List Col := [0, 1, 2, 3, 4]

/-- Conditions of the inner join tree over the LEFT join and `t`. -/
def treeConds : List Cond :=
  [ .eq c e
  , .lt d e
  , .and (.eq a e) (.not (.isNull c))
  , .or (.eq c e) (.eq d e)
  , .or (.eq c e) (.isNull c)
  , .eq a e
  , .not (.eq d e)
  , .lt (int 50) d
  , .isNull d
  ]

def jtree (on q : Cond) : String :=
  let inner := rejectsNull R q
  let lj := if inner then innerJoin L on ls rs else leftJoin L on ls rs
  jobj [ ("on", jstr (condWv on))
       , ("where", jstr (condWv q))
       , ("place", jstr (if inner then "inner" else "left"))
       , ("rows", jarr ((innerJoin outCols q lj ts).map fun x =>
           jarr (treeCols.map fun i => jvalue (x i)))) ]

def jcase (k : JoinKind) (on p : Cond) : String :=
  jobj [ ("kind", jstr (kindName k))
       , ("on", jstr (condWv on))
       , ("where", jstr (condWv p))
       , ("place", jstr (placeName (place k L R p)))
       , ("trueConstant", if p.reads.isEmpty && p.holds Row.null then "true" else "false")
       , ("rows", jarr ((rewrite k L R on p ls rs).map jrow)) ]

def fixture : String :=
  let cases := kinds.flatMap fun k => ons.flatMap fun on => conjuncts.map fun p => jcase k on p
  let trees := ons.flatMap fun on => treeConds.map fun q => jtree on q
  jobj [ ("left", jarr (leftRows.map fun r => jarr (r.map jvalue)))
       , ("right", jarr (rightRows.map fun r => jarr (r.map jvalue)))
       , ("third", jarr (thirdRows.map fun r => jarr (r.map jvalue)))
       , ("cases", "[\n  " ++ ",\n  ".intercalate cases ++ "\n]")
       , ("tree", "[\n  " ++ ",\n  ".intercalate trees ++ "\n]") ] ++ "\n"

end Wvlet.Pushdown.Traces
