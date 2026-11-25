/-
Licensed under the Apache License, Version 2.0.

Conformance traces for moving a LEFT join above inner joins (`commute_left_joins` in the engine's
`ReorderJoins`), exported as JSON so that a Scala test can replay them against the shipped pass.
The tables are those of the filter pushdown traces: `l(a, b)` (ids 0, 1), `r(c, d)` (ids 2, 3), and
`t(e)` (id 4), in `from l left join r on ..., t where q`. Each case gives whether the LEFT join
moves above the inner join (`commutes`) and the rows: those of the moved plan when it moves
(`commute`: the same bag), else those of the written one.
-/
import WvletFormal.LeftJoinCommute.Rule
import WvletFormal.Pushdown.Traces

namespace Wvlet.LeftJoinCommute.Traces

open Wvlet.Algebra
open Wvlet.Rewrite (Value)
open Wvlet.Pushdown.Traces (condWv jvalue ls rs ts ons leftRows rightRows thirdRows)

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

def LA : List Col := [0, 1]
def LB : List Col := [2, 3]
def LC : List Col := [4]
def cols : List Col := [0, 1, 2, 3, 4]

private def int (n : Int) : Term := .lit (.int n)
private def a : Term := .col 0
private def b : Term := .col 1
private def c : Term := .col 2
private def e : Term := .col 4

/-- Conditions of the inner join with `t`: over `l` and `t` only (the LEFT join moves), and over
`r` without rejecting its NULLs (it stays, and stays a LEFT join). -/
def conds : List Cond :=
  [ .eq a e
  , .lt b e
  , .or (.eq a e) (.isNull e)
  , .and (.eq a e) (.isNull b)
  , .not (.eq b e)
  , .or (.eq a e) (.isNull c)
  , .or (.isNull c) (.lt (int 50) e)
  ]

def jrow (x : Row) : String := jarr (cols.map fun i => jvalue (x i))

def jcase (on q : Cond) : String :=
  let moves := commutes LB q
  let rows :=
    if moves then leftJoin (LA ++ LC) on (innerJoin LA q ls ts) rs
    else innerJoin (LA ++ LB) q (leftJoin LA on ls rs) ts
  jobj [ ("on", jstr (condWv on))
       , ("where", jstr (condWv q))
       , ("commutes", if moves then "true" else "false")
       , ("rows", jarr (rows.map jrow)) ]

def fixture : String :=
  let cases := ons.flatMap fun on => conds.map fun q => jcase on q
  jobj [ ("left", jarr (leftRows.map fun r => jarr (r.map jvalue)))
       , ("right", jarr (rightRows.map fun r => jarr (r.map jvalue)))
       , ("third", jarr (thirdRows.map fun r => jarr (r.map jvalue)))
       , ("cases", "[\n  " ++ ",\n  ".intercalate cases ++ "\n]") ] ++ "\n"

end Wvlet.LeftJoinCommute.Traces
