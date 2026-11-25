/-
Licensed under the Apache License, Version 2.0.

Conformance traces for `FactorOr`, exported as JSON so that a Scala test can replay them against
the engine. One inline table with NULLs in every column meets WHERE conditions that are an `or` of
branches: a conjunct common to every branch, absorption, no common conjunct, three branches,
duplicate and reordered conjuncts, the conditions the old `InList.sig` confused (a NULL
among IN values; `'x,y'` against `'x', 'y'`), and the ones the old `Const.sig` confused (`'null'`
against a NULL string), and the one the old planner key (`exprName`) confused (two columns with the
same name in different inputs) or would confuse with whitespace collapsed (`'x  y'` against
`'x y'`). Each case gives the condition (also over a join, where `b` and
`c` are `l.v` and `r.v`), whether it factors
with the condition itself as the key and into how many conjuncts, the factored condition, and the
rows. Factoring is proved to keep them (`Rule.lean`).

Columns: `a` (int), `b`, `c` (bool), `s` (string), ids 0 to 3.
-/
import WvletFormal.FactorOr.Necessity
import WvletFormal.FactorOr.Implied

namespace Wvlet.FactorOr.Traces

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

def jvalue : Value → String
  | .null => "null"
  | .int n => toString n
  | .str s => jstr s
  | .bool b => if b then "true" else "false"

def colName : Col → String
  | 0 => "a"
  | 1 => "b"
  | 2 => "c"
  | _ => "s"

/-- The columns over a join of `l(i, a, v)` and `r(i, v, s)` on `i`: `b` and `c` are columns of
two inputs with the same name `v`, which the old key (`exprName`) named alike. -/
def joinColName : Col → String
  | 0 => "l.a"
  | 1 => "l.v"
  | 2 => "r.v"
  | _ => "r.s"

/-- A literal in Wvlet syntax (the strings have no quotes to escape). -/
def valueWv : Value → String
  | .str s => "'" ++ s ++ "'"
  | v => jvalue v

/-- A column's type, for a typed NULL literal. -/
def colType : Col → String
  | 0 => "int"
  | 3 => "string"
  | _ => "boolean"

section Render

variable (name : Col → String)

def termWv : Term → String
  | .col c => name c
  | .lit v => valueWv v

def valuesWv (vs : List Value) : String := "(" ++ ", ".intercalate (vs.map valueWv) ++ ")"

/-- The condition in Wvlet syntax, with `name` for the columns. -/
def condWv : Cond → String
  -- A boolean column as a conjunct, as `where b` is written: TRUE exactly when `b = true` is
  | .eq (.col c) (.lit (.bool true)) => name c
  -- An untyped `= null` is `is null` in Wvlet: type the NULL as the column, as SQL's `= NULL`
  | .eq (.col c) (.lit .null) => s!"{name c} = null::{colType c}"
  | .eq (.lit .null) (.col c) => s!"null::{colType c} = {name c}"
  | .eq a b => s!"{termWv name a} = {termWv name b}"
  | .lt a b => s!"{termWv name a} < {termWv name b}"
  | .isNull a => s!"{termWv name a} is null"
  | .inList a vs => s!"{termWv name a} in {valuesWv vs}"
  | .not (.inList a vs) => s!"{termWv name a} not in {valuesWv vs}"
  | .not p => s!"not ({condWv p})"
  | .and p q => s!"({condWv p}) and ({condWv q})"
  | .or p q => s!"({condWv p}) or ({condWv q})"

end Render

private def a : Term := .col 0
private def b : Term := .col 1
private def c : Term := .col 2
private def s : Term := .col 3
private def int (n : Int) : Value := .int n

def a1 : Cond := .eq a (.lit (int 1))
def aLt2 : Cond := .lt a (.lit (int 2))
def aNull : Cond := .isNull a
def bT : Cond := .eq b (.lit (.bool true))
def cT : Cond := .eq c (.lit (.bool true))
def in13 : Cond := .inList a [int 1, int 3]
def notIn13 : Cond := .not (.inList a [int 1, int 3])
def notIn13Null : Cond := .not (.inList a [int 1, int 3, .null])
def sXY : Cond := .inList s [.str "x,y"]
def sX_Y : Cond := .inList s [.str "x", .str "y"]
def sX : Cond := .inList s [.str "x"]
def sNullText : Cond := .eq s (.lit (.str "null"))
def sNull : Cond := .eq s (.lit .null)
def sTwoSpaces : Cond := .eq s (.lit (.str "x  y"))
def sOneSpace : Cond := .eq s (.lit (.str "x y"))

/-- Rows `(a, b, c, s)`, with NULLs in every column and a duplicate row. -/
def tableRows : List (List Value) :=
  [ [int 1, .bool true, .bool false, .str "x,y"]
  , [int 1, .bool false, .bool true, .str "x"]
  , [int 1, .null, .bool true, .null]
  , [int 2, .bool true, .bool true, .str "y"]
  , [int 2, .bool false, .bool true, .str "x,y"]
  , [int 3, .bool true, .null, .str "z"]
  , [.null, .bool true, .bool true, .str "x"]
  , [.null, .bool false, .null, .str "x,y"]
  , [.null, .bool false, .bool true, .str "null"]
  , [int 3, .bool true, .bool false, .str "null"]
  , [int 1, .bool true, .bool false, .str "x,y"]
  , [int 2, .bool false, .bool true, .str "x  y"] ]

def mkRow (vs : List Value) : Row := fun i => vs.getD i .null

def table : Rel := tableRows.map mkRow

/-- Each case: the conjuncts of each branch of the `or`. -/
def cases : List (List (List Cond)) :=
  [ [[a1, bT], [a1, cT]]
  , [[a1], [a1, bT]]
  , [[a1, bT], [aLt2, cT]]
  , [[a1, bT], [a1, cT], [a1, sX]]
  , [[a1, bT, cT], [bT, a1]]
  , [[a1, a1, bT], [cT, a1]]
  , [[aNull, bT], [cT, aNull]]
  , [[.not a1, bT], [a1, bT]]
  , [[in13, bT], [in13, cT]]
  , [[notIn13, bT], [notIn13Null, cT]]
  , [[notIn13Null, bT], [notIn13Null, cT]]
  , [[sXY, bT], [sX_Y, cT]]
  , [[sX_Y, bT], [sX_Y, cT], [sX_Y]]
  , [[sNullText, bT], [sNull, cT]]
  , [[sNullText, bT], [sNullText, cT]]
  , [[bT, a1], [cT, a1]]
  , [[sTwoSpaces, bT], [sOneSpace, cT]]
  -- Implied filters on both join inputs (`Implied.lean`), with NULLs that make a branch unknown
  , [[a1, cT], [aLt2, sX]]
  , [[aNull, sNull], [a1, bT, cT]]
  , [[a1, cT], [bT]]
  , [[a1, cT], [aLt2, sX], [aNull, cT]] ]

def jrow (x : Row) : String := jarr ([0, 1, 2, 3].map fun i => jvalue (x i))

/-- An uncorrelated subquery that is TRUE, so the replay can route a condition to the planner's
copy of the rule (which factors conditions with subqueries) without changing its value. -/
def existsWv : String := "exists { from [[1]] as u(x) }"

/-- The condition over the join with `existsWv` added to each branch: the same value, with a
subquery in every branch and a conjunct common to all of them. -/
def existsBranchesWv (bs : List (List Cond)) : String :=
  " or ".intercalate (bs.map fun br => s!"(({condWv joinColName (conj br)}) and {existsWv})")

/-- The columns of each input of the join form: `l.a`, `l.v` and `r.v`, `r.s`. -/
def leftCols : List Col := [0, 1]
def rightCols : List Col := [2, 3]

/-- The filters the factored conjuncts imply on the input with columns `cols` (`Implied.lean`),
as the engine derives them after `Normalize` factors the condition. -/
def impliedOn (cols : List Col) (e : Cond) : List Cond :=
  (conjuncts e).filterMap (impliedCond cols)

def jcase (bs : List (List Cond)) : String :=
  let e := disj (bs.map conj)
  let f := factorConjuncts (K := Cond) id e
  let n := match f with
    | some e' => (conjuncts e').length
    | none => 1
  let impliedWv (cols : List Col) :=
    jarr ((impliedOn cols (f.getD e)).map fun i => jstr (condWv joinColName i))
  jobj [ ("where", jstr (condWv colName e))
       , ("joinWhere", jstr (condWv joinColName e))
       , ("existsBranchesWhere", jstr (existsBranchesWv bs))
       , ("factored", if f.isSome then "true" else "false")
       , ("conjuncts", toString n)
       , ("factoredWhere", jstr (condWv colName (f.getD e)))
       , ("impliedLeft", impliedWv leftCols)
       , ("impliedRight", impliedWv rightCols)
       , ("rows", jarr ((filter e table).map jrow)) ]

def fixture : String :=
  jobj [ ("table", jarr (tableRows.map fun r => jarr (r.map jvalue)))
       , ("cases", "[\n  " ++ ",\n  ".intercalate (cases.map jcase) ++ "\n]") ] ++ "\n"

end Wvlet.FactorOr.Traces
