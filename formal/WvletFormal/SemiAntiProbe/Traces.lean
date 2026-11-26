/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the one-probe EXISTS / NOT EXISTS model, exported as JSON so that a Scala
test can replay them against the engine's `SemiAntiProbe`. The outer table `b(k, s, id)` and the
table `t(k, s, a, c, id)` have NULL keys and values. Each case is

  `select id from b where [not] exists (from t where t.k = b.k [and t.s <> b.s] and q)
                      and [not] exists (from t where t.k = b.k [and t.s <> b.s] and p)`

with the EXISTS first or second (`order`), the residual `t.s <> b.s` on each side or not, the
EXISTS filtered by `q` and the NOT EXISTS by `p`. `rows` are the fused join's (`fused`),
`join_rows` the nested joins'; they are the same (`anti_semi_eq`, `semi_anti_eq`).
-/
import WvletFormal.SemiAntiProbe.Rule

namespace Wvlet.SemiAntiProbe.Traces

open Wvlet.Rewrite (Value Tri)
open Wvlet.Algebra

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

-- Columns: the right side t(k, s, a, c, id), the left side b(bk, bs, bid)
def k : Col := 0
def s : Col := 1
def a : Col := 2
def c : Col := 3
def tid : Col := 4
def bk : Col := 10
def bs : Col := 11
def bid : Col := 12

def L : List Col := [bk, bs, bid]

def v : Option Int → Value
  | none => .null
  | some n => .int n

/-- The rows of b: (k, s, id). -/
def bs_ : List (Option Int × Option Int × Int) :=
  [(none, some 1, 0), (some 1, some 1, 1), (some 1, some 2, 2), (some 2, some 1, 3),
   (some 2, none, 4), (some 3, some 1, 5), (some 4, some 1, 6), (some 5, some 1, 7),
   (some 1, some 1, 8)]

/-- The rows of t: (k, s, a, c, id). -/
def ts : List (Option Int × Option Int × Option Int × Option Int × Int) :=
  [(none, some 2, some 2, some 1, 0),
   (some 1, some 1, some 1, some 2, 1), (some 1, some 2, some 3, some 1, 2),
   (some 1, some 3, some 1, some 1, 3),
   (some 2, some 1, some 2, some 1, 4), (some 2, some 2, none, some 1, 5),
   (some 3, some 2, some 1, some 5, 6), (some 3, none, some 4, some 1, 7),
   (some 4, some 1, some 3, some 1, 8), (some 4, some 2, some 1, some 1, 9),
   (some 6, some 1, some 1, some 1, 10)]

def bRel : Rel := bs_.map fun (x, y, i) => fun col =>
  if col = bk then v x else if col = bs then v y else if col = bid then .int i else .null

def tRel : Rel := ts.map fun (x, y, z, w, i) => fun col =>
  if col = k then v x else if col = s then v y else if col = a then v z
  else if col = c then v w else if col = tid then .int i else .null

def keyOn : Cond := .eq (.col bk) (.col k)

/-- `t.s <> b.s` -/
def differ : Cond := .not (.eq (.col bs) (.col s))

def onOf (resid : Bool) : Cond := if resid then .and keyOn differ else keyOn

/-- Filters of the EXISTS, with their SQL text over t. -/
def qs : List (Cond × String) :=
  [(.not (.isNull (.col tid)), "t.id is not null"), (.lt (.lit (.int 2)) (.col tid), "t.id > 2")]

/-- Filters of the NOT EXISTS: `c < a` is unknown when either is NULL. -/
def ps : List (Cond × String) :=
  [(.lt (.col c) (.col a), "t.c < t.a"), (.isNull (.col a), "t.a is null")]

def ids (r : Rel) : String :=
  jarr (r.map fun z => match z bid with | .int n => toString n | _ => "null")

def caseJson (existsFirst semiResid antiResid : Bool) (q p : Cond × String) : String :=
  let on₁ := onOf semiResid
  let on₂ := onOf antiResid
  let nested :=
    if existsFirst then antiJoin L on₂ (semiJoin L on₁ bRel (filter q.1 tRel)) (filter p.1 tRel)
    else semiJoin L on₁ (antiJoin L on₂ bRel (filter p.1 tRel)) (filter q.1 tRel)
  let rows := fused L (.and q.1 on₁) (.and p.1 on₂) bRel tRel
  jobj [("exists_first", toString existsFirst), ("semi_residual", toString semiResid),
    ("anti_residual", toString antiResid), ("q", jstr q.2), ("p", jstr p.2), ("rows", ids rows),
    ("join_rows", ids nested)]

def jopt : Option Int → String
  | none => "null"
  | some n => toString n

def bools : List Bool := [true, false]

/-- The fixture document. -/
def fixture : String :=
  let cases := bools.flatMap fun o => bools.flatMap fun sr => bools.flatMap fun ar =>
    qs.flatMap fun q => ps.map fun p => caseJson o sr ar q p
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe semi-anti-probe-traces (do not edit by hand)\",\n" ++
  "  \"b\": " ++ jarr (bs_.map fun (x, y, i) => jarr [jopt x, jopt y, toString i]) ++ ",\n" ++
  "  \"t\": " ++
    jarr (ts.map fun (x, y, z, w, i) => jarr [jopt x, jopt y, jopt z, jopt w, toString i]) ++ ",\n" ++
  "  \"cases\": [\n" ++ ",\n".intercalate (cases.map ("    " ++ ·)) ++ "\n  ]\n" ++
  "}\n"

end Wvlet.SemiAntiProbe.Traces
