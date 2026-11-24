/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the semi-join reduction model, exported as JSON so that a Scala test can
replay them against the engine's `ReduceSemiJoins`. Each case is a consumer (IN, NOT IN, EXISTS,
NOT EXISTS) of `o` over the subquery `select lk from l, r where lk = rk and <extra>`, on fixed
inputs with NULLs and duplicates, and the `oid`s of the rows it keeps. The rows are computed over
the reduced subquery (`l ⋉ r`); `inner_rows`, over the inner join, are the same
(`Wvlet.SemiReduce.semiJoin_reduce`, `antiJoin_reduce`, `notIn_reduce`).
-/
import WvletFormal.SemiReduce.Rule

namespace Wvlet.SemiReduce.Traces

open Wvlet.Rewrite (Value)
open Wvlet.Algebra

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

-- Columns: o(ok, oid), l(lk, lv), r(rk, rv)
def ok : Col := 0
def oid : Col := 1
def lk : Col := 10
def lv : Col := 11
def rk : Col := 20
def rv : Col := 21

def v : Option Int → Value
  | none => .null
  | some n => .int n

def row (cols : List (Col × Option Int)) : Row := fun c =>
  match cols.find? (·.1 == c) with
  | some (_, x) => v x
  | none => .null

def os : List (Option Int × Int) := [(none, 0), (some 1, 1), (some 2, 2), (some 3, 3), (some 4, 4)]
def ls : List (Option Int × Option Int) :=
  [(none, some 1), (some 1, some 1), (some 1, some 2), (some 2, none), (some 3, some 3),
    (some 3, some 3)]
def rs : List (Option Int × Option Int) :=
  [(none, some 1), (some 1, some 1), (some 1, some 3), (some 2, some 2), (some 3, none),
    (some 4, some 4)]

def oRel : Rel := os.map fun (k, i) => row [(ok, k), (oid, some i)]
def lRel : Rel := ls.map fun (k, x) => row [(lk, k), (lv, x)]
def rRel : Rel := rs.map fun (k, x) => row [(rk, k), (rv, x)]

/-- The extra conditions of the subquery's join, each with its SQL text. -/
def extras : List (Option Cond × String) :=
  [(none, ""),
   (some (.not (.eq (.col lv) (.col rv))), "lv <> rv"),
   (some (.lt (.col lv) (.col rv)), "lv < rv"),
   (some (.or (.not (.eq (.col lv) (.col rv))) (.isNull (.col rv))), "(lv <> rv or rv is null)")]

def onOf : Option Cond → Cond
  | none => .eq (.col lk) (.col rk)
  | some e => .and (.eq (.col lk) (.col rk)) e

def consumers : List String := ["in", "not_in", "exists", "not_exists"]

def keep (consumer : String) (sub : Rel) : Rel :=
  let byKey := Cond.eq (.col ok) (.col lk)
  match consumer with
  | "in" | "exists" => semiJoin [ok, oid] byKey oRel sub
  | "not_in" => notIn (.col ok) lk oRel sub
  | _ => antiJoin [ok, oid] byKey oRel sub

def ids (r : Rel) : String :=
  jarr (r.map fun x => match x oid with | .int n => toString n | _ => "null")

def caseJson (consumer : String) (e : Option Cond × String) : String :=
  let on := onOf e.1
  let reduced := keep consumer (semiJoin [lk, lv] on lRel rRel)
  let inner := keep consumer (innerJoin [lk, lv] on lRel rRel)
  jobj [("consumer", jstr consumer), ("extra", jstr e.2), ("rows", ids reduced),
    ("inner_rows", ids inner)]

def jopt : Option Int → String
  | none => "null"
  | some n => toString n

/-- The fixture document. -/
def fixture : String :=
  let cases := consumers.flatMap fun c => extras.map fun e => caseJson c e
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe semi-reduce-traces (do not edit by hand)\",\n" ++
  "  \"o\": " ++ jarr (os.map fun (k, i) => jarr [jopt k, toString i]) ++ ",\n" ++
  "  \"l\": " ++ jarr (ls.map fun (k, x) => jarr [jopt k, jopt x]) ++ ",\n" ++
  "  \"r\": " ++ jarr (rs.map fun (k, x) => jarr [jopt k, jopt x]) ++ ",\n" ++
  "  \"cases\": [\n" ++ ",\n".intercalate (cases.map ("    " ++ ·)) ++ "\n  ]\n" ++
  "}\n"

end Wvlet.SemiReduce.Traces
