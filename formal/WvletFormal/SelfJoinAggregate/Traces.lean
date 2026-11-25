/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the self-join aggregation model, exported as JSON so that a Scala test can
replay them against the engine's `SelfJoinAggregate`. The table `t(k, x, id)` has NULL keys, NULL
values, and repeated values. Each case is a consumer of the self join
`t as t1 ⋉ t as t2 on t1.k = t2.k and t1.x <> t2.x`, its left side filtered by `extra`:

- `exists`: the `id`s of the rows of t1 with such a match (`semiJoin_eq`);
- `in`: the `id`s of the rows of t whose k is among the keys of t1 with such a match; with no
  `extra`, the left side reads all of t, so these are the aggregation's keys (`keys_eq`);
- `distinct`: those distinct keys themselves (`select distinct t1.k`), so a NULL key among them
  would show.

`rows` are computed over the rewritten plan (the aggregation), `join_rows` over the self join; they
are the same.
-/
import WvletFormal.SelfJoinAggregate.Rule

namespace Wvlet.SelfJoinAggregate.Traces

open Wvlet.Rewrite (Value Tri)
open Wvlet.Algebra

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

-- Columns: the right side t2(k, x), the left side t1(lk, lx, lid)
def k : Col := 0
def x : Col := 1
def lk : Col := 10
def lx : Col := 11
def lid : Col := 12

def v : Option Int → Value
  | none => .null
  | some n => .int n

/-- The rows of t: (k, x, id). -/
def ts : List (Option Int × Option Int × Int) :=
  [(none, some 1, 0), (none, some 2, 1),
   (some 1, some 1, 2), (some 1, some 1, 3),
   (some 2, some 1, 4), (some 2, some 2, 5),
   (some 3, none, 6), (some 3, some 5, 7),
   (some 4, none, 8), (some 4, some 1, 9), (some 4, some 2, 10),
   (some 5, some 7, 11)]

def rRel : Rel := ts.map fun (a, b, _) => fun c => if c = k then v a else if c = x then v b else .null

def lRel : Rel := ts.map fun (a, b, i) => fun c =>
  if c = lk then v a else if c = lx then v b else if c = lid then .int i else .null

/-- Filters of the left side, each with its SQL text over t1. -/
def extras : List (Option Cond × String) :=
  [(none, ""),
   (some (.lt (.lit (.int 5)) (.col lid)), "t1.id > 5"),
   (some (.not (.eq (.col lx) (.lit (.int 2)))), "t1.x <> 2")]

def leftOf : Option Cond → Rel
  | none => lRel
  | some e => filter e lRel

/-- The self join. -/
def joined (e : Option Cond) : Rel := semiJoin [lk, lx] (selfOn lk lx k x) (leftOf e) rRel

/-- The rewritten plan: a semi join on the key with the aggregation. -/
def rewritten (e : Option Cond) : Rel :=
  semiJoin [lk, lx] (.eq (.col lk) (.col k)) (filter (.not (.isNull (.col lx))) (leftOf e))
    (having k x rRel)

/-- The keys the rewritten plan gives an IN: the aggregation's own with no filter. -/
def rewrittenKeys : Option Cond → List Value
  | none => (having k x rRel).map (· k)
  | e => (rewritten e).map (· lk)

/-- The rows of t whose k is IN the keys, under SQL's NULL rules. -/
def keepIn (keys : List Value) : Rel := lRel.filter fun z => inTri (z lk) keys == .true

def jval : Value → String
  | .int n => toString n
  | _ => "null"

def ids (r : Rel) : String :=
  jarr (r.map fun z => match z lid with | .int n => toString n | _ => "null")

def caseJson (consumer : String) (e : Option Cond × String) : String :=
  let (rows, joinRows) :=
    if consumer == "exists" then (ids (rewritten e.1), ids (joined e.1))
    else if consumer == "in" then
      (ids (keepIn (rewrittenKeys e.1)), ids (keepIn ((joined e.1).map (· lk))))
    else
      (jarr ((rewrittenKeys e.1).eraseDups.map jval),
        jarr (((joined e.1).map (· lk)).eraseDups.map jval))
  jobj [("consumer", jstr consumer), ("extra", jstr e.2), ("rows", rows),
    ("join_rows", joinRows)]

def jopt : Option Int → String
  | none => "null"
  | some n => toString n

/-- The fixture document. -/
def fixture : String :=
  let cases := ["exists", "in", "distinct"].flatMap fun c => extras.map fun e => caseJson c e
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe self-join-aggregate-traces (do not edit by hand)\",\n" ++
  "  \"t\": " ++ jarr (ts.map fun (a, b, i) => jarr [jopt a, jopt b, toString i]) ++ ",\n" ++
  "  \"cases\": [\n" ++ ",\n".intercalate (cases.map ("    " ++ ·)) ++ "\n  ]\n" ++
  "}\n"

end Wvlet.SelfJoinAggregate.Traces
