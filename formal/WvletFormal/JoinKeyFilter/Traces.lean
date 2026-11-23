/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the join-key filter model, exported as JSON so that a Scala test can replay
them against the engine's `PushDownFilters`. Each case is a join of two fixed inputs (keys with
NULLs and duplicates), a join kind, a conjunct over the preserved input's key (either input's key
for an inner join), and the rows of the filtered join (`sound_left`, `sound_right`,
`sound_inner_right`: the same as joining the two inputs each filtered by the conjunct).
-/
import WvletFormal.JoinKeyFilter.Findings

namespace Wvlet.JoinKeyFilter.Traces

open Wvlet.JoinKeyFilter

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

/-- The conjuncts of the traces, each with its SQL text over the key column `{k}`. -/
inductive PredE where
  | eq (n : Int)
  | ne (n : Int)
  | gt (n : Int)
  | inList (ns : List Int)
  | isNull
  | coalesceEq (d n : Int)

def PredE.eval : PredE → Pred
  | .eq n, v => match v with | none => .unk | some a => if a = n then .tt else .ff
  | .ne n, v => match v with | none => .unk | some a => if a = n then .ff else .tt
  | .gt n, v => match v with | none => .unk | some a => if a > n then .tt else .ff
  | .inList ns, v => match v with | none => .unk | some a => if ns.contains a then .tt else .ff
  | .isNull, v => if v.isNone then .tt else .ff
  | .coalesceEq d n, v => if v.getD d = n then .tt else .ff

def PredE.sql (k : String) : PredE → String
  | .eq n => s!"{k} = {n}"
  | .ne n => s!"{k} <> {n}"
  | .gt n => s!"{k} > {n}"
  | .inList ns => s!"{k} in ({", ".intercalate (ns.map toString)})"
  | .isNull => s!"{k} is null"
  | .coalesceEq d n => s!"coalesce({k}, {d}) = {n}"

def preds : List PredE :=
  [.eq 2, .ne 2, .gt 1, .inList [1, 4], .isNull, .coalesceEq 1 1]

def ls : List Row := [⟨none, 0⟩, ⟨some 1, 1⟩, ⟨some 2, 2⟩, ⟨some 2, 3⟩, ⟨some 3, 4⟩]
def rs : List Row := [⟨none, 10⟩, ⟨some 1, 11⟩, ⟨some 2, 12⟩, ⟨some 2, 13⟩, ⟨some 4, 14⟩]

def kindName : Kind → String
  | .inner => "inner"
  | .left => "left"
  | .right => "right"
  | .semi => "semi"
  | .anti => "anti"

/-- The join kinds, each with the inputs whose key a conjunct reads. -/
def kinds : List (Kind × String) :=
  [(.inner, "left"), (.inner, "right"), (.left, "left"), (.right, "right"), (.semi, "left"),
    (.anti, "left")]

def jopt : Option Nat → String
  | none => "null"
  | some n => toString n

def jrowsIn (rows : List Row) : String :=
  jarr (rows.map fun r => jarr [(match r.key with | none => "null" | some k => toString k),
    toString r.id])

def caseJson (k : Kind) (side : String) (p : PredE) : String :=
  let out := join (fun _ _ => true) k (keep p.eval ls) (keep p.eval rs)
  jobj [("kind", jstr (kindName k)), ("side", jstr side), ("pred", jstr (p.sql "{k}")),
    ("rows", jarr (out.map fun o => jarr [jopt (o.1.map Row.id), jopt (o.2.map Row.id)]))]

/-- The fixture document. -/
def fixture : String :=
  let cases := kinds.flatMap fun (k, side) => preds.map fun p => caseJson k side p
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe join-key-filter-traces (do not edit by hand)\",\n" ++
  "  \"left\": " ++ jrowsIn ls ++ ",\n" ++
  "  \"right\": " ++ jrowsIn rs ++ ",\n" ++
  "  \"cases\": [\n" ++ ",\n".intercalate (cases.map ("    " ++ ·)) ++ "\n  ]\n" ++
  "}\n"

end Wvlet.JoinKeyFilter.Traces
