/-
Licensed under the Apache License, Version 2.0.

Conformance traces: the model's expected outcomes, exported as JSON so that Scala tests can
replay them against the shipped `ItemStore`, `TaskQueue`, and `BatchExecutor`. Attempt id `n` of
the model is `a<n>` in the fixture; the initial attempt is `a0`.
-/
import WvletFormal.Activity.Protocol

namespace Wvlet.Activity.Traces

open Wvlet.Activity

private def jstr (s : String) : String := "\"" ++ s ++ "\""
private def jbool (b : Bool) : String := if b then "true" else "false"
private def jattempt (n : Nat) : String := jstr ("a" ++ toString n)

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarrayLines (items : List String) : String :=
  "[\n" ++ ",\n".intercalate (items.map ("    " ++ ·)) ++ "\n  ]"

/-! ## Store traces -/

def outcomeName (o : Outcome) : String := o.toState.name

/-- Operations on the current attempt (`a0`) and on a stale one (`a1`). -/
def storeOps : List StoreOp :=
  [0, 1].flatMap fun a =>
    [StoreOp.claim a, .markCancelling a] ++ Outcome.all.map (StoreOp.report a)

/-- All operation sequences of length 1 and 2. -/
def storeOpSeqs : List (List StoreOp) :=
  storeOps.map ([·]) ++ storeOps.flatMap fun x => storeOps.map fun y => [x, y]

private def opJson (op : StoreOp) (accepted : Bool) (after : Item) : String :=
  let base :=
    match op with
    | .claim a => [("op", jstr "claim"), ("attempt", jattempt a)]
    | .markCancelling a => [("op", jstr "markCancelling"), ("attempt", jattempt a)]
    | .report a o =>
      [("op", jstr "reportResult"), ("attempt", jattempt a), ("outcome", jstr (outcomeName o))]
  jobj (base ++ [("accepted", jbool accepted), ("state", jstr after.state.name)])

/-- Replay a sequence, recording the acceptance and resulting state of each step. -/
def storeTrace (init : Item) (ops : List StoreOp) : String :=
  let (_, steps) := ops.foldl (init := (init, ([] : List String))) fun (i, acc) op =>
    let (ok, i') := op.apply i
    (i', acc ++ [opJson op ok i'])
  jobj [("init", jstr init.state.name), ("ops", "[" ++ ", ".intercalate steps ++ "]")]

def storeTraces : List String :=
  ItemState.all.flatMap fun s => storeOpSeqs.map (storeTrace ⟨s, 0⟩)

/-! ## Batch restart cases -/

def batchCases : List String :=
  ItemState.all.flatMap fun s => [false, true].flatMap fun r => BatchPolicy.all.map fun p =>
    let out := batchStart p r 1 ⟨s, 0⟩
    jobj [("init", jstr s.name), ("canResume", jbool r),
      ("retryUncertain", jbool p.retryUncertain), ("retryFailed", jbool p.retryFailed),
      ("rerunCancelled", jbool p.rerunCancelled),
      ("state", jstr out.state.name), ("newAttempt", jbool (out.attempt != 0))]

/-! ## Queue restart cases -/

def queueCases : List String :=
  ItemState.all.flatMap fun s => [false, true].flatMap fun w => [false, true].map fun e =>
    let out := queueRestart w e ⟨s, 0⟩
    jobj [("init", jstr s.name), ("hasWorker", jbool w), ("leaseExpired", jbool e),
      ("state", jstr out.state.name)]

/-- The fixture document. -/
def fixture : String :=
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe activity-traces (do not edit by hand)\",\n" ++
  "  \"store\": " ++ jarrayLines storeTraces ++ ",\n" ++
  "  \"batchRestart\": " ++ jarrayLines batchCases ++ ",\n" ++
  "  \"queueRestart\": " ++ jarrayLines queueCases ++ "\n" ++
  "}\n"

end Wvlet.Activity.Traces
