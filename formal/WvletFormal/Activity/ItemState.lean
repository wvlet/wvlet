/-
Licensed under the Apache License, Version 2.0.

Model of `wvlet.lang.runner.activity.ItemState` and `ItemRecord`
(wvlet-runner/src/main/scala/wvlet/lang/runner/activity/ItemState.scala).
-/

namespace Wvlet.Activity

/-- The state of a single item within a fan-out batch (mirrors the Scala `ItemState` enum). -/
inductive ItemState where
  | queued
  | running
  | succeeded
  | failed
  | cancelled
  | cancelling
  | uncertain
  deriving DecidableEq, Repr, Inhabited

namespace ItemState

/-- The persisted name of the state (`ItemState.stateName` in Scala). -/
def name : ItemState → String
  | queued => "queued"
  | running => "running"
  | succeeded => "succeeded"
  | failed => "failed"
  | cancelled => "cancelled"
  | cancelling => "cancelling"
  | uncertain => "uncertain"

def all : List ItemState :=
  [queued, running, succeeded, failed, cancelled, cancelling, uncertain]

/-- Terminal for scheduling: the item is no longer queued or in flight. -/
def isTerminal : ItemState → Bool
  | succeeded | failed | cancelled | uncertain => true
  | _ => false

/--
A final outcome is never overwritten by a result report. `uncertain` is terminal but not final:
a late result of the same attempt may still resolve it.
-/
def isFinal : ItemState → Bool
  | succeeded | failed | cancelled => true
  | _ => false

/-- An attempt is in flight: claimed by a worker (or submitted to an executor) and not resolved. -/
def isInFlight : ItemState → Bool
  | running | cancelling => true
  | _ => false

theorem final_isTerminal (s : ItemState) (h : s.isFinal = true) : s.isTerminal = true := by
  cases s <;> simp_all [isFinal, isTerminal]

theorem name_injective (s t : ItemState) (h : s.name = t.name) : s = t := by
  cases s <;> cases t <;> simp_all [name]

end ItemState

/-- The outcomes a result report can record (the `state` argument of `ItemStore.reportResult`). -/
inductive Outcome where
  | succeeded
  | failed
  | cancelled
  | uncertain
  deriving DecidableEq, Repr, Inhabited

namespace Outcome

def toState : Outcome → ItemState
  | succeeded => .succeeded
  | failed => .failed
  | cancelled => .cancelled
  | uncertain => .uncertain

def all : List Outcome := [succeeded, failed, cancelled, uncertain]

theorem toState_isTerminal (o : Outcome) : o.toState.isTerminal = true := by
  cases o <;> rfl

end Outcome

/--
The part of an `ItemRecord` that the state machine depends on. Attempt ids are ULIDs in Scala;
the model only needs equality, so they are natural numbers here.
-/
structure Item where
  state : ItemState
  attempt : Nat
  deriving DecidableEq, Repr, Inhabited

end Wvlet.Activity
