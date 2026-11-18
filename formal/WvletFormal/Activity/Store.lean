/-
Licensed under the Apache License, Version 2.0.

Model of the fenced operations of `wvlet.lang.runner.activity.ItemStore`
(InMemoryItemStore / SQLiteItemStore). Each operation is a conditional update guarded by the
attempt id, and returns whether it was applied.
-/
import WvletFormal.Activity.ItemState

namespace Wvlet.Activity

/-- A fenced store operation, addressed to one attempt of an item. -/
inductive StoreOp where
  /-- `claimItem`: queued → running for the given attempt. -/
  | claim (attempt : Nat)
  /-- `markCancelling`: queued or running → cancelling for the given attempt. -/
  | markCancelling (attempt : Nat)
  /-- `reportResult`: record an outcome unless the attempt is stale or the item is final. -/
  | report (attempt : Nat) (outcome : Outcome)
  deriving DecidableEq, Repr

namespace StoreOp

def attempt : StoreOp → Nat
  | claim a | markCancelling a | report a _ => a

/-- Apply the operation; returns `(accepted, item after the operation)`. -/
def apply : StoreOp → Item → Bool × Item
  | claim a, i =>
    if i.attempt = a ∧ i.state = .queued then (true, { i with state := .running }) else (false, i)
  | markCancelling a, i =>
    if i.attempt = a ∧ (i.state = .queued ∨ i.state = .running) then
      (true, { i with state := .cancelling })
    else (false, i)
  | report a o, i =>
    if i.attempt = a ∧ i.state.isFinal = false then (true, { i with state := o.toState })
    else (false, i)

/-- The item after the operation. -/
def step (op : StoreOp) (i : Item) : Item := (op.apply i).2

end StoreOp

/-! ## Fencing -/

/-- A rejected operation leaves the item unchanged. -/
theorem StoreOp.rejected_noop (op : StoreOp) (i : Item) (h : (op.apply i).1 = false) :
    op.step i = i := by
  cases op <;> simp only [StoreOp.apply, StoreOp.step] at * <;> split <;> simp_all

/-- An operation for a stale attempt never changes the item. -/
theorem StoreOp.stale_noop (op : StoreOp) (i : Item) (h : op.attempt ≠ i.attempt) :
    op.step i = i := by
  cases op <;> simp only [StoreOp.attempt, StoreOp.step, StoreOp.apply] at * <;>
    split <;> simp_all

/-- A result report never changes an item that has a final outcome. -/
theorem StoreOp.report_final_noop (a : Nat) (o : Outcome) (i : Item) (h : i.state.isFinal = true) :
    (StoreOp.report a o).step i = i := by
  simp [StoreOp.step, StoreOp.apply, h]

/-- Final outcomes are absorbing under every fenced store operation. -/
theorem StoreOp.final_absorbing (op : StoreOp) (i : Item) (h : i.state.isFinal = true) :
    op.step i = i := by
  cases op with
  | report a o => exact StoreOp.report_final_noop a o i h
  | claim a =>
    have : i.state ≠ .queued := by intro e; rw [e] at h; simp [ItemState.isFinal] at h
    simp [StoreOp.step, StoreOp.apply, this]
  | markCancelling a =>
    have h1 : i.state ≠ .queued := by intro e; rw [e] at h; simp [ItemState.isFinal] at h
    have h2 : i.state ≠ .running := by intro e; rw [e] at h; simp [ItemState.isFinal] at h
    simp [StoreOp.step, StoreOp.apply, h1, h2]

/-- Fenced operations never change the attempt id; only a new attempt (enqueue/rerun) does. -/
theorem StoreOp.attempt_preserved (op : StoreOp) (i : Item) : (op.step i).attempt = i.attempt := by
  cases op <;> simp only [StoreOp.step, StoreOp.apply] <;> split <;> rfl

/-- No fenced operation moves an item back to `queued`. -/
theorem StoreOp.never_requeues (op : StoreOp) (i : Item) (h : (op.step i).state = .queued) :
    i.state = .queued ∧ op.step i = i := by
  cases op with
  | claim a =>
    simp only [StoreOp.step, StoreOp.apply] at *; split at h <;> simp_all
  | markCancelling a =>
    simp only [StoreOp.step, StoreOp.apply] at *; split at h <;> simp_all
  | report a o =>
    simp only [StoreOp.step, StoreOp.apply] at *
    by_cases hc : i.attempt = a ∧ i.state.isFinal = false
    · rw [ite_eq_left hc] at h; cases o <;> simp [Outcome.toState] at h
    · rw [ite_eq_right hc] at h ⊢; exact ⟨h, rfl⟩

/-! ## Transition table

The state changes the store can make. This is wider than the diagram in the Scala `ItemState`
doc comment: a result report is accepted on any non-final state (e.g. a late result resolving an
`uncertain` or `cancelling` item), and a queued attempt can be marked cancelling.
-/

/-- An allowed state change of a single fenced store operation. -/
def StoreEdge : ItemState → ItemState → Prop
  | .queued, .running => True
  | .queued, .cancelling => True
  | .running, .cancelling => True
  | s, t => s.isFinal = false ∧ t.isTerminal = true

instance (s t : ItemState) : Decidable (StoreEdge s t) := by
  cases s <;> cases t <;> simp [StoreEdge] <;> infer_instance

/-- Every state change a fenced store operation makes is in the transition table. -/
theorem StoreOp.step_edge (op : StoreOp) (i : Item) (h : (op.step i).state ≠ i.state) :
    StoreEdge i.state (op.step i).state := by
  cases op with
  | claim a =>
    simp only [StoreOp.step, StoreOp.apply] at *
    split at h <;> simp_all [StoreEdge]
  | markCancelling a =>
    simp only [StoreOp.step, StoreOp.apply] at *
    split at h
    · rename_i hc
      obtain ⟨_, hq | hr⟩ := hc <;> simp_all [StoreEdge]
    · simp_all
  | report a o =>
    simp only [StoreOp.step, StoreOp.apply] at *
    split at h
    · rename_i hc
      have hT := Outcome.toState_isTerminal o
      cases hs : i.state <;> cases ho : o.toState <;> simp_all [StoreEdge, ItemState.isFinal]
    · simp_all

end Wvlet.Activity
