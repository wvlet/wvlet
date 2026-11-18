/-
Licensed under the Apache License, Version 2.0.

Model of the dispatch protocol around the item store: new attempts (`TaskQueue.enqueue`,
`BatchExecutor` submission and reruns), lease expiry (`TaskQueue.expireLeases`), queue restart
(`TaskQueue.recover`), and batch restart (`BatchExecutor.recoverItems`).
See adr/2026-10-23-activity-attempt-recovery.md and adr/2026-10-23-activity-rerun-and-cancel.md.
-/
import WvletFormal.Activity.Store

namespace Wvlet.Activity

/-! ## Batch restart -/

/-- The rerun options of `BatchConfig`. -/
structure BatchPolicy where
  retryUncertain : Bool
  retryFailed : Bool
  rerunCancelled : Bool
  deriving DecidableEq, Repr

namespace BatchPolicy

/-- `BatchExecutor.rerunnable`: whether a batch start runs the item again under a new attempt. -/
def rerunnable (p : BatchPolicy) : ItemState → Bool
  | .cancelled => p.rerunCancelled
  | .failed => p.retryFailed
  | .uncertain => p.retryUncertain
  | _ => false

def none : BatchPolicy := ⟨false, false, false⟩

def all : List BatchPolicy :=
  [false, true].flatMap fun u => [false, true].flatMap fun f => [false, true].map fun c => ⟨u, f, c⟩

end BatchPolicy

/--
Reconciliation of an existing item record when a batch starts. Terminal items are reused,
attempts the executor still tracks are awaited (`canResume`), queued items are re-admitted, and
any other in-flight attempt is marked uncertain through a fenced result report.
-/
def batchRecover (canResume : Bool) (i : Item) : Item :=
  if i.state.isTerminal then i
  else if canResume then i
  else if i.state = .queued then i
  else (StoreOp.report i.attempt .uncertain).step i

/-- Whether a batch start awaits the item's in-flight attempt instead of starting a new one. -/
def batchResumes (canResume : Bool) (i : Item) : Bool := !i.state.isTerminal && canResume

/-- A batch start: reconcile, then rerun the item under the `fresh` attempt when policy opts in. -/
def batchStart (p : BatchPolicy) (canResume : Bool) (fresh : Nat) (i : Item) : Item :=
  if p.rerunnable (batchRecover canResume i).state then { state := .queued, attempt := fresh }
  else batchRecover canResume i

/-! ## Queue restart -/

/--
A restarted `TaskQueue` (`recover`) followed by a lease sweep (`expireLeases`), for an item that
was dispatched through the queue. Queued items are re-queued; claimed attempts are re-adopted and
become uncertain once their lease has expired; a cancellation requested before any worker
claimed the task is confirmed. Running items without a worker are not tracked by the queue.
-/
def queueRestart (hasWorker leaseExpired : Bool) (i : Item) : Item :=
  match i.state, hasWorker with
  | .running, true | .cancelling, true =>
    if leaseExpired then (StoreOp.report i.attempt .uncertain).step i else i
  | .cancelling, false => (StoreOp.report i.attempt .cancelled).step i
  | _, _ => i

/-! ## The system -/

/-- One item, the attempt-id supply, and the attempts claimed by workers (newest first). -/
structure Sys where
  item : Item
  next : Nat
  claimed : List Nat

inductive Event where
  /-- Start a new attempt under a fresh id: `TaskQueue.enqueue` or a batch submission. -/
  | newAttempt
  /-- Any fenced store operation, for any attempt id (current or stale). -/
  | store (op : StoreOp)
  /-- The lease of the current in-flight attempt expired. -/
  | expire
  /-- The task queue restarted on the same store (`TaskQueue.recover` + `expireLeases`). -/
  | queueRestarted (hasWorker leaseExpired : Bool)
  /-- The batch started again on the same store (`BatchExecutor.recoverItems`). -/
  | batchStarted (p : BatchPolicy) (canResume : Bool)

def Event.apply : Event → Sys → Sys
  | .newAttempt, s => { item := ⟨.queued, s.next⟩, next := s.next + 1, claimed := s.claimed }
  | .store op, s =>
    let r := op.apply s.item
    let claimed :=
      match op with
      | .claim a => if r.1 then a :: s.claimed else s.claimed
      | _ => s.claimed
    { s with item := r.2, claimed := claimed }
  | .expire, s =>
    if s.item.state.isInFlight then
      { s with item := (StoreOp.report s.item.attempt .uncertain).step s.item }
    else s
  | .queueRestarted w e, s => { s with item := queueRestart w e s.item }
  | .batchStarted p r, s => { s with item := batchStart p r s.next s.item, next := s.next + 1 }

def Sys.init : Sys := { item := ⟨.queued, 0⟩, next := 1, claimed := [] }

inductive Reachable : Sys → Prop where
  | init : Reachable Sys.init
  | step {s : Sys} (e : Event) : Reachable s → Reachable (e.apply s)

/-! ## Restart properties -/

/-- Reconciliation never starts a new attempt. -/
theorem batchRecover_attempt (r : Bool) (i : Item) : (batchRecover r i).attempt = i.attempt := by
  unfold batchRecover; split
  · rfl
  · split
    · rfl
    · split
      · rfl
      · exact StoreOp.attempt_preserved _ _

/-- Reconciliation either keeps the item or records an outcome through the fenced store. -/
theorem batchRecover_cases (r : Bool) (i : Item) :
    batchRecover r i = i ∨ batchRecover r i = (StoreOp.report i.attempt .uncertain).step i := by
  unfold batchRecover; split
  · exact .inl rfl
  · split
    · exact .inl rfl
    · split
      · exact .inl rfl
      · exact .inr rfl

/-- A batch start awaits an in-flight attempt that its executor still tracks. -/
theorem batchStart_resumes (p : BatchPolicy) (f : Nat) (i : Item)
    (h : i.state.isInFlight = true) : batchStart p true f i = i := by
  cases hs : i.state <;> simp_all [ItemState.isInFlight, batchStart, batchRecover,
    BatchPolicy.rerunnable, ItemState.isTerminal]

/--
A batch start never re-executes an untracked in-flight attempt: the item becomes `uncertain`
under the same attempt, and runs again only when `retryUncertain` opts in.
-/
theorem batchStart_inflight_untracked (p : BatchPolicy) (f : Nat) (i : Item)
    (h : i.state.isInFlight = true) :
    batchStart p false f i =
      if p.retryUncertain then ⟨.queued, f⟩ else ⟨.uncertain, i.attempt⟩ := by
  cases hs : i.state <;>
    simp_all [ItemState.isInFlight, batchStart, batchRecover, BatchPolicy.rerunnable,
      ItemState.isTerminal, StoreOp.step, StoreOp.apply, ItemState.isFinal, Outcome.toState] <;>
    split <;> simp_all

/-- A resumed item keeps its state and attempt: the executor awaits the attempt it tracks. -/
theorem batchResumes_unchanged (p : BatchPolicy) (f : Nat) (i : Item)
    (h : batchResumes true i = true) : batchStart p true f i = i := by
  cases hs : i.state <;> simp_all [batchResumes, batchStart, batchRecover, BatchPolicy.rerunnable,
    ItemState.isTerminal]

/-- A succeeded item is never run again. -/
theorem batchStart_succeeded (p : BatchPolicy) (r : Bool) (f : Nat) (i : Item)
    (h : i.state = .succeeded) : batchStart p r f i = i := by
  simp [batchStart, batchRecover, h, ItemState.isTerminal, BatchPolicy.rerunnable]

/-- A batch start uses a new attempt only for an item whose rerun policy is enabled. -/
theorem batchStart_new_attempt_requires_policy (p : BatchPolicy) (r : Bool) (f : Nat) (i : Item)
    (h : (batchStart p r f i).attempt ≠ i.attempt) :
    p.rerunnable (batchRecover r i).state = true := by
  unfold batchStart at h
  split at h
  · assumption
  · exact absurd (batchRecover_attempt r i) h

/-- Without rerun options, a batch start never starts a new attempt. -/
theorem batchStart_no_policy (r : Bool) (f : Nat) (i : Item) :
    batchStart BatchPolicy.none r f i = batchRecover r i := by
  unfold batchStart
  have : BatchPolicy.none.rerunnable (batchRecover r i).state = false := by
    cases (batchRecover r i).state <;> rfl
  simp [this]

/-- A queue restart keeps the attempt and either keeps the item or records an outcome. -/
theorem queueRestart_cases (w e : Bool) (i : Item) :
    queueRestart w e i = i ∨ ∃ o, queueRestart w e i = (StoreOp.report i.attempt o).step i := by
  unfold queueRestart
  split
  · split
    · exact .inr ⟨_, rfl⟩
    · exact .inl rfl
  · split
    · exact .inr ⟨_, rfl⟩
    · exact .inl rfl
  · exact .inr ⟨_, rfl⟩
  · exact .inl rfl

/-- A queue restart never re-dispatches a claimed attempt: in-flight items stay in flight
unchanged or are resolved. -/
theorem queueRestart_no_redispatch (w e : Bool) (i : Item)
    (h : (queueRestart w e i).state = .queued) : i.state = .queued ∧ queueRestart w e i = i := by
  rcases queueRestart_cases w e i with heq | ⟨o, heq⟩
  · rw [heq] at h ⊢; exact ⟨h, rfl⟩
  · rw [heq] at h ⊢; exact StoreOp.never_requeues _ _ h

/-! ## At most one execution per attempt -/

/-- The invariant of reachable systems. -/
structure Inv (s : Sys) : Prop where
  attempt_lt : s.item.attempt < s.next
  claimed_lt : ∀ a ∈ s.claimed, a < s.next
  queued_unclaimed : s.item.state = .queued → s.item.attempt ∉ s.claimed
  nodup : s.claimed.Nodup

/-- The invariant survives any change that keeps the attempt and never re-queues the item. -/
theorem Inv.of_fenced {i i' : Item} {n n' : Nat} {c : List Nat} (hinv : Inv ⟨i, n, c⟩)
    (hn : n ≤ n') (ha : i'.attempt = i.attempt) (hq : i'.state = .queued → i' = i) :
    Inv ⟨i', n', c⟩ where
  attempt_lt := by have := hinv.attempt_lt; simp_all; omega
  claimed_lt := fun a ha' => Nat.lt_of_lt_of_le (hinv.claimed_lt a ha') hn
  queued_unclaimed := fun h => by
    have := hq h; subst this; exact hinv.queued_unclaimed h
  nodup := hinv.nodup

theorem Inv.fresh {s : Sys} (hinv : Inv s) :
    Inv { item := ⟨.queued, s.next⟩, next := s.next + 1, claimed := s.claimed } where
  attempt_lt := Nat.lt_succ_self _
  claimed_lt := fun a ha => Nat.lt_succ_of_lt (hinv.claimed_lt a ha)
  queued_unclaimed := fun _ hmem => Nat.lt_irrefl _ (hinv.claimed_lt _ hmem)
  nodup := hinv.nodup

theorem Inv.report {s : Sys} (hinv : Inv s) (a : Nat) (o : Outcome) :
    Inv { s with item := (StoreOp.report a o).step s.item } :=
  Inv.of_fenced (i := s.item) (n := s.next) (c := s.claimed) hinv (Nat.le_refl _)
    (StoreOp.attempt_preserved _ _) (fun h => (StoreOp.never_requeues _ _ h).2)

theorem Inv.step {s : Sys} (e : Event) (hinv : Inv s) : Inv (e.apply s) := by
  cases e with
  | newAttempt => exact hinv.fresh
  | store op =>
    cases op with
    | claim a =>
      simp only [Event.apply]
      by_cases hc : s.item.attempt = a ∧ s.item.state = .queued
      · obtain ⟨ha, hq⟩ := hc
        simp only [StoreOp.apply, ha, hq, and_self, ite_true]
        have hfresh : a ∉ s.claimed := ha ▸ hinv.queued_unclaimed hq
        exact {
          attempt_lt := show a < s.next from ha ▸ hinv.attempt_lt
          claimed_lt := fun b hb => by
            cases List.mem_cons.mp hb with
            | inl h => show b < s.next; rw [h, ← ha]; exact hinv.attempt_lt
            | inr h => exact hinv.claimed_lt b h
          queued_unclaimed := fun h => by simp at h
          nodup := List.nodup_cons.mpr ⟨hfresh, hinv.nodup⟩ }
      · simp only [StoreOp.apply, hc, ite_false]
        exact hinv
    | markCancelling a =>
      exact Inv.of_fenced (i := s.item) (n := s.next) (c := s.claimed) hinv (Nat.le_refl _)
        (StoreOp.attempt_preserved (.markCancelling a) _)
        (fun h => (StoreOp.never_requeues (.markCancelling a) _ h).2)
    | report a o => exact hinv.report a o
  | expire =>
    simp only [Event.apply]; split
    · exact hinv.report _ _
    · exact hinv
  | queueRestarted w e =>
    rcases queueRestart_cases w e s.item with heq | ⟨o, heq⟩
    · simp only [Event.apply, heq]; exact hinv
    · simp only [Event.apply, heq]; exact hinv.report _ _
  | batchStarted p r =>
    simp only [Event.apply, batchStart]
    split
    · exact hinv.fresh
    · exact Inv.of_fenced (i := s.item) (n := s.next) (c := s.claimed) hinv (Nat.le_succ _)
        (batchRecover_attempt r s.item) (fun h => by
          rcases batchRecover_cases r s.item with heq | heq
          · exact heq
          · rw [heq] at h ⊢; exact (StoreOp.never_requeues _ _ h).2)

theorem Inv.init : Inv Sys.init where
  attempt_lt := by decide
  claimed_lt := by simp [Sys.init]
  queued_unclaimed := by simp [Sys.init]
  nodup := List.nodup_nil

theorem Reachable.inv {s : Sys} (h : Reachable s) : Inv s := by
  induction h with
  | init => exact Inv.init
  | step e _ ih => exact ih.step e

/--
No attempt is ever claimed twice, whatever the interleaving of claims, stale reports,
cancellations, lease expiries, and queue or batch restarts: each attempt is executed by at most
one worker, at most once.
-/
theorem claimed_nodup {s : Sys} (h : Reachable s) : s.claimed.Nodup := h.inv.nodup

end Wvlet.Activity
