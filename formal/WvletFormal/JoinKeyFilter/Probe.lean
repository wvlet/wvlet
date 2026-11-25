/-
Licensed under the Apache License, Version 2.0.

Runtime join filters (`runtime_join_filters`, `row_join_filters` in the engine's `Lowering`): a
hash join's build gives a scan on its probe side a test that is TRUE for every non-NULL build key
(a [min, max] range, a bitmap, a Bloom filter), and the scan drops the probe rows failing it, NULL
keys included. That keeps the join's rows exactly when the join drops unmatched probe rows: the left
input of inner, semi, and RIGHT joins, and the right input of inner, LEFT, semi, and anti joins.
The other sides keep (or emit) unmatched probe rows, so a filter there changes the result
(`probe_left_of_left_unsound`, `probe_left_of_anti_unsound`, `probe_right_of_right_unsound`).

The model covers the join itself. Which scans below the probe input may take the filter (through
filters, plain projections, UNION ALL inputs, and the probe sides of joins that keep their rows,
`Lowering.scanColumns`) is not modeled.
-/
import WvletFormal.JoinKeyFilter.Sound

namespace Wvlet.JoinKeyFilter

/-- `p` is TRUE on every non-NULL key of `rs`, as a build's runtime filter is on its keys. -/
def Covers (p : Pred) (rs : List Row) : Prop :=
  ∀ r ∈ rs, ∀ a : Int, r.key = some a → holds p (some a) = true

/-- Dropping rows of `rs` that fail `p` keeps the rows matching `l`, when `p` holds on `l`'s key
(or `l`'s key is NULL). -/
theorem matching_keep_covered (θ : Residual) (p : Pred) (l : Row)
    (h : ∀ a : Int, l.key = some a → holds p (some a) = true) :
    ∀ rs : List Row, matching θ (keep p rs) l = matching θ rs l := by
  intro rs
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    unfold keep matching at *
    by_cases hr : holds p r.key = true
    · simp only [List.filter_cons, hr, ite_true]
      split <;> simp [ih]
    · have hm : (keyEq l.key r.key && θ l r) = false := by
        cases hh : (keyEq l.key r.key && θ l r)
        · rfl
        · simp only [Bool.and_eq_true] at hh
          have he := keyEq_eq hh.1
          cases hl : l.key with
          | none => rw [hl] at hh; simp [keyEq] at hh
          | some a =>
            rw [← he, hl] at hr
            exact absurd (h a hl) hr
      simp [hr, hm, ih]

/-- A row failing a test that covers `rs` matches no row of `rs`. -/
theorem matching_nil (θ : Residual) (p : Pred) (l : Row) (hl : holds p l.key = false) :
    ∀ rs : List Row, Covers p rs → matching θ rs l = []
  | [], _ => rfl
  | r :: rs, hc => by
    have ih := matching_nil θ p l hl rs (fun x hx => hc x (by simp [hx]))
    have hm : (keyEq l.key r.key && θ l r) = false := by
      cases hh : (keyEq l.key r.key && θ l r)
      · rfl
      · simp only [Bool.and_eq_true] at hh
        have he := keyEq_eq hh.1
        cases hr : r.key with
        | none => rw [hr] at hh; cases l.key <;> simp [keyEq] at hh
        | some a =>
          have := hc r (by simp) a hr
          rw [he, hr] at hl
          rw [this] at hl
          exact absurd hl (by decide)
    unfold matching at *
    simp [hm, ih]

/-- Inner and semi joins: dropping the left (probe) rows that fail a test covering the right
input's keys keeps the join's rows. -/
theorem probe_left (θ : Residual) (p : Pred) (k : Kind) (hk : k = .inner ∨ k = .semi)
    (rs : List Row) (hc : Covers p rs) :
    ∀ ls : List Row, join θ k (keep p ls) rs = join θ k ls rs
  | [] => rfl
  | l :: ls => by
    have ih := probe_left θ p k hk rs hc ls
    have hjoin : ∀ xs, join θ k xs rs = xs.flatMap fun l => rowsOf k l (matching θ rs l) := by
      intro xs; rcases hk with rfl | rfl <;> rfl
    rw [hjoin, hjoin] at ih ⊢
    by_cases hl : holds p l.key = true
    · have hkeep : keep p (l :: ls) = l :: keep p ls := by simp [keep, hl]
      rw [hkeep, List.flatMap_cons, List.flatMap_cons, ih]
    · have hkeep : keep p (l :: ls) = keep p ls := by simp [keep, hl]
      have hnil := matching_nil θ p l (by simpa using hl) rs hc
      rw [hkeep, List.flatMap_cons, hnil, ih]
      rcases hk with rfl | rfl <;> simp [rowsOf]

/-- The rows of `ls` matching each row of `rs` stay the same when `ls` drops the rows failing a
test covering `rs`'s keys. -/
theorem flatMap_matching_keep (θ : Residual) (p : Pred) (f : Row → List Row → List Out)
    (ls : List Row) :
    ∀ rs : List Row, Covers p rs →
      (rs.flatMap fun r => f r (matching θ (keep p ls) r)) =
        rs.flatMap fun r => f r (matching θ ls r)
  | [], _ => rfl
  | r :: rs, hc => by
    have ih := flatMap_matching_keep θ p f ls rs (fun x hx => hc x (by simp [hx]))
    have hr := matching_keep_covered θ p r (hc r (by simp)) ls
    simp only [List.flatMap_cons, hr, ih]

/-- RIGHT joins: dropping the left (probe) rows that fail a test covering the right input's keys
keeps the join's rows. -/
theorem probe_left_of_right (θ : Residual) (p : Pred) (ls rs : List Row) (hc : Covers p rs) :
    join θ .right (keep p ls) rs = join θ .right ls rs := by
  simp only [join]
  rw [flatMap_matching_keep (fun a b => θ b a) p (fun r ms => rowsOf .left r ms) ls rs hc]

/-- Inner, LEFT, semi, and anti joins: dropping the right (probe) rows that fail a test covering
the left input's keys keeps the join's rows. -/
theorem probe_right (θ : Residual) (p : Pred) (k : Kind) (hk : k ≠ .right) (rs : List Row) :
    ∀ ls : List Row, Covers p ls → join θ k ls (keep p rs) = join θ k ls rs := by
  intro ls hc
  have hjoin : ∀ ys, join θ k ls ys = ls.flatMap fun l => rowsOf k l (matching θ ys l) := by
    intro ys; cases k with
    | right => exact absurd rfl hk
    | _ => rfl
  rw [hjoin, hjoin]
  exact flatMap_matching_keep θ p (fun l ms => rowsOf k l ms) rs ls hc

/-- A test that is never TRUE: it covers an empty build. -/
def noneP : Pred := fun _ => .ff

theorem covers_nil (p : Pred) : Covers p [] := by
  intro r hr; simp at hr

/-- A LEFT join keeps unmatched left rows: filtering its left input drops them. -/
theorem probe_left_of_left_unsound :
    join (fun _ _ => true) .left (keep noneP [⟨some 1, 0⟩]) [] ≠
      join (fun _ _ => true) .left [⟨some 1, 0⟩] [] := by
  decide

/-- An anti join emits unmatched left rows: filtering its left input drops them. -/
theorem probe_left_of_anti_unsound :
    join (fun _ _ => true) .anti (keep noneP [⟨some 1, 0⟩]) [] ≠
      join (fun _ _ => true) .anti [⟨some 1, 0⟩] [] := by
  decide

/-- A RIGHT join keeps unmatched right rows: filtering its right input drops them. -/
theorem probe_right_of_right_unsound :
    join (fun _ _ => true) .right [] (keep noneP [⟨some 1, 0⟩]) ≠
      join (fun _ _ => true) .right [] [⟨some 1, 0⟩] := by
  decide

end Wvlet.JoinKeyFilter
