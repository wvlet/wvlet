/-
Licensed under the Apache License, Version 2.0.

Soundness of carrying a filter across join keys: filtering the join's output by a conjunct over the
preserved input's key gives the same rows, in the same order, as joining the two inputs each
filtered by the conjunct over its own key. The residual condition is arbitrary, and the conjunct
any three-valued function of the key, NULL included (`IS NULL`, `coalesce(k, 0) = 0`, `k <> 1`).
-/
import WvletFormal.JoinKeyFilter.Model

namespace Wvlet.JoinKeyFilter

/-- Matching keys are the same value, and not NULL. -/
theorem keyEq_eq {a b : Val} (h : keyEq a b = true) : a = b := by
  cases a <;> cases b <;> simp_all [keyEq]

/-- Rows that match a row passing the conjunct pass it too, so filtering them first changes
nothing. -/
theorem matching_keep (θ : Residual) (p : Pred) (l : Row) (h : holds p l.key = true) :
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
          rw [← keyEq_eq hh.1] at hr
          exact absurd h hr
      simp [hr, hm, ih]

/-- The kinds whose output rows each carry the left row they come from. -/
def leftRooted : Kind → Bool
  | .right => false
  | _ => true

/-- Every output row for `l` has `l` as its left row. -/
theorem rowsOf_fst (k : Kind) (hk : leftRooted k = true) (l : Row) (ms : List Row) :
    ∀ o ∈ rowsOf k l ms, o.1 = some l := by
  intro o ho
  have hmap : ∀ o ∈ ms.map (fun r => ((some l, some r) : Out)), o.1 = some l := by
    intro o ho
    obtain ⟨r, _, rfl⟩ := List.mem_map.mp ho
    rfl
  cases k with
  | right => simp [leftRooted] at hk
  | inner => exact hmap o ho
  | left =>
    cases ms with
    | nil => simp [rowsOf] at ho; subst ho; rfl
    | cons r rs => exact hmap o ho
  | semi =>
    cases ms with
    | nil => simp [rowsOf] at ho
    | cons => simp [rowsOf] at ho; subst ho; rfl
  | anti =>
    cases ms with
    | nil => simp [rowsOf] at ho; subst ho; rfl
    | cons => simp [rowsOf] at ho

/-- A filter whose value is the same on every row keeps all rows or none. -/
theorem filter_const {α : Type} (q : α → Bool) (b : Bool) :
    ∀ xs : List α, (∀ x ∈ xs, q x = b) → xs.filter q = if b then xs else []
  | [], _ => by cases b <;> rfl
  | x :: xs, h => by
    have hx := h x (by simp)
    have ih := filter_const q b xs (fun y hy => h y (by simp [hy]))
    cases b <;> simp_all

/-- The core lemma, for one left row at a time. -/
theorem core (θ : Residual) (p : Pred) (k : Kind) (hk : leftRooted k = true) (rs : List Row) :
    ∀ ls : List Row,
      (ls.flatMap fun l => rowsOf k l (matching θ rs l)).filter (fun o => holds p (leftKey o)) =
        (keep p ls).flatMap fun l => rowsOf k l (matching θ (keep p rs) l)
  | [] => rfl
  | l :: ls => by
    have ih := core θ p k hk rs ls
    have hconst := filter_const (fun o => holds p (leftKey o)) (holds p l.key)
      (rowsOf k l (matching θ rs l))
      (fun o ho => by simp [leftKey, rowsOf_fst k hk l _ o ho])
    simp only [List.flatMap_cons, List.filter_append, ih, hconst]
    by_cases hl : holds p l.key = true
    · have hk : keep p (l :: ls) = l :: keep p ls := by simp [keep, hl]
      rw [hk, List.flatMap_cons, matching_keep θ p l hl rs]
      simp [hl]
    · have hk : keep p (l :: ls) = keep p ls := by simp [keep, hl]
      rw [hk]
      simp [hl]

/-- Inner, LEFT, semi, and anti joins: a conjunct over the left input's key, above the join,
filters both inputs instead. -/
theorem sound_left (θ : Residual) (p : Pred) (k : Kind) (hk : k ≠ .right) (ls rs : List Row) :
    (join θ k ls rs).filter (fun o => holds p (leftKey o)) = join θ k (keep p ls) (keep p rs) := by
  cases k with
  | right => exact absurd rfl hk
  | inner => exact core θ p .inner rfl rs ls
  | left => exact core θ p .left rfl rs ls
  | semi => exact core θ p .semi rfl rs ls
  | anti => exact core θ p .anti rfl rs ls

/-- RIGHT joins: a conjunct over the right (preserved) input's key filters both inputs. -/
theorem sound_right (θ : Residual) (p : Pred) (ls rs : List Row) :
    (join θ .right ls rs).filter (fun o => holds p (rightKey o)) =
      join θ .right (keep p ls) (keep p rs) := by
  simp only [join, List.filter_map]
  have h := core (fun a b => θ b a) p .left rfl ls rs
  have hc : ((fun o => holds p (rightKey o)) ∘ swap) = fun o => holds p (leftKey o) := by
    funext o; rfl
  rw [hc, h]

/-- Inner join rows have equal keys on both sides. -/
theorem inner_keys (θ : Residual) (ls rs : List Row) :
    ∀ o ∈ join θ .inner ls rs, leftKey o = rightKey o := by
  intro o ho
  simp only [join, rowsOf, matching, List.mem_flatMap, List.mem_map, List.mem_filter,
    Bool.and_eq_true] at ho
  obtain ⟨l, _, r, ⟨_, hk, _⟩, rfl⟩ := ho
  simp [leftKey, rightKey, keyEq_eq hk]

theorem filter_congr' {α : Type} (p q : α → Bool) :
    ∀ xs : List α, (∀ x ∈ xs, p x = q x) → xs.filter p = xs.filter q
  | [], _ => rfl
  | x :: xs, h => by
    have hx := h x (by simp)
    have ih := filter_congr' p q xs (fun y hy => h y (by simp [hy]))
    simp only [List.filter_cons, hx, ih]

/-- Inner joins: a conjunct over the right input's key filters both inputs, too. -/
theorem sound_inner_right (θ : Residual) (p : Pred) (ls rs : List Row) :
    (join θ .inner ls rs).filter (fun o => holds p (rightKey o)) =
      join θ .inner (keep p ls) (keep p rs) := by
  rw [← sound_left θ p .inner (by decide) ls rs]
  exact filter_congr' _ _ _ (fun o ho => by rw [inner_keys θ ls rs o ho])

end Wvlet.JoinKeyFilter
