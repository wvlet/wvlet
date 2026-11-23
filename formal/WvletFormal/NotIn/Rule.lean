/-
Licensed under the Apache License, Version 2.0.

How the engine binds `x NOT IN (subquery)` (wvlet-engine), and the proofs that each binding
returns SQL's rows (`notIn`, `Algebra/Bag.lean`) under three-valued logic.

- An uncorrelated NOT IN conjunct (`RuntimeBinder.plans.subqueryJoin`) evaluates the subquery at
  plan time. Over no values every row passes; over a NULL value no row does; otherwise it is a
  hash anti join on `x = y` followed by `x is not null` (`uncorrelated`).
- A correlated NOT IN (`Planner.correlatedJoin`) is an anti join whose condition is the
  correlation and `x = y or x is null or y is null` (`correlated`).
- Elsewhere (a select item, under OR or NOT, or with `Decorrelate` off), the subquery's values
  become an `InList` set: the non-NULL values and whether one was NULL (`inList`).

Throughout, the target `t` reads only left columns (`L`) and the subquery's value is a column `c`
off them.
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.NotIn

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

/-- The equality of the hash anti join's keys, on the joined row. -/
def keyEq (t : Term) (c : Col) : Cond := .eq t (.col c)

/-- The rows of the plan `RuntimeBinder` builds for an uncorrelated NOT IN conjunct. -/
def uncorrelated (L : List Col) (t : Term) (c : Col) (ls rs : Rel) : Rel :=
  if rs.isEmpty then ls
  else if rs.any (fun y => y c == .null) then []
  else filter (.not (.isNull t)) (antiJoin L (keyEq t c) ls rs)

/-- The condition of the anti join `Planner.correlatedJoin` builds for a correlated NOT IN. -/
def nullAware (t : Term) (c : Col) : Cond :=
  .or (.or (keyEq t c) (.isNull t)) (.isNull (.col c))

/-- The rows of the plan for a NOT IN correlated by `corr`. -/
def correlated (L : List Col) (corr : Cond) (t : Term) (c : Col) (ls rs : Rel) : Rel :=
  antiJoin L (.and corr (nullAware t c)) ls rs

/-- SQL's correlated NOT IN: for each left row, NOT IN over the right rows the correlation
matches. -/
def notInCorrelated (L : List Col) (corr : Cond) (t : Term) (c : Col) (ls rs : Rel) : Rel :=
  ls.filter fun x =>
    (inTri (t.eval x) ((rs.filter fun y => corr.holds (Row.merge L x y)).map (· c))).not == .true

/-- `InList`'s value of `v IN (set)`, the set being the subquery's values `vs`. A NULL `v` is
unknown unless the set is empty and has no NULL. -/
def inList (v : Value) (vs : List Value) : Tri :=
  if v = .null then (if vs.isEmpty then .false else .unknown)
  else if vs.contains v then .true
  else if vs.contains .null then .unknown
  else .false

/-! ### The target and the value on joined rows -/

section
variable {L : List Col} {t : Term} {c : Col}

theorem target_merge (ht : ∀ c' ∈ t.reads, c' ∈ L) (x y : Row) :
    t.eval (Row.merge L x y) = t.eval x :=
  t.eval_congr fun c' hc => merge_left x y (ht c' hc)

theorem value_merge (hc : c ∉ L) (x y : Row) : (Term.col c).eval (Row.merge L x y) = y c :=
  merge_right x y hc

end

/-! ### Soundness -/

/-- The key equality on a joined row compares the target with the value. -/
theorem keyEq_holds {L : List Col} {t : Term} {c : Col} (ht : ∀ c' ∈ t.reads, c' ∈ L)
    (hc : c ∉ L) (x y : Row) :
    (keyEq t c).holds (Row.merge L x y) = (eqTri (t.eval x) (y c) == .true) := by
  simp only [keyEq, Cond.holds, Cond.eval, target_merge ht, value_merge hc]

/-- Over values without NULL, `x = y` is never unknown for a non-NULL `x`. -/
theorem eqTri_not_unknown {a b : Value} (ha : a ≠ .null) (hb : b ≠ .null) :
    eqTri a b ≠ .unknown := fun h => by
  rcases eqTri_eq_unknown.mp h with h | h <;> contradiction

/-- NOT IN over some value is never TRUE for a NULL target. -/
theorem notIn_null_target {vs : List Value} (hne : vs ≠ []) :
    (inTri .null vs).not ≠ .true := fun h => by
  obtain ⟨y, hy⟩ := List.exists_mem_of_ne_nil vs hne
  have := (notIn_tri_true _ _).mp h y hy
  simp [eqTri] at this

/-- The uncorrelated binding is SQL's NOT IN. -/
theorem uncorrelated_sound (L : List Col) (t : Term) (c : Col) (ht : ∀ c' ∈ t.reads, c' ∈ L)
    (hc : c ∉ L) (ls rs : Rel) : uncorrelated L t c ls rs = notIn t c ls rs := by
  unfold uncorrelated notIn
  by_cases he : rs.isEmpty
  · have : rs = [] := List.isEmpty_iff.mp he
    subst this
    symm
    apply List.filter_eq_self.mpr
    intro _ _
    rfl
  · have hne : rs ≠ [] := by simpa using he
    have hmne : rs.map (· c) ≠ [] := by simpa using hne
    simp only [he, Bool.false_eq_true, ite_false]
    by_cases hn : rs.any (fun y => y c == .null)
    · simp only [hn, ite_true]
      -- every left row meets a NULL value: unknown, so none passes
      obtain ⟨y, hy, hyn⟩ := List.any_eq_true.mp hn
      have hyn : y c = .null := by simpa using hyn
      symm
      apply List.filter_eq_nil_iff.mpr
      intro x _ hx
      have hx : (inTri (t.eval x) (rs.map (· c))).not = .true := by simpa using hx
      have hall := (notIn_tri_true _ _).mp hx (y c) (List.mem_map_of_mem (f := (· c)) hy)
      rw [hyn] at hall
      cases h : t.eval x <;> simp [eqTri, h] at hall
    · simp only [hn, Bool.false_eq_true, ite_false]
      have hnn : ∀ y ∈ rs, y c ≠ .null := by
        intro y hy hyn
        exact hn (List.any_eq_true.mpr ⟨y, hy, by simp [hyn]⟩)
      unfold filter antiJoin
      rw [List.filter_filter]
      congr 1
      funext x
      simp only [keyEq_holds ht hc]
      by_cases hx : t.eval x = .null
      · -- a NULL target: dropped by the filter, and NOT IN is not TRUE
        have hl : (Cond.not (.isNull t)).holds x = false := by
          simp [Cond.holds, Cond.eval, hx, Tri.not]
        have hr : ((inTri (t.eval x) (rs.map (· c))).not == .true) = false := by
          rw [hx]; simpa using notIn_null_target hmne
        rw [hl, hr]; rfl
      · have hl : (Cond.not (.isNull t)).holds x = true := by
          simp [Cond.holds, Cond.eval, hx, Tri.not]
        rw [hl, Bool.true_and]
        apply Bool.eq_iff_iff.mpr
        simp only [Bool.not_eq_true', List.any_eq_false, beq_iff_eq, notIn_tri_true,
          List.mem_map]
        constructor
        · rintro hnone _ ⟨y, hy, rfl⟩
          have h1 := hnone y hy
          have h2 := eqTri_not_unknown hx (hnn y hy)
          cases h : eqTri (t.eval x) (y c) <;> simp_all
        · intro hall y hy
          simp [hall _ ⟨y, hy, rfl⟩]

/-- `x = y or x is null or y is null` is TRUE exactly when `x = y` is not FALSE. -/
theorem nullAware_tri (a b : Value) :
    ((((eqTri a b).or (if a = .null then .true else .false)).or
        (if b = .null then .true else .false)) == .true) = !(eqTri a b == .false) := by
  by_cases ha : a = .null
  · subst ha; cases b <;> rfl
  · by_cases hb : b = .null
    · subst hb; cases a <;> simp_all [eqTri, Tri.or]
    · have := eqTri_not_unknown ha hb
      simp only [ha, hb, ite_false]
      cases h : eqTri a b <;> simp_all [Tri.or]

/-- The null-aware condition is TRUE exactly when `x = y` is not FALSE. -/
theorem nullAware_holds (L : List Col) (t : Term) (c : Col) (ht : ∀ c' ∈ t.reads, c' ∈ L)
    (hc : c ∉ L) (x y : Row) :
    (nullAware t c).holds (Row.merge L x y) = !(eqTri (t.eval x) (y c) == .false) := by
  simp only [nullAware, keyEq, Cond.holds, Cond.eval, target_merge ht, value_merge hc]
  exact nullAware_tri _ _

/-- The correlated binding is SQL's correlated NOT IN. -/
theorem correlated_sound (L : List Col) (corr : Cond) (t : Term) (c : Col)
    (ht : ∀ c' ∈ t.reads, c' ∈ L) (hc : c ∉ L) (ls rs : Rel) :
    correlated L corr t c ls rs = notInCorrelated L corr t c ls rs := by
  unfold correlated notInCorrelated antiJoin
  congr 1
  funext x
  apply Bool.eq_iff_iff.mpr
  simp only [Cond.holds_and, nullAware_holds L t c ht hc, beq_iff_eq, notIn_tri_true,
    List.mem_map, List.mem_filter, Bool.not_eq_true', List.any_eq_false, Bool.and_eq_true]
  constructor
  · rintro h _ ⟨y, ⟨hy, hcorr⟩, rfl⟩
    have := h y hy
    simp only [hcorr, true_and] at this
    cases h' : eqTri (t.eval x) (y c) <;> simp_all
  · intro h y hy
    by_cases hcorr : corr.holds (Row.merge L x y) = true
    · simp [hcorr, h _ ⟨y, ⟨hy, hcorr⟩, rfl⟩]
    · simp [hcorr]

/-- `InList` evaluates `v IN (vs)` as SQL does. -/
theorem inList_sound (v : Value) (vs : List Value) : inList v vs = inTri v vs := by
  unfold inList
  by_cases hv : v = .null
  · subst hv
    cases vs with
    | nil => rfl
    | cons y ys =>
      have : inTri .null (y :: ys) = (Tri.unknown).or (inTri .null ys) := by
        simp [inTri, eqTri]
      rw [this]
      simp only [ite_true, List.isEmpty_cons, Bool.false_eq_true, ite_false]
      have hnt : inTri .null ys ≠ .true := fun h => by
        obtain ⟨_, _, h⟩ := (inTri_eq_true _ _).mp h
        simp [eqTri] at h
      cases h : inTri .null ys <;> first | rfl | exact absurd h hnt
  · simp only [hv, ite_false]
    by_cases hm : vs.contains v = true
    · simp only [hm, ite_true]
      symm
      apply (inTri_eq_true v vs).mpr
      exact ⟨v, by simpa using hm, eqTri_eq_true.mpr ⟨hv, rfl⟩⟩
    · simp only [hm, Bool.false_eq_true, ite_false]
      have hnt : ∀ y ∈ vs, eqTri v y ≠ .true := by
        intro y hy h
        obtain ⟨_, rfl⟩ := eqTri_eq_true.mp h
        exact hm (by simpa using hy)
      by_cases hn : vs.contains .null = true
      · simp only [hn, ite_true]
        -- not TRUE (no match), not FALSE (the NULL value): unknown
        have hf : inTri v vs ≠ .false := by
          intro h
          have := (inTri_eq_false v vs).mp h .null (by simpa using hn)
          cases v <;> simp_all [eqTri]
        have ht : inTri v vs ≠ .true := fun h =>
          let ⟨y, hy, hy'⟩ := (inTri_eq_true v vs).mp h; hnt y hy hy'
        cases h : inTri v vs <;> simp_all
      · simp only [hn, Bool.false_eq_true, ite_false]
        symm
        apply (inTri_eq_false v vs).mpr
        intro y hy
        have hyn : y ≠ .null := fun h => hn (by simpa [h] using hy)
        cases h : eqTri v y
        · exact absurd h (hnt y hy)
        · rfl
        · exact absurd (eqTri_eq_unknown.mp h) (by simp [hv, hyn])

end Wvlet.NotIn
