/-
Licensed under the Apache License, Version 2.0.

`FactorOr` (wvlet-engine's `Normalize`), and the proof that it keeps the value of every condition
under three-valued logic, so a WHERE keeps the same rows.

The rule splits a conjunct `o` that is an `or` into its branches, and each branch into its
conjuncts. A conjunct whose key appears in every branch is common; with `C` the common conjuncts of
the first branch and `R i` the other conjuncts of branch `i`:

- `(C and R 1) or ... or (C and R n)` becomes `C and (R 1 or ... or R n)` (distributivity), and
- when some `R i` is empty, `C` alone (absorption: `C or (C and R) = C`).

Both are laws of Kleene logic, NULL as unknown (`factor_sound`), provided conjuncts with the same
key have the same value (`FaithfulOn`). The engine's key is the bound signature (`VExpr.sig`); the
model's fixed key is the condition itself (`faithful_id`). `Necessity.lean` shows what a key that
identifies different conditions breaks, as `InList.sig` did.
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.FactorOr

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

/-! ### Conjunctions and disjunctions of lists -/

/-- A condition that is always TRUE (`NULL is null`), the `and` of no conjunct. -/
def tt : Cond := .isNull (.lit .null)

/-- The `and` of conjuncts, reduced from the left as `FactorOr.and` does. -/
def conj : List Cond → Cond
  | [] => tt
  | p :: ps => ps.foldl .and p

/-- The `or` of branches, reduced from the left. -/
def disj : List Cond → Cond
  | [] => .not tt
  | p :: ps => ps.foldl .or p

/-- The Kleene `and` of values: TRUE for none. -/
def andT (ts : List Tri) : Tri := ts.foldr Tri.and .true

/-- The Kleene `or` of values: FALSE for none. -/
def orT (ts : List Tri) : Tri := ts.foldr Tri.or .false

theorem Tri.and_true (a : Tri) : a.and .true = a := by cases a <;> rfl
theorem Tri.or_false (a : Tri) : a.or .false = a := by cases a <;> rfl
theorem Tri.true_and (a : Tri) : Tri.true.and a = a := by cases a <;> rfl
theorem Tri.false_or (a : Tri) : Tri.false.or a = a := by cases a <;> rfl

theorem Tri.and_eq_true (a b : Tri) : a.and b = .true ↔ a = .true ∧ b = .true := by
  cases a <;> cases b <;> decide

theorem Tri.and_eq_false (a b : Tri) : a.and b = .false ↔ a = .false ∨ b = .false := by
  cases a <;> cases b <;> decide

/-- Two values are equal when they agree on being TRUE and on being FALSE. -/
theorem Tri.ext {a b : Tri} (ht : a = .true ↔ b = .true) (hf : a = .false ↔ b = .false) :
    a = b := by
  cases a <;> cases b <;> simp_all

theorem andT_eq_true (ts : List Tri) : andT ts = .true ↔ ∀ t ∈ ts, t = .true := by
  induction ts with
  | nil => simp [andT]
  | cons t ts ih =>
    simp only [andT, List.foldr_cons] at ih ⊢
    rw [Tri.and_eq_true, ih]
    simp

theorem andT_eq_false (ts : List Tri) : andT ts = .false ↔ ∃ t ∈ ts, t = .false := by
  induction ts with
  | nil => simp [andT]
  | cons t ts ih =>
    simp only [andT, List.foldr_cons] at ih ⊢
    rw [Tri.and_eq_false, ih]
    simp [eq_comm]

theorem orT_eq_true (ts : List Tri) : orT ts = .true ↔ ∃ t ∈ ts, t = .true := by
  induction ts with
  | nil => simp [orT]
  | cons t ts ih =>
    simp only [orT, List.foldr_cons] at ih ⊢
    rw [Tri.or_eq_true, ih]
    simp [eq_comm]

theorem orT_eq_false (ts : List Tri) : orT ts = .false ↔ ∀ t ∈ ts, t = .false := by
  induction ts with
  | nil => simp [orT]
  | cons t ts ih =>
    simp only [orT, List.foldr_cons] at ih ⊢
    rw [Tri.or_eq_false, ih]
    simp

theorem foldl_and_eval (x : Row) (ps : List Cond) (p : Cond) :
    (ps.foldl .and p).eval x = (p.eval x).and (andT (ps.map (·.eval x))) := by
  induction ps generalizing p with
  | nil => simp [andT, Tri.and_true]
  | cons q qs ih =>
    simp only [List.foldl_cons, ih, Cond.eval, List.map_cons, andT, List.foldr_cons,
      Wvlet.Rewrite.Tri.and_assoc]

theorem foldl_or_eval (x : Row) (ps : List Cond) (p : Cond) :
    (ps.foldl .or p).eval x = (p.eval x).or (orT (ps.map (·.eval x))) := by
  induction ps generalizing p with
  | nil => simp [orT, Tri.or_false]
  | cons q qs ih =>
    simp only [List.foldl_cons, ih, Cond.eval, List.map_cons, orT, List.foldr_cons,
      Wvlet.Rewrite.Tri.or_assoc]

/-- A conjunction is the Kleene `and` of its conjuncts. -/
theorem conj_eval (x : Row) (ps : List Cond) : (conj ps).eval x = andT (ps.map (·.eval x)) := by
  cases ps with
  | nil => rfl
  | cons p ps => simp [conj, foldl_and_eval, andT]

/-- A disjunction is the Kleene `or` of its branches. -/
theorem disj_eval (x : Row) (ps : List Cond) : (disj ps).eval x = orT (ps.map (·.eval x)) := by
  cases ps with
  | nil => rfl
  | cons p ps => simp [disj, foldl_or_eval, orT]

theorem andT_append (xs ys : List Tri) : andT (xs ++ ys) = (andT xs).and (andT ys) := by
  induction xs with
  | nil => exact (Tri.true_and _).symm
  | cons t ts ih =>
    simp only [List.cons_append, andT, List.foldr_cons] at ih ⊢
    rw [ih, Wvlet.Rewrite.Tri.and_assoc]

theorem orT_append (xs ys : List Tri) : orT (xs ++ ys) = (orT xs).or (orT ys) := by
  induction xs with
  | nil => exact (Tri.false_or _).symm
  | cons t ts ih =>
    simp only [List.cons_append, orT, List.foldr_cons] at ih ⊢
    rw [ih, Wvlet.Rewrite.Tri.or_assoc]

/-! ### The rule -/

/-- The conjuncts of a condition: every `and` is split. (The engine keeps a bound `between` as one
conjunct; either split has the same value.) -/
def conjuncts : Cond → List Cond
  | .and p q => conjuncts p ++ conjuncts q
  | c => [c]

/-- The branches of an `or`. -/
def disjuncts : Cond → List Cond
  | .or p q => disjuncts p ++ disjuncts q
  | c => [c]

theorem conj_conjuncts (x : Row) (e : Cond) : (conj (conjuncts e)).eval x = e.eval x := by
  rw [conj_eval]
  induction e with
  | and p q ihp ihq =>
    simp only [conjuncts, List.map_append, andT_append, ihp, ihq, Cond.eval]
  | _ => simp [conjuncts, andT, Tri.and_true]

theorem disj_disjuncts (x : Row) (e : Cond) : (disj (disjuncts e)).eval x = e.eval x := by
  rw [disj_eval]
  induction e with
  | or p q ihp ihq =>
    simp only [disjuncts, List.map_append, orT_append, ihp, ihq, Cond.eval]
  | _ => simp [disjuncts, orT, Tri.or_false]

variable {K : Type} [DecidableEq K]

/-- A conjunct is common when every branch has a conjunct with its key. -/
def isCommon (key : Cond → K) (bs : List (List Cond)) (c : Cond) : Bool :=
  bs.all fun b => b.any fun c' => decide (key c' = key c)

/-- `FactorOr.factor` over the branches' conjuncts: the factored conjuncts, or none when no
conjunct is common to every branch. The common conjuncts are taken from the first branch. -/
def factor (key : Cond → K) (bs : List (List Cond)) : Option (List Cond) :=
  match bs with
  | [] => none
  | h :: _ =>
    let common := h.filter (isCommon key bs)
    if common.isEmpty then none
    else
      let rest := bs.map fun b => b.filter fun c => !isCommon key bs c
      if rest.any List.isEmpty then some common
      else some (common ++ [disj (rest.map conj)])

/-- The factored conjuncts of a conjunct that is an `or`. -/
def factorCond (key : Cond → K) : Cond → Option (List Cond)
  | .or p q => factor key ((disjuncts (.or p q)).map conjuncts)
  | _ => none

/-- `FactorOr.factorConjuncts`: each conjunct of a filter condition replaced by its factored
conjuncts, or none when no conjunct factors. -/
def factorConjuncts (key : Cond → K) (e : Cond) : Option Cond :=
  let cs := conjuncts e
  let fs := cs.map (factorCond key)
  if fs.all Option.isNone then none
  else some (conj ((cs.zip fs).flatMap fun (c, fc) => fc.getD [c]))

/-- Conditions with the same key have the same value on the row. -/
def FaithfulOn (key : Cond → K) (x : Row) : Prop :=
  ∀ p q : Cond, key p = key q → p.eval x = q.eval x

/-- The condition itself is a faithful key. -/
theorem faithful_id (x : Row) : FaithfulOn (K := Cond) id x := by
  intro p q h
  simp only [id] at h
  rw [h]

/-! ### Soundness -/

section
variable {key : Cond → K} {bs : List (List Cond)} {x : Row}

/-- A common conjunct has a conjunct with its key, hence its value, in every branch. -/
theorem common_in_branch (hf : FaithfulOn key x) {c : Cond} (hc : isCommon key bs c = true)
    {b : List Cond} (hb : b ∈ bs) : ∃ c' ∈ b, c'.eval x = c.eval x ∧ isCommon key bs c' = true := by
  simp only [isCommon, List.all_eq_true, List.any_eq_true, decide_eq_true_eq] at hc
  obtain ⟨c', hc', hk⟩ := hc b hb
  refine ⟨c', hc', hf _ _ hk, ?_⟩
  simp only [isCommon, List.all_eq_true, List.any_eq_true, decide_eq_true_eq, hk]
  exact hc

end

theorem andT_map_true {α : Type} (l : List α) (f : α → Tri) :
    andT (l.map f) = .true ↔ ∀ a ∈ l, f a = .true := by
  rw [andT_eq_true]
  constructor
  · intro h a ha; exact h _ (List.mem_map_of_mem ha)
  · intro h t ht; obtain ⟨a, ha, rfl⟩ := List.mem_map.mp ht; exact h a ha

theorem andT_map_false {α : Type} (l : List α) (f : α → Tri) :
    andT (l.map f) = .false ↔ ∃ a ∈ l, f a = .false := by
  rw [andT_eq_false]
  constructor
  · rintro ⟨t, ht, htf⟩; obtain ⟨a, ha, rfl⟩ := List.mem_map.mp ht; exact ⟨a, ha, htf⟩
  · rintro ⟨a, ha, haf⟩; exact ⟨f a, List.mem_map_of_mem ha, haf⟩

theorem orT_map_true {α : Type} (l : List α) (f : α → Tri) :
    orT (l.map f) = .true ↔ ∃ a ∈ l, f a = .true := by
  rw [orT_eq_true]
  constructor
  · rintro ⟨t, ht, htt⟩; obtain ⟨a, ha, rfl⟩ := List.mem_map.mp ht; exact ⟨a, ha, htt⟩
  · rintro ⟨a, ha, hat⟩; exact ⟨f a, List.mem_map_of_mem ha, hat⟩

theorem orT_map_false {α : Type} (l : List α) (f : α → Tri) :
    orT (l.map f) = .false ↔ ∀ a ∈ l, f a = .false := by
  rw [orT_eq_false]
  constructor
  · intro h a ha; exact h _ (List.mem_map_of_mem ha)
  · intro h t ht; obtain ⟨a, ha, rfl⟩ := List.mem_map.mp ht; exact h a ha

theorem mem_filter_iff {α : Type} {p : α → Bool} {l : List α} {a : α} :
    a ∈ l.filter p ↔ a ∈ l ∧ p a = true := List.mem_filter

section
variable {x : Row} {ic : Cond → Bool} {bs : List (List Cond)} {C : List Cond}

/-- Absorption: when a branch `b0` has only common conjuncts, the `or` is the `and` of the common
conjuncts `C`. `toC`: a common conjunct has a counterpart with its value in `C`; `toB`: every
conjunct of `C` has one in every branch. -/
theorem absorb (b0 : List Cond) (hb0 : b0 ∈ bs) (hall : ∀ c ∈ b0, ic c = true)
    (toC : ∀ c, ic c = true → ∃ c' ∈ C, c'.eval x = c.eval x)
    (toB : ∀ c ∈ C, ∀ b ∈ bs, ∃ c' ∈ b, c'.eval x = c.eval x) :
    (conj C).eval x = orT (bs.map fun b => (conj b).eval x) := by
  apply Tri.ext
  · simp only [conj_eval, andT_map_true, orT_map_true]
    constructor
    · intro hc
      refine ⟨b0, hb0, fun c hc0 => ?_⟩
      obtain ⟨c', hc', he⟩ := toC c (hall c hc0)
      rw [← he]; exact hc c' hc'
    · rintro ⟨b, hb, hbt⟩ c hc
      obtain ⟨c', hc', he⟩ := toB c hc b hb
      rw [← he]; exact hbt c' hc'
  · simp only [conj_eval, andT_map_false, orT_map_false]
    constructor
    · rintro ⟨c, hc, hcf⟩ b hb
      obtain ⟨c', hc', he⟩ := toB c hc b hb
      exact ⟨c', hc', he ▸ hcf⟩
    · intro hb
      obtain ⟨c, hc, hcf⟩ := hb b0 hb0
      obtain ⟨c', hc', he⟩ := toC c (hall c hc)
      exact ⟨c', hc', he ▸ hcf⟩

/-- Distributivity: the `or` is the `and` of the common conjuncts `C` and the `or` of the other
conjuncts of each branch. -/
theorem distrib (toC : ∀ c, ic c = true → ∃ c' ∈ C, c'.eval x = c.eval x)
    (toB : ∀ c ∈ C, ∀ b ∈ bs, ∃ c' ∈ b, c'.eval x = c.eval x) :
    (conj (C ++ [disj (bs.map fun b => conj (b.filter fun c => !ic c))])).eval x =
      orT (bs.map fun b => (conj b).eval x) := by
  rw [conj_eval, List.map_append, andT_append, List.map_singleton]
  have hs : ∀ t : Tri, andT [t] = t := fun t => Tri.and_true t
  rw [hs, disj_eval, List.map_map]
  apply Tri.ext
  · rw [Tri.and_eq_true]
    simp only [Function.comp_def, conj_eval, andT_map_true, orT_map_true, mem_filter_iff,
      and_imp]
    constructor
    · rintro ⟨hc, b, hb, hbt⟩
      refine ⟨b, hb, fun c hc0 => ?_⟩
      cases hic : ic c
      · exact hbt c hc0 (by simp [hic])
      · obtain ⟨c', hc', he⟩ := toC c hic
        rw [← he]; exact hc c' hc'
    · rintro ⟨b, hb, hbt⟩
      refine ⟨fun c hc => ?_, b, hb, fun c hc0 _ => hbt c hc0⟩
      obtain ⟨c', hc', he⟩ := toB c hc b hb
      rw [← he]; exact hbt c' hc'
  · rw [Tri.and_eq_false]
    simp only [Function.comp_def, conj_eval, andT_map_false, orT_map_false, mem_filter_iff]
    constructor
    · rintro (⟨c, hc, hcf⟩ | hr) b hb
      · obtain ⟨c', hc', he⟩ := toB c hc b hb
        exact ⟨c', hc', he ▸ hcf⟩
      · obtain ⟨c, ⟨hc, -⟩, hcf⟩ := hr b hb
        exact ⟨c, hc, hcf⟩
    · intro hb
      by_cases hcf : ∃ c ∈ C, c.eval x = .false
      · exact Or.inl hcf
      · refine Or.inr fun b hbm => ?_
        obtain ⟨c, hc, hcf'⟩ := hb b hbm
        cases hic : ic c
        · exact ⟨c, ⟨hc, by simp [hic]⟩, hcf'⟩
        · obtain ⟨c', hc', he⟩ := toC c hic
          exact absurd ⟨c', hc', he ▸ hcf'⟩ hcf

end

/-- Factoring keeps the value of the `or`, on every row whose conjuncts with the same key have the
same value: the factored conjuncts' `and` is the `or` of the branches. -/
theorem factor_sound (key : Cond → K) (bs : List (List Cond)) (cs : List Cond)
    (h : factor key bs = some cs) (x : Row) (hf : FaithfulOn key x) :
    (conj cs).eval x = (disj (bs.map conj)).eval x := by
  rw [disj_eval, List.map_map, Function.comp_def]
  match bs, h with
  | hd :: tl, h =>
    have toC : ∀ c, isCommon key (hd :: tl) c = true →
        ∃ c' ∈ hd.filter (isCommon key (hd :: tl)), c'.eval x = c.eval x := by
      intro c hc
      obtain ⟨c', hc', he, hcc⟩ := common_in_branch hf hc List.mem_cons_self
      exact ⟨c', List.mem_filter.mpr ⟨hc', hcc⟩, he⟩
    have toB : ∀ c ∈ hd.filter (isCommon key (hd :: tl)), ∀ b ∈ hd :: tl,
        ∃ c' ∈ b, c'.eval x = c.eval x := by
      intro c hc b hb
      obtain ⟨c', hc', he, -⟩ := common_in_branch hf (List.mem_filter.mp hc).2 hb
      exact ⟨c', hc', he⟩
    simp only [factor] at h
    split at h
    · contradiction
    split at h
    · cases h
      rename_i hrest
      obtain ⟨r, hr, hre⟩ := List.any_eq_true.mp hrest
      obtain ⟨b0, hb0, rfl⟩ := List.mem_map.mp hr
      refine absorb b0 hb0 (fun c hc => ?_) toC toB
      cases hic : isCommon key (hd :: tl) c
      · have : c ∈ b0.filter fun c => !isCommon key (hd :: tl) c :=
          List.mem_filter.mpr ⟨hc, by simp [hic]⟩
        simp_all
      · rfl
    · cases h
      simpa only [List.map_map, Function.comp_def] using distrib toC toB

/-- `factorCond` keeps the value of the conjunct. -/
theorem factorCond_sound (key : Cond → K) (e : Cond) (cs : List Cond)
    (h : factorCond key e = some cs) (x : Row) (hf : FaithfulOn key x) :
    (conj cs).eval x = e.eval x := by
  cases e with
  | or p q =>
    rw [factor_sound key _ cs h x hf, disj_eval, List.map_map, ← disj_disjuncts x (.or p q),
      disj_eval]
    rw [List.map_map]
    congr 1
    apply List.map_congr_left
    intro b _
    simp only [Function.comp_def]
    exact conj_conjuncts x b
  | _ => simp [factorCond] at h

/-- `factorConjuncts` keeps the value of the condition. -/
theorem factorConjuncts_sound (key : Cond → K) (e e' : Cond) (h : factorConjuncts key e = some e')
    (x : Row) (hf : FaithfulOn key x) : e'.eval x = e.eval x := by
  simp only [factorConjuncts] at h
  split at h
  · contradiction
  cases h
  rw [← conj_conjuncts x e, conj_eval, conj_eval]
  -- conjunct by conjunct
  have : ∀ cs : List Cond,
      andT (((cs.zip (cs.map (factorCond key))).flatMap fun (c, fc) => fc.getD [c]).map
        (·.eval x)) = andT (cs.map (·.eval x)) := by
    intro cs
    induction cs with
    | nil => rfl
    | cons c cs ih =>
      simp only [List.map_cons, List.zip_cons_cons, List.flatMap_cons, List.map_append,
        andT_append, ih]
      congr 1
      cases hc : factorCond key c with
      | none => simp [andT, Tri.and_true]
      | some fc =>
        simp only [Option.getD_some]
        rw [← conj_eval, factorCond_sound key c fc hc x hf]
  exact this _

/-- A WHERE over the factored condition keeps the same rows. -/
theorem filter_sound (key : Cond → K) (e e' : Cond) (h : factorConjuncts key e = some e')
    (r : Rel) (hf : ∀ x ∈ r, FaithfulOn key x) : filter e' r = filter e r := by
  unfold filter
  apply List.filter_congr
  intro x hx
  simp only [Cond.holds, factorConjuncts_sound key e e' h x (hf x hx)]

/-- With the condition itself as the key, factoring always keeps the rows. -/
theorem filter_sound_id (e e' : Cond) (h : factorConjuncts (K := Cond) id e = some e') (r : Rel) :
    filter e' r = filter e r :=
  filter_sound id e e' h r fun x _ => faithful_id x

end Wvlet.FactorOr
