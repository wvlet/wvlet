/-
Licensed under the Apache License, Version 2.0.

Model of the common-type and coercion rules of `wvlet.lang.compiler.typer.TypeInference`
(wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/TypeInference.scala): `unify`,
`findCommonType`, `canCoerce`, and `widen`. Each definition mirrors the order of the Scala `match`.
-/

namespace Wvlet.Typer

/--
The types the coercion rules distinguish. `other n` stands for any data type that no rule
mentions, which the rules compare only by equality: date, short, byte, real, decimal(p, s),
varchar(n), arrays, and so on. `error` abstracts every `ErrorType(msg)`; the message is not
modelled. Type variables are out of scope (see formal/README.md).
-/
inductive Ty where
  | noType
  | null
  | int
  | long
  | float
  | double
  | boolean
  | string
  | other (id : Nat)
  | error
  deriving DecidableEq, Repr, Inhabited

namespace Ty

/-- Position in the numeric widening chain int < long < float < double. -/
def numRank : Ty → Option Nat
  | int => some 0
  | long => some 1
  | float => some 2
  | double => some 3
  | _ => none

def isNumeric (t : Ty) : Bool := t.numRank.isSome

end Ty

open Ty

/-! ## The Scala functions -/

/-- `TypeInference.unify`. -/
def unify (t1 t2 : Ty) : Ty :=
  if t1 = t2 then t1
  else
    match t1, t2 with
    | noType, t => t
    | t, noType => t
    | null, t => t
    | t, null => t
    | int, long | long, int => long
    | int, float | float, int => float
    | int, double | double, int => double
    | long, float | float, long => float
    | long, double | double, long => double
    | float, double | double, float => double
    | error, _ => error
    | _, error => error
    | _, _ => error

/-- `TypeInference.canCoerce`. -/
def canCoerce (src dst : Ty) : Bool :=
  if src = dst then true
  else
    match src, dst with
    | null, _ => true
    | noType, _ | _, noType => true
    | int, long | int, float | int, double => true
    | long, float | long, double => true
    | float, double => true
    | int, string | long, string | float, string | double, string | boolean, string => true
    | _, _ => false

/-- `TypeInference.widen`. -/
def widen (src dst : Ty) : Ty :=
  if canCoerce src dst then dst else error

/-- `TypeInference.findNumericCommonType`: the widest numeric type present. -/
def findNumericCommonType (ts : List Ty) : Option Ty :=
  if ts.all isNumeric then [double, float, long, int].find? (· ∈ ts) else none

/-- `TypeInference.findStringCommonType`. -/
def findStringCommonType (ts : List Ty) : Option Ty :=
  if ts.all (· = string) then some string else none

/-- `TypeInference.findCommonType`. -/
def findCommonType (ts : List Ty) : Ty :=
  match ts.filter (· ≠ null) with
  | [] => if ts.isEmpty then noType else null
  | h :: rest =>
    if (h :: rest).all (· = h) then h
    else if error ∈ ts then error
    else
      ((findNumericCommonType (h :: rest)).orElse fun _ => findStringCommonType (h :: rest)).getD
        error

/-! ## The widening order

`le a b`: a value of type `a` can be widened to `b`. NoType is below everything, NULL is below
every type, the numeric types form the chain int < long < float < double, and `error` is the top.
It is `canCoerce` without the string-concatenation rule and without treating NoType as a
wildcard (`le_iff_canCoerce`).
-/

def le (a b : Ty) : Bool :=
  a = b || a = noType || (a = null && b ≠ noType) || b = error ||
    match a.numRank, b.numRank with
    | some i, some j => decide (i ≤ j)
    | _, _ => false

theorem le_refl (a : Ty) : le a a = true := by simp [le]

theorem le_antisymm (a b : Ty) (h1 : le a b = true) (h2 : le b a = true) : a = b := by
  cases a <;> cases b <;> simp_all [le, numRank]

theorem le_trans (a b c : Ty) (h1 : le a b = true) (h2 : le b c = true) : le a c = true := by
  cases a <;> cases b <;> cases c <;> simp_all [le, numRank] <;> omega

/-- `r` is the least upper bound of `a` and `b` in the widening order. -/
def IsLub (r a b : Ty) : Prop :=
  le a r = true ∧ le b r = true ∧ ∀ c, le a c = true → le b c = true → le r c = true

theorem lub_unique {r s a b : Ty} (hr : IsLub r a b) (hs : IsLub s a b) : r = s :=
  le_antisymm r s (hr.2.2 s hs.1 hs.2.1) (hs.2.2 r hr.1 hr.2.1)

/-! ## Laws of `unify` -/

/-- `unify` is the least upper bound in the widening order. -/
theorem unify_isLub (a b : Ty) : IsLub (unify a b) a b := by
  refine ⟨?_, ?_, ?_⟩
  · cases a <;> cases b <;> simp [unify, le, numRank] <;> (try split) <;> simp_all
  · cases a <;> cases b <;> simp [unify, le, numRank] <;> (try split) <;> simp_all <;> omega
  · intro c
    cases a <;> cases b <;> cases c <;> simp [unify, le, numRank] <;>
      (try split) <;> simp_all <;> omega

theorem unify_comm (a b : Ty) : unify a b = unify b a := by
  apply lub_unique (unify_isLub a b)
  have h := unify_isLub b a
  exact ⟨h.2.1, h.1, fun c ha hb => h.2.2 c hb ha⟩

theorem unify_idem (a : Ty) : unify a a = a := by simp [unify]

theorem unify_assoc (a b c : Ty) : unify (unify a b) c = unify a (unify b c) := by
  have hab := unify_isLub a b
  have hbc := unify_isLub b c
  have l := unify_isLub (unify a b) c
  have r := unify_isLub a (unify b c)
  apply le_antisymm
  · -- unify a (unify b c) is an upper bound of (unify a b) and c
    apply l.2.2
    · exact hab.2.2 _ r.1 (le_trans _ _ _ hbc.1 r.2.1)
    · exact le_trans _ _ _ hbc.2.1 r.2.1
  · apply r.2.2
    · exact le_trans _ _ _ hab.1 l.1
    · exact hbc.2.2 _ (le_trans _ _ _ hab.2.1 l.1) l.2.1

theorem unify_noType_left (t : Ty) : unify noType t = t := by
  cases t <;> simp [unify]

theorem unify_noType_right (t : Ty) : unify t noType = t := by
  rw [unify_comm]; exact unify_noType_left t

/-- NULL is the identity of `unify` on every type except NoType (where NoType wins). -/
theorem unify_null_left (t : Ty) (h : t ≠ noType) : unify null t = t := by
  cases t <;> simp_all [unify]

theorem unify_null_right (t : Ty) (h : t ≠ noType) : unify t null = t := by
  rw [unify_comm]; exact unify_null_left t h

theorem unify_error_left (t : Ty) : unify error t = error := by
  cases t <;> simp [unify]

theorem unify_error_right (t : Ty) : unify t error = error := by
  rw [unify_comm]; exact unify_error_left t

/-! ## `unify` against `canCoerce` -/

theorem unify_ne_noType (a b : Ty) (ha : a ≠ noType) : unify a b ≠ noType := by
  intro h
  have hl := (unify_isLub a b).1
  rw [h] at hl
  cases a <;> simp_all [le, numRank]

/-- The string-concatenation rule of `canCoerce`: a number or boolean becomes a string. -/
def stringRule (a b : Ty) : Bool :=
  b = string && (a.isNumeric || a = boolean)

/-- Away from NoType and errors, the widening order is `canCoerce` minus the string rule. -/
theorem le_iff_canCoerce (a b : Ty) (ha : a ≠ noType) (hb : b ≠ noType) (he : b ≠ error) :
    le a b = (canCoerce a b && !stringRule a b) := by
  cases a <;> cases b <;> simp_all [le, canCoerce, stringRule, numRank, isNumeric]

/-- Upper bound: both sides of a successful `unify` can be coerced to the result. -/
theorem unify_upper_canCoerce (a b : Ty) (h : unify a b ≠ error) :
    canCoerce a (unify a b) = true ∧ canCoerce b (unify a b) = true := by
  cases a <;> cases b <;> simp_all [unify, canCoerce] <;> split at * <;> simp_all

/--
Least: any type that both sides widen to, without the string rule, is reachable from the result.
With the string rule, int and boolean both coerce to string, yet `unify int boolean = error`.
-/
theorem unify_least_canCoerce (a b c : Ty)
    (ha : a ≠ noType) (hb : b ≠ noType) (hc : c ≠ noType)
    (hce : c ≠ error) (hac : canCoerce a c = true) (hbc : canCoerce b c = true)
    (hsa : stringRule a c = false) (hsb : stringRule b c = false) :
    canCoerce (unify a b) c = true := by
  have hla : le a c = true := by rw [le_iff_canCoerce a c ha hc hce]; simp [hac, hsa]
  have hlb : le b c = true := by rw [le_iff_canCoerce b c hb hc hce]; simp [hbc, hsb]
  have hr := (unify_isLub a b).2.2 c hla hlb
  have hu : unify a b ≠ noType := unify_ne_noType a b ha
  have := le_iff_canCoerce (unify a b) c hu hc hce
  rw [hr] at this
  have h2 := this.symm
  simp only [Bool.and_eq_true] at h2
  exact h2.1

theorem unify_counterexample_string :
    canCoerce int string = true ∧ canCoerce boolean string = true ∧
      unify int boolean = error := by
  decide

/-! ## `widen` -/

theorem widen_spec (a b : Ty) : widen a b = if canCoerce a b then b else error := rfl

theorem widen_ok (a b : Ty) (h : widen a b ≠ error) : canCoerce a b = true ∧ widen a b = b := by
  unfold widen at *; split at * <;> simp_all

/-! ## `findCommonType` is a fold of `unify` -/

/-- `r` is the least upper bound of all elements of `ts`. -/
def IsLubList (r : Ty) (ts : List Ty) : Prop :=
  (∀ x ∈ ts, le x r = true) ∧ ∀ c, (∀ x ∈ ts, le x c = true) → le r c = true

theorem lubList_unique {r s : Ty} {ts : List Ty} (hr : IsLubList r ts) (hs : IsLubList s ts) :
    r = s :=
  le_antisymm r s (hr.2 s hs.1) (hs.2 r hr.1)

theorem foldl_unify_isLub (acc : Ty) (ts : List Ty) :
    IsLubList (ts.foldl unify acc) (acc :: ts) := by
  induction ts generalizing acc with
  | nil => exact ⟨by simp [le_refl], fun c h => h acc (by simp)⟩
  | cons x xs ih =>
    have hu := unify_isLub acc x
    have hi := ih (unify acc x)
    simp only [List.foldl]
    refine ⟨?_, ?_⟩
    · intro y hy
      simp only [List.mem_cons] at hy
      rcases hy with rfl | rfl | hy
      · exact le_trans _ _ _ hu.1 (hi.1 _ (by simp))
      · exact le_trans _ _ _ hu.2.1 (hi.1 _ (by simp))
      · exact hi.1 y (by simp [hy])
    · intro c hc
      apply hi.2 c
      intro y hy
      simp only [List.mem_cons] at hy
      rcases hy with rfl | hy
      · exact hu.2.2 c (hc _ (by simp)) (hc _ (by simp))
      · exact hc y (by simp [hy])

theorem foldl_unify_noType_isLub (ts : List Ty) : IsLubList (ts.foldl unify noType) ts := by
  have h := foldl_unify_isLub noType ts
  refine ⟨fun x hx => h.1 x (by simp [hx]), fun c hc => h.2 c ?_⟩
  intro y hy
  simp only [List.mem_cons] at hy
  rcases hy with rfl | hy
  · simp [le]
  · exact hc y hy

private theorem find_widest_isLub (ts : List Ty) (h : ts ≠ []) (hn : ts.all isNumeric = true) :
    ∃ r, [double, float, long, int].find? (· ∈ ts) = some r ∧ IsLubList r ts := by
  have hmem : ∀ x ∈ ts, x = int ∨ x = long ∨ x = float ∨ x = double := by
    intro x hx
    have := List.all_eq_true.mp hn x hx
    cases x <;> simp_all [isNumeric, numRank]
  obtain ⟨y, hy⟩ := List.exists_mem_of_ne_nil ts h
  by_cases hd : double ∈ ts
  · refine ⟨double, by simp [List.find?, hd], ?_, fun c hc => hc _ hd⟩
    intro x hx; rcases hmem x hx with rfl | rfl | rfl | rfl <;> simp [le, numRank]
  by_cases hf : float ∈ ts
  · refine ⟨float, by simp [List.find?, hd, hf], ?_, fun c hc => hc _ hf⟩
    intro x hx; rcases hmem x hx with rfl | rfl | rfl | rfl <;> simp_all [le, numRank]
  by_cases hl : long ∈ ts
  · refine ⟨long, by simp [List.find?, hd, hf, hl], ?_, fun c hc => hc _ hl⟩
    intro x hx; rcases hmem x hx with rfl | rfl | rfl | rfl <;> simp_all [le, numRank]
  have hi : int ∈ ts := by rcases hmem y hy with rfl | rfl | rfl | rfl <;> simp_all
  refine ⟨int, by simp [List.find?, hd, hf, hl, hi], ?_, fun c hc => hc _ hi⟩
  intro x hx; rcases hmem x hx with rfl | rfl | rfl | rfl <;> simp_all [le]

/-- A non-numeric type and a different type have no common upper bound except `error`. -/
private theorem top_of_incomparable (e f c : Ty) (hen : e.isNumeric = false) (hfe : f ≠ e)
    (he0 : e ≠ noType) (hf0 : f ≠ noType) (he1 : e ≠ null) (hf1 : f ≠ null)
    (he2 : e ≠ error) (hf2 : f ≠ error) (hec : le e c = true) (hfc : le f c = true) : le error c = true := by
  cases e <;> cases f <;> cases c <;> simp_all [le, isNumeric, numRank]

/--
On lists without NoType (as `CASE` typing calls it), `findCommonType` is the least upper bound.
-/
theorem findCommonType_isLub (ts : List Ty) (hts : noType ∉ ts) :
    IsLubList (findCommonType ts) ts := by
  unfold findCommonType
  split
  · -- every element is NULL
    rename_i hnil
    have hall : ∀ x ∈ ts, x = null := by
      intro x hx
      apply Classical.byContradiction
      intro hne
      have : x ∈ ts.filter (· ≠ null) := List.mem_filter.mpr ⟨hx, by simp [hne]⟩
      rw [hnil] at this
      simp at this
    cases ts with
    | nil => exact ⟨by simp, fun c _ => by simp [le]⟩
    | cons y ys =>
      have hy := hall y (by simp)
      subst hy
      simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte]
      refine ⟨?_, fun c hc => by simpa using hc null (by simp)⟩
      intro x hx; rw [hall x hx]; simp [le]
  · rename_i h rest hcons
    have hsub : ∀ x, x ∈ h :: rest ↔ x ∈ ts ∧ x ≠ null := by
      intro x; rw [← hcons, List.mem_filter]; simp
    have hh : h ∈ ts ∧ h ≠ null := (hsub h).mp (by simp)
    have hnt : h ≠ noType := fun e => hts (e ▸ hh.1)
    -- an element of `ts` is NULL or in `h :: rest`
    have hcase : ∀ x ∈ ts, x = null ∨ x ∈ h :: rest := by
      intro x hx; by_cases e : x = null
      · exact Or.inl e
      · exact Or.inr ((hsub x).mpr ⟨hx, e⟩)
    have hnull_le : ∀ c, c ≠ noType → le null c = true := by intro c hc; simp [le, hc]
    split
    · -- all non-NULL elements equal `h`
      rename_i hall
      refine ⟨?_, fun c hc => hc h hh.1⟩
      intro x hx
      rcases hcase x hx with rfl | hx'
      · exact hnull_le h hnt
      · have := List.all_eq_true.mp hall x hx'; simp at this; rw [this]; exact le_refl h
    · rename_i hne
      split
      · -- an error is present: it is the top
        rename_i herr
        have he : error ∈ ts := herr
        exact ⟨fun x _ => by simp [le], fun c hc => hc error he⟩
      · rename_i hnoerr
        have hnoerr' : error ∉ ts := hnoerr
        -- two distinct non-NULL elements
        obtain ⟨d, hd, hdh⟩ : ∃ d ∈ h :: rest, d ≠ h := by
          have hf : ((h :: rest).all fun x => decide (x = h)) = false := by simpa using hne
          obtain ⟨d, hd, hdh⟩ := List.all_eq_false.mp hf
          exact ⟨d, hd, by simpa using hdh⟩
        cases hnum : (h :: rest).all isNumeric with
        | true =>
          obtain ⟨r, hr, hlub⟩ := find_widest_isLub (h :: rest) (by simp) hnum
          simp only [findNumericCommonType, hnum, ↓reduceIte, hr, Option.orElse_some,
            Option.getD_some]
          refine ⟨?_, fun c hc => hlub.2 c fun x hx => hc x ((hsub x).mp hx).1⟩
          intro x hx
          rcases hcase x hx with rfl | hx'
          · apply hnull_le
            intro e; subst e
            have := List.all_eq_true.mp hnum _ (List.mem_cons_self ..)
            have hr' := hlub.1 h (by simp)
            cases h <;> simp_all [le, isNumeric, numRank]
          · exact hlub.1 x hx'
        | false =>
          have hns : (findStringCommonType (h :: rest)) = none := by
            simp only [findStringCommonType]
            split
            · rename_i hs
              have e1 := List.all_eq_true.mp hs h (by simp)
              have e2 := List.all_eq_true.mp hs d hd
              simp at e1 e2; exact absurd (e2.trans e1.symm) hdh
            · rfl
          simp only [findNumericCommonType, hnum, Bool.false_eq_true, ↓reduceIte, hns,
            Option.orElse_none, Option.getD_none]
          refine ⟨fun x _ => by simp [le], ?_⟩
          -- the only common upper bound of a non-numeric element and a different one is error
          intro c hc
          obtain ⟨e, he, hen⟩ : ∃ e ∈ h :: rest, e.isNumeric = false := by
            simp only [List.all_eq_false] at hnum
            obtain ⟨e, he, hen⟩ := hnum; exact ⟨e, he, by simpa using hen⟩
          obtain ⟨f, hf, hfe⟩ : ∃ f ∈ h :: rest, f ≠ e := by
            by_cases heh : e = h
            · exact ⟨d, hd, heh ▸ hdh⟩
            · exact ⟨h, by simp, fun x => heh x.symm⟩
          have he' := (hsub e).mp he
          have hf' := (hsub f).mp hf
          have hec := hc e he'.1
          have hfc := hc f hf'.1
          have hent : e ≠ noType := fun x => hts (x ▸ he'.1)
          have hfnt : f ≠ noType := fun x => hts (x ▸ hf'.1)
          have hee : e ≠ error := fun x => hnoerr' (x ▸ he'.1)
          have hfe' : f ≠ error := fun x => hnoerr' (x ▸ hf'.1)
          exact top_of_incomparable e f c hen hfe hent hfnt he'.2 hf'.2 hee hfe' hec hfc

theorem findCommonType_eq_foldl (ts : List Ty) (hts : noType ∉ ts) :
    findCommonType ts = ts.foldl unify noType :=
  lubList_unique (findCommonType_isLub ts hts) (foldl_unify_noType_isLub ts)

/-- With NoType present the two differ, which is why `CASE` typing filters NoType out first. -/
theorem findCommonType_noType_counterexample :
    findCommonType [null, noType] = noType ∧ [null, noType].foldl unify noType = null := by
  decide

end Wvlet.Typer
