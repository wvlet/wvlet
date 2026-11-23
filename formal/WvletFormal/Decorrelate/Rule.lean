/-
Licensed under the Apache License, Version 2.0.

How the engine decorrelates `[NOT] EXISTS` and `IN` over a correlated subquery (wvlet-engine's
`Planner.correlatedJoin` and `Planner.decorrelateExists`), and the proofs that each plan returns
SQL's rows (and values) under three-valued logic, NULLs and duplicates included.

The subquery is `from rs where w1 and ... and wn` (with `select c` for IN), evaluated for each
outer row with its columns (`L`) in scope. `planCorrelated` splits the conjuncts: those that read
only the subquery's own columns (`R`) filter its input (`locals`); the others are the correlated
conditions (`corrs`), which the join evaluates on the joined row (hash keys for the equalities, a
residual for the rest).

- A filter conjunct `[NOT] EXISTS` is a semi (anti) join of the outer input with the filtered
  body on the correlated conditions (`planExists`).
- A filter conjunct `t IN (subquery)` is a semi join that also compares `t = c` (`planIn`).
- An EXISTS under OR, NOT, or CASE is a mark: the outer input left-joined with the distinct
  correlation keys of the filtered body, and `key is not null` as the flag (`markJoin`). Every
  correlated condition must be an equality `outer = inner`.

NOT IN has its own model (`NotIn/`): its anti join needs NULL-aware conditions.
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.Decorrelate

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

/-! ### Conjunct lists -/

/-- A condition that is always TRUE: `null is null`. -/
def Cond.tt : Cond := .isNull (.lit .null)

/-- The conjunction of a list of conditions, TRUE for none. -/
def andAll : List Cond → Cond
  | [] => Cond.tt
  | [p] => p
  | p :: ps => .and p (andAll ps)

theorem holds_andAll (ps : List Cond) (x : Row) : (andAll ps).holds x = ps.all (·.holds x) := by
  induction ps with
  | nil => rfl
  | cons p ps ih =>
    cases ps with
    | nil => simp [andAll]
    | cons q qs =>
      simp only [andAll] at ih ⊢
      rw [Cond.holds_and, ih]
      simp

/-- The conjunct reads only columns of `cols`. -/
def readsOnly (p : Cond) (cols : List Col) : Bool := p.reads.all (· ∈ cols)

/-- The conjuncts that read only the subquery's columns: a filter on its input. -/
def locals (R : List Col) (ws : List Cond) : List Cond := ws.filter (readsOnly · R)

/-- The other conjuncts: conditions of the join. -/
def corrs (R : List Col) (ws : List Cond) : List Cond := ws.filter (! readsOnly · R)

/-- The subquery's WHERE holds on a joined row exactly when its correlated conditions do and its
local ones hold on the inner row. -/
theorem holds_split (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ws : List Cond) (x y : Row) :
    (andAll ws).holds (Row.merge L x y) =
      ((andAll (corrs R ws)).holds (Row.merge L x y) && (andAll (locals R ws)).holds y) := by
  simp only [holds_andAll, corrs, locals]
  induction ws with
  | nil => rfl
  | cons w ws ih =>
    by_cases h : readsOnly w R = true
    · have hr : ∀ c ∈ w.reads, c ∈ R := by simpa [readsOnly] using h
      have hw : w.holds (Row.merge L x y) = w.holds y :=
        w.holds_congr fun c hc => merge_right x y (disj c (hr c hc))
      simp only [List.all_cons, List.filter_cons, h, Bool.not_true, Bool.false_eq_true, ite_false,
        ite_true, hw, ih]
      cases w.holds y <;> simp
    · simp only [Bool.not_eq_true] at h
      simp only [List.all_cons, List.filter_cons, h, Bool.not_false, ite_true, Bool.false_eq_true,
        ite_false, ih, Bool.and_assoc]

theorem any_filter' (p q : Row → Bool) (rs : Rel) :
    (rs.filter p).any q = rs.any fun y => p y && q y := by
  induction rs with
  | nil => rfl
  | cons y ys ih => by_cases h : p y = true <;> simp_all

/-! ### SQL's meaning -/

/-- `EXISTS (from rs where w)` for the outer row `x`: some inner row makes `w` TRUE. Never
unknown. -/
def existsSql (L : List Col) (w : Cond) (x : Row) (rs : Rel) : Bool :=
  rs.any fun y => w.holds (Row.merge L x y)

/-- `where [not] exists (from rs where w)`. -/
def whereExists (L : List Col) (w : Cond) (neg : Bool) (ls rs : Rel) : Rel :=
  ls.filter fun x => existsSql L w x rs != neg

/-- `t IN (from rs where w select c)` for the outer row `x`. -/
def inSql (L : List Col) (t : Term) (c : Col) (w : Cond) (x : Row) (rs : Rel) : Tri :=
  inTri (t.eval x) ((rs.filter fun y => w.holds (Row.merge L x y)).map (· c))

/-- `where t in (from rs where w select c)`. -/
def whereIn (L : List Col) (t : Term) (c : Col) (w : Cond) (ls rs : Rel) : Rel :=
  ls.filter fun x => inSql L t c w x rs == .true

/-! ### The plans -/

/-- `correlatedJoin` for `[NOT] EXISTS`: a semi (anti) join on the correlated conditions with the
body filtered by the local ones. -/
def planExists (L R : List Col) (ws : List Cond) (neg : Bool) (ls rs : Rel) : Rel :=
  let body := filter (andAll (locals R ws)) rs
  if neg then antiJoin L (andAll (corrs R ws)) ls body
  else semiJoin L (andAll (corrs R ws)) ls body

/-- `correlatedJoin` for `IN`: a semi join on `t = c` and the correlated conditions. -/
def planIn (L R : List Col) (t : Term) (c : Col) (ws : List Cond) (ls rs : Rel) : Rel :=
  semiJoin L (.and (.eq t (.col c)) (andAll (corrs R ws))) ls (filter (andAll (locals R ws)) rs)

theorem any_filter_merge (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ws : List Cond) (x : Row)
    (rs : Rel) :
    (filter (andAll (locals R ws)) rs).any (fun y => (andAll (corrs R ws)).holds (Row.merge L x y))
      = existsSql L (andAll ws) x rs := by
  unfold existsSql filter
  rw [any_filter']
  congr 1
  funext y
  rw [holds_split L R disj ws x y, Bool.and_comm]

/-- The semi and anti joins of `correlatedJoin` are SQL's `EXISTS` and `NOT EXISTS`. The inputs'
columns must be disjoint. -/
theorem planExists_sound (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ws : List Cond) (neg : Bool)
    (ls rs : Rel) : planExists L R ws neg ls rs = whereExists L (andAll ws) neg ls rs := by
  unfold planExists whereExists semiJoin antiJoin
  cases neg <;> simp only [ite_true, Bool.false_eq_true, ite_false] <;> congr 1 <;> funext x <;>
    rw [any_filter_merge L R disj ws x rs] <;> simp

/-- IN is TRUE exactly when some matching inner row has a value equal to the target. -/
theorem inSql_true (L : List Col) (t : Term) (c : Col) (w : Cond) (x : Row) (rs : Rel) :
    (inSql L t c w x rs == .true) =
      rs.any fun y => w.holds (Row.merge L x y) && (eqTri (t.eval x) (y c) == .true) := by
  apply Bool.eq_iff_iff.mpr
  simp only [inSql, beq_iff_eq, inTri_eq_true, List.mem_map, List.mem_filter, List.any_eq_true,
    Bool.and_eq_true]
  constructor
  · rintro ⟨_, ⟨y, ⟨hy, hw⟩, rfl⟩, he⟩; exact ⟨y, hy, hw, he⟩
  · rintro ⟨y, hy, hw, he⟩; exact ⟨y c, ⟨y, ⟨hy, hw⟩, rfl⟩, he⟩

/-- The IN semi join of `correlatedJoin` is SQL's `IN` as a WHERE conjunct. The target reads only
outer columns, and the value is an inner column. -/
theorem planIn_sound (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (t : Term) (c : Col)
    (ht : ∀ c' ∈ t.reads, c' ∈ L) (hc : c ∉ L) (ws : List Cond) (ls rs : Rel) :
    planIn L R t c ws ls rs = whereIn L t c (andAll ws) ls rs := by
  unfold planIn whereIn semiJoin
  congr 1
  funext x
  rw [inSql_true]
  have hkey : ∀ y, (Cond.eq t (.col c)).holds (Row.merge L x y) =
      (eqTri (t.eval x) (y c) == .true) := fun y => by
    show (eqTri (t.eval (Row.merge L x y)) (Row.merge L x y c) == .true) = _
    rw [t.eval_congr (y := x) (fun c' hc' => merge_left x y (ht c' hc')), merge_right x y hc]
  unfold filter
  rw [any_filter']
  congr 1
  funext y
  rw [holds_split L R disj ws x y, Cond.holds_and, hkey]
  cases (andAll (locals R ws)).holds y <;> cases (andAll (corrs R ws)).holds (Row.merge L x y) <;>
    cases (eqTri (t.eval x) (y c) == .true) <;> rfl

/-! ### The EXISTS mark -/

/-- Duplicate elimination (the grouping by the correlation keys). -/
def dedup {α : Type} [DecidableEq α] : List α → List α
  | [] => []
  | a :: l => if a ∈ l then dedup l else a :: dedup l

theorem mem_dedup {α : Type} [DecidableEq α] {a : α} {l : List α} : a ∈ dedup l ↔ a ∈ l := by
  induction l with
  | nil => simp [dedup]
  | cons b l ih =>
    by_cases h : b ∈ l
    · simp only [dedup, h, ite_true, ih, List.mem_cons]
      constructor
      · exact Or.inr
      · rintro (rfl | h'); exact h; exact h'
    · simp [dedup, h, ih]

/-- A deduplicated list has at most one copy of each element. -/
theorem filter_eq_dedup {α : Type} [DecidableEq α] (k : α) (l : List α) :
    (dedup l).filter (fun d => decide (d = k)) = if k ∈ l then [k] else [] := by
  induction l with
  | nil => simp [dedup]
  | cons b l ih =>
    by_cases h : b ∈ l
    · simp only [dedup, h, ite_true, ih, List.mem_cons]
      by_cases hk : k ∈ l
      · simp [hk]
      · have : k ≠ b := fun e => hk (e ▸ h)
        simp [hk, this]
    · simp only [dedup, h, ite_false, List.filter_cons, ih, List.mem_cons]
      by_cases hb : b = k
      · subst hb; simp [h]
      · have : ¬ k = b := fun e => hb e.symm
        by_cases hk : k ∈ l <;> simp [hb, hk, this]

/-- The equality of hash keys: pairwise TRUE, so NULL keys never match. -/
def keysMatch : List Value → List Value → Bool
  | [], [] => true
  | a :: as, b :: bs => (eqTri a b == .true) && keysMatch as bs
  | _, _ => false

theorem keysMatch_eq {as bs : List Value} (h : keysMatch as bs = true) : bs = as := by
  induction as generalizing bs with
  | nil => cases bs <;> simp_all [keysMatch]
  | cons a as ih =>
    cases bs with
    | nil => simp [keysMatch] at h
    | cons b bs =>
      simp only [keysMatch, Bool.and_eq_true, beq_iff_eq] at h
      obtain ⟨_, rfl⟩ := eqTri_eq_true.mp h.1
      rw [ih h.2]

/-- Matching keys are not NULL. -/
theorem keysMatch_head {a : Value} {as bs : List Value} (h : keysMatch (a :: as) bs = true) :
    a ≠ .null := by
  cases bs with
  | nil => simp [keysMatch] at h
  | cons b bs =>
    simp only [keysMatch, Bool.and_eq_true, beq_iff_eq] at h
    exact (eqTri_eq_true.mp h.1).1

/-- The correlation keys: `outer = inner` pairs, the outer term over `L` and the inner one over
`R`. -/
abbrev Keys := List (Term × Term)

def outerKey (ks : Keys) (x : Row) : List Value := ks.map (·.1.eval x)
def innerKey (ks : Keys) (y : Row) : List Value := ks.map (·.2.eval y)

/-- The correlated conditions of the keys. -/
def keyConds (ks : Keys) : List Cond := ks.map fun (o, i) => .eq o i

/-- The keys' conditions on a joined row are the hash key equality. -/
theorem holds_keyConds (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ks : Keys)
    (hk : ∀ k ∈ ks, (∀ c ∈ k.1.reads, c ∈ L) ∧ (∀ c ∈ k.2.reads, c ∈ R)) (x y : Row) :
    (andAll (keyConds ks)).holds (Row.merge L x y) = keysMatch (outerKey ks x) (innerKey ks y) := by
  rw [holds_andAll]
  induction ks with
  | nil => rfl
  | cons k ks ih =>
    obtain ⟨o, i⟩ := k
    have ⟨ho, hi⟩ := hk (o, i) (by simp)
    have ih := ih (fun k hk' => hk k (by simp [hk']))
    simp only [keyConds, List.map_cons, List.all_cons, outerKey, innerKey, keysMatch] at ih ⊢
    rw [ih]
    congr 1
    simp only [Cond.holds, Cond.eval]
    rw [o.eval_congr (y := x) (fun c hc => merge_left x y (ho c hc)),
      i.eval_congr (y := y) (fun c hc => merge_right x y (disj c (hi c hc)))]

/-- `decorrelateExists`: for each outer row, its matches among the distinct keys of the filtered
body, or one row padded with NULL keys; the flag is `first key is not null`. Outer rows are kept
whole, so the output is the outer row with its flag. -/
def markJoin (ks : Keys) (body : Rel) (ls : Rel) : List (Row × Bool) :=
  let ds := dedup (body.map (innerKey ks))
  ls.flatMap fun x =>
    let m := ds.filter (keysMatch (outerKey ks x))
    if m.isEmpty then [(x, false)] else m.map fun d => (x, d.headD .null != .null)

theorem keysMatch_iff (k d : List Value) : keysMatch k d = (decide (d = k) && keysMatch k k) := by
  by_cases h : keysMatch k d = true
  · have := keysMatch_eq h; subst this; simp [h]
  · simp only [Bool.not_eq_true] at h
    rw [h]
    by_cases e : d = k
    · subst e; simp [h]
    · simp [e]

/-- Conditions over inner columns see the inner row of a joined row. -/
theorem all_holds_merge_right (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ws : List Cond)
    (hl : ∀ w ∈ ws, readsOnly w R = true) (x y : Row) :
    ws.all (·.holds (Row.merge L x y)) = ws.all (·.holds y) := by
  induction ws with
  | nil => rfl
  | cons w ws ih =>
    have hw : w.holds (Row.merge L x y) = w.holds y :=
      have hr : ∀ c ∈ w.reads, c ∈ R := by simpa [readsOnly] using hl w (by simp)
      w.holds_congr fun c hc => merge_right x y (disj c (hr c hc))
    simp only [List.all_cons, hw, ih (fun w' h' => hl w' (by simp [h']))]

/-- One outer row of the mark join: itself, flagged by whether a body row has its key. -/
theorem markRow (ks : Keys) (hne : ks ≠ []) (body : Rel) (x : Row) :
    (let m := (dedup (body.map (innerKey ks))).filter (keysMatch (outerKey ks x))
     if m.isEmpty then [(x, false)] else m.map fun d => (x, d.headD .null != .null)) =
      [(x, body.any fun y => keysMatch (outerKey ks x) (innerKey ks y))] := by
  generalize hkx : outerKey ks x = k
  have hfilt : (dedup (body.map (innerKey ks))).filter (keysMatch k) =
      (dedup (body.map (innerKey ks))).filter (fun d => decide (d = k) && keysMatch k k) := by
    congr 1; funext d; exact keysMatch_iff k d
  have hany : (body.any fun y => keysMatch k (innerKey ks y)) =
      (decide (k ∈ body.map (innerKey ks)) && keysMatch k k) := by
    apply Bool.eq_iff_iff.mpr
    simp only [List.any_eq_true, Bool.and_eq_true, decide_eq_true_eq, List.mem_map]
    constructor
    · rintro ⟨y, hy, hm⟩
      have := keysMatch_eq hm
      exact ⟨⟨y, hy, this⟩, this ▸ hm⟩
    · rintro ⟨⟨y, hy, rfl⟩, hm⟩; exact ⟨y, hy, hm⟩
  simp only
  rw [hfilt, hany]
  by_cases hkk : keysMatch k k = true
  · simp only [hkk, Bool.and_true]
    rw [filter_eq_dedup]
    by_cases hm : k ∈ body.map (innerKey ks)
    · -- one match, whose first key is not NULL
      obtain ⟨⟨o, i⟩, ks', rfl⟩ := List.exists_cons_of_ne_nil hne
      have hk0 : k.headD .null ≠ .null := by
        subst hkx
        simp only [outerKey, List.map_cons, List.headD_cons]
        exact keysMatch_head (by simpa [outerKey] using hkk)
      simpa [hm] using hk0
    · simp [hm]
  · simp only [Bool.not_eq_true] at hkk
    simp [hkk]

/-- The mark join keeps every outer row once, with SQL's EXISTS as its flag. The subquery's WHERE
is the key equalities `outer = inner` (at least one) and conditions over its own columns `lws`,
which filter the body. -/
theorem markJoin_sound (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ks : Keys) (hne : ks ≠ [])
    (hk : ∀ k ∈ ks, (∀ c ∈ k.1.reads, c ∈ L) ∧ (∀ c ∈ k.2.reads, c ∈ R)) (lws : List Cond)
    (hl : ∀ w ∈ lws, readsOnly w R = true) (ls rs : Rel) :
    markJoin ks (filter (andAll lws) rs) ls =
      ls.map fun x => (x, existsSql L (andAll (keyConds ks ++ lws)) x rs) := by
  have hsplit : ∀ x, existsSql L (andAll (keyConds ks ++ lws)) x rs =
      (filter (andAll lws) rs).any fun y => keysMatch (outerKey ks x) (innerKey ks y) := by
    intro x
    unfold existsSql filter
    rw [any_filter']
    congr 1
    funext y
    rw [holds_andAll, List.all_append, all_holds_merge_right L R disj lws hl x y,
      ← holds_andAll, holds_keyConds L R disj ks hk x y, holds_andAll, Bool.and_comm]
  unfold markJoin
  induction ls with
  | nil => rfl
  | cons x xs ih =>
    rw [List.flatMap_cons, List.map_cons, ih, markRow ks hne _ x, hsplit x]
    rfl

/-- So any condition over an outer row and its flag (EXISTS under OR, NOT, or CASE) keeps SQL's
rows. -/
theorem markFilter_sound (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ks : Keys) (hne : ks ≠ [])
    (hk : ∀ k ∈ ks, (∀ c ∈ k.1.reads, c ∈ L) ∧ (∀ c ∈ k.2.reads, c ∈ R)) (lws : List Cond)
    (hl : ∀ w ∈ lws, readsOnly w R = true) (f : Row → Bool → Tri) (ls rs : Rel) :
    ((markJoin ks (filter (andAll lws) rs) ls).filter (fun (x, m) => f x m == .true)).map (·.1) =
      ls.filter fun x => f x (existsSql L (andAll (keyConds ks ++ lws)) x rs) == .true := by
  rw [markJoin_sound L R disj ks hne hk lws hl, List.filter_map, List.map_map]
  induction ls with
  | nil => rfl
  | cons x xs ih => by_cases h : f x (existsSql L (andAll (keyConds ks ++ lws)) x rs) == .true <;>
      simp_all [Function.comp_def]

end Wvlet.Decorrelate
