/-
Licensed under the Apache License, Version 2.0.

Where `PushDownFilters` (wvlet-engine runtime) moves a WHERE conjunct that sits directly over a
join, and the proofs that every move keeps the rows.

The pass moves a conjunct into the preserved input of a LEFT or RIGHT join when it reads only that
input (with `outer_to_inner`, a LEFT join under a conjunct that is never TRUE on a NULL-padded right
row becomes an inner join, and the conjunct one of its conditions; see `rejectsNull`), into the left input of a semi or anti join (whose output has only left columns), and into
the condition list of an inner join tree, which `ReorderJoins` places later. A conjunct that reads
no column reads "only" any input, so it moves too. Over a FULL join, and over the null-supplying
input of an outer join, it stays above: there, NULL-padded rows can satisfy it (`c is null`) or a
removed match can turn into a padded row, so moving it changes the result (`Necessity.lean`).
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.Pushdown

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

/-- Where a conjunct goes. -/
inductive Place where
  /-- A filter over the left input. -/
  | left
  /-- A filter over the right input. -/
  | right
  /-- One more condition of the inner join tree. -/
  | cond
  /-- The LEFT join becomes an inner join tree, with the conjunct one of its conditions. -/
  | inner
  /-- It stays in a filter above the join. -/
  | above
  deriving DecidableEq, Repr

/-- The conjunct reads only columns of `cols`. -/
def readsOnly (p : Cond) (cols : List Col) : Bool := p.reads.all (· ∈ cols)

/-! ### NULL-rejecting conditions -/

/-- The term is a column of `R`. -/
def strictTerm (R : List Col) : Term → Bool
  | .col c => c ∈ R
  | .lit _ => false

/-- The condition is unknown (NULL) on every row whose columns of `R` are all NULL: a comparison
with a column of `R` as an operand, `IN` over a non-empty list (over no values it is FALSE), and
`not`, `and`, `or` of such conditions. -/
def strict (R : List Col) : Cond → Bool
  | .eq a b => strictTerm R a || strictTerm R b
  | .lt a b => strictTerm R a || strictTerm R b
  | .inList a vs => strictTerm R a && !vs.isEmpty
  | .isNull _ => false
  | .not p => strict R p
  | .and p q => strict R p && strict R q
  | .or p q => strict R p && strict R q

/-- The condition is never TRUE on a row whose columns of `R` are all NULL: a strict condition,
`c is not null` for a column `c` of `R`, an `and` with such a side, and an `or` of two such
conditions. `PushDownFilters`' `rejectsNull` (engine) recognizes the same forms. -/
def rejectsNull (R : List Col) : Cond → Bool
  | .eq a b => strict R (.eq a b)
  | .lt a b => strict R (.lt a b)
  | .inList a vs => strict R (.inList a vs)
  | .isNull _ => false
  | .not (.isNull a) => strictTerm R a
  | .not p => strict R (.not p)
  | .and p q => rejectsNull R p || rejectsNull R q
  | .or p q => rejectsNull R p && rejectsNull R q

/-- Every column of `R` is NULL in the row. -/
def NullOn (R : List Col) (z : Row) : Prop := ∀ c ∈ R, z c = .null

theorem strictTerm_eval {R : List Col} {z : Row} (hz : NullOn R z) {t : Term}
    (h : strictTerm R t = true) : t.eval z = .null := by
  cases t with
  | col c => exact hz c (by simpa [strictTerm] using h)
  | lit v => simp [strictTerm] at h

theorem eqTri_null_right (a : Value) : eqTri a .null = .unknown := by cases a <;> rfl

theorem ltTri_null_left (a : Value) : ltTri .null a = .unknown := by cases a <;> rfl

theorem ltTri_null_right (a : Value) : ltTri a .null = .unknown := by
  cases a <;> rfl

theorem inTri_null : ∀ (vs : List Value), vs.isEmpty = false → inTri .null vs = .unknown
  | [], h => by simp at h
  | [_], _ => rfl
  | y :: y' :: ys, _ => by
    have ih := inTri_null (y' :: ys) rfl
    show (eqTri .null y).or (inTri .null (y' :: ys)) = .unknown
    rw [ih]
    rfl

theorem strict_eval {R : List Col} {z : Row} (hz : NullOn R z) :
    ∀ {p : Cond}, strict R p = true → p.eval z = .unknown
  | .eq a b, h => by
    simp only [strict, Bool.or_eq_true] at h
    rcases h with h | h
    · simp only [Cond.eval, strictTerm_eval hz h]; rfl
    · simp only [Cond.eval, strictTerm_eval hz h, eqTri_null_right]
  | .lt a b, h => by
    simp only [strict, Bool.or_eq_true] at h
    rcases h with h | h
    · simp only [Cond.eval, strictTerm_eval hz h, ltTri_null_left]
    · simp only [Cond.eval, strictTerm_eval hz h, ltTri_null_right]
  | .inList a vs, h => by
    simp only [strict, Bool.and_eq_true, Bool.not_eq_true'] at h
    simp only [Cond.eval, strictTerm_eval hz h.1, inTri_null vs h.2]
  | .isNull _, h => by simp [strict] at h
  | .not p, h => by
    simp only [Cond.eval, strict_eval hz (p := p) (by simpa [strict] using h)]
    rfl
  | .and p q, h => by
    simp only [strict, Bool.and_eq_true] at h
    simp only [Cond.eval, strict_eval hz h.1, strict_eval hz h.2]
    rfl
  | .or p q, h => by
    simp only [strict, Bool.and_eq_true] at h
    simp only [Cond.eval, strict_eval hz h.1, strict_eval hz h.2]
    rfl

theorem strict_holds {R : List Col} {z : Row} (hz : NullOn R z) {p : Cond}
    (h : strict R p = true) : p.holds z = false := by
  simp [Cond.holds, strict_eval hz h]

/-- A NULL-rejecting condition is never TRUE on a row whose columns of `R` are NULL. -/
theorem rejectsNull_holds {R : List Col} {z : Row} (hz : NullOn R z) :
    ∀ {p : Cond}, rejectsNull R p = true → p.holds z = false
  | .eq a b, h => strict_holds hz h
  | .lt a b, h => strict_holds hz h
  | .inList a vs, h => strict_holds hz h
  | .isNull _, h => by simp [rejectsNull] at h
  | .not (.isNull a), h => by
    simp only [Cond.holds, Cond.eval, strictTerm_eval hz (by simpa [rejectsNull] using h)]
    rfl
  | .not (.eq a b), h => strict_holds hz h
  | .not (.lt a b), h => strict_holds hz h
  | .not (.inList a vs), h => strict_holds hz h
  | .not (.not p), h => strict_holds hz h
  | .not (.and p q), h => strict_holds hz h
  | .not (.or p q), h => strict_holds hz h
  | .and p q, h => by
    simp only [rejectsNull, Bool.or_eq_true] at h
    rw [Cond.holds_and]
    rcases h with h | h
    · simp [rejectsNull_holds hz h]
    · simp [rejectsNull_holds hz h]
  | .or p q, h => by
    simp only [rejectsNull, Bool.and_eq_true] at h
    have hp := rejectsNull_holds hz h.1
    have hq := rejectsNull_holds hz h.2
    simp only [Cond.holds, beq_eq_false_iff_ne, ne_eq] at hp hq ⊢
    simp only [Cond.eval]
    rw [Tri.or_eq_true]
    exact fun h' => h'.elim hp hq

/-- A left row padded with NULLs has NULL in every right column. -/
theorem nullOn_pad {L R : List Col} (disj : ∀ c ∈ R, c ∉ L) (x : Row) :
    NullOn R (Row.merge L x Row.null) := fun c hc => by
  simp [Row.merge, disj c hc, Row.null]

/-- `PushDownFilters`' choice for a conjunct `p` over a join of kind `k` whose inputs have the
columns `L` and `R`. -/
def place (k : JoinKind) (L R : List Col) (p : Cond) : Place :=
  match k with
  | .inner => .cond
  | .left =>
    if readsOnly p L then .left else if rejectsNull R p then .inner else .above
  | .semi | .anti => if readsOnly p L then .left else .above
  | .right => if readsOnly p R then .right else .above
  | .full => .above

/-- The rows of the plan the pass builds for `filter p (join k L on ls rs)`. -/
def rewrite (k : JoinKind) (L R : List Col) (on p : Cond) (ls rs : Rel) : Rel :=
  match place k L R p with
  | .left => join k L on (filter p ls) rs
  | .right => join k L on ls (filter p rs)
  | .cond => join k L (.and on p) ls rs
  | .inner => innerJoin L (.and on p) ls rs
  | .above => filter p (join k L on ls rs)

theorem readsOnly_spec {p : Cond} {cols : List Col} (h : readsOnly p cols = true) :
    ∀ c ∈ p.reads, c ∈ cols := by
  simpa [readsOnly] using h

/-! ### Filters and the joined rows -/

/-- A conjunct over left columns sees the left row of a joined row. -/
theorem holds_merge_left {L : List Col} {p : Cond} (h : readsOnly p L = true) (x y : Row) :
    p.holds (Row.merge L x y) = p.holds x :=
  p.holds_congr fun c hc => merge_left x y (readsOnly_spec h c hc)

/-- A conjunct over right columns, which are not left columns, sees the right row. -/
theorem holds_merge_right {L R : List Col} {p : Cond} (disj : ∀ c ∈ R, c ∉ L)
    (h : readsOnly p R = true) (x y : Row) :
    p.holds (Row.merge L x y) = p.holds y :=
  p.holds_congr fun c hc => merge_right x y (disj c (readsOnly_spec h c hc))

/-- A filter over rows that all agree on it keeps all of them or none. -/
theorem filter_uniform (q : Row → Bool) (b : Bool) (zs : Rel) (hq : ∀ z ∈ zs, q z = b) :
    zs.filter q = if b then zs else [] := by
  induction zs with
  | nil => cases b <;> rfl
  | cons z zs ih =>
    have hz := hq z (by simp)
    have ih := ih (fun z' hz' => hq z' (by simp [hz']))
    cases b <;> simp_all

/-- The joined rows of one left row agree on a conjunct over left columns. -/
theorem filter_map_merge_left {L : List Col} {p : Cond} (h : readsOnly p L = true) (x : Row)
    (ys : Rel) :
    filter p (ys.map (Row.merge L x)) = if p.holds x then ys.map (Row.merge L x) else [] :=
  filter_uniform _ _ _ (by simp [holds_merge_left h])

/-- The joined rows of one right row agree on a conjunct over right columns. -/
theorem filter_map_merge_right {L R : List Col} {p : Cond} (disj : ∀ c ∈ R, c ∉ L)
    (h : readsOnly p R = true) (y : Row) (xs : Rel) :
    filter p (xs.map (fun x => Row.merge L x y)) =
      if p.holds y then xs.map (fun x => Row.merge L x y) else [] :=
  filter_uniform _ _ _ (by simp [holds_merge_right disj h])

theorem filter_flatMap (p : Cond) (f : Row → Rel) (xs : Rel) :
    filter p (xs.flatMap f) = xs.flatMap (fun x => filter p (f x)) := by
  unfold filter
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.flatMap_cons, List.filter_append, ih]

/-- The filtered rows of a list, flat-mapped: rows failing the filter contribute nothing. -/
theorem flatMap_filter (q : Row → Bool) (f : Row → Rel) (xs : Rel) :
    (xs.filter q).flatMap f = xs.flatMap (fun x => if q x then f x else []) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases hq : q x <;> simp [hq, ih]

/-! ### Soundness per join kind -/

theorem inner_cond (L : List Col) (on p : Cond) (ls rs : Rel) :
    innerJoin L (.and on p) ls rs = filter p (innerJoin L on ls rs) := by
  unfold innerJoin
  rw [filter_flatMap]
  congr 1
  funext x
  simp only [matchesOf, filter, List.filter_map, List.filter_filter, Cond.holds_and,
    Function.comp_def]
  congr 2
  funext y
  exact Bool.and_comm _ _

/-- Pushing into the left input of an inner join, which `ReorderJoins` may do with a
condition over one input, keeps the rows too. -/
theorem inner_left (L : List Col) (on p : Cond) (h : readsOnly p L = true) (ls rs : Rel) :
    innerJoin L on (filter p ls) rs = filter p (innerJoin L on ls rs) := by
  unfold innerJoin
  rw [filter_flatMap, filter, flatMap_filter]
  congr 1
  funext x
  unfold matchesOf
  rw [filter_map_merge_left h]

/-- One preserved row's output (its matches, or itself padded) against a conjunct that
agrees on all of them: all of it or none. -/
theorem filter_padded {p : Cond} {x pad : Row} (m : Rel) (hpad : p.holds pad = p.holds x)
    (hm : filter p m = if p.holds x then m else []) :
    (if p.holds x then (if m.isEmpty then [pad] else m) else []) =
      filter p (if m.isEmpty then [pad] else m) := by
  by_cases he : m.isEmpty <;> by_cases hp : p.holds x <;>
    simp only [he, hp, ite_true, ite_false, Bool.false_eq_true]
  · simp [filter, hpad, hp]
  · simp [filter, hpad, hp]
  · rw [hm]; simp [hp]
  · rw [hm]; simp [hp]

theorem left_left (L : List Col) (on p : Cond) (h : readsOnly p L = true) (ls rs : Rel) :
    leftJoin L on (filter p ls) rs = filter p (leftJoin L on ls rs) := by
  unfold leftJoin
  rw [filter_flatMap, filter, flatMap_filter]
  congr 1
  funext x
  exact filter_padded _ (holds_merge_left h x Row.null) (filter_map_merge_left h x _)

theorem right_right (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (on p : Cond)
    (h : readsOnly p R = true) (ls rs : Rel) :
    rightJoin L on ls (filter p rs) = filter p (rightJoin L on ls rs) := by
  unfold rightJoin
  rw [filter_flatMap, filter, flatMap_filter]
  congr 1
  funext y
  exact filter_padded _ (holds_merge_right disj h Row.null y) (filter_map_merge_right disj h y _)

/-- Under a conjunct that rejects the NULL-padded rows, a LEFT join returns the rows of the inner
join: each unmatched left row's padded row fails the conjunct. -/
theorem filter_left_inner (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (on p : Cond)
    (h : rejectsNull R p = true) (ls rs : Rel) :
    filter p (leftJoin L on ls rs) = filter p (innerJoin L on ls rs) := by
  unfold leftJoin innerJoin
  rw [filter_flatMap, filter_flatMap]
  congr 1
  funext x
  by_cases he : (matchesOf L on x rs).isEmpty
  · have hnil : matchesOf L on x rs = [] := List.isEmpty_iff.mp he
    simp [hnil, filter, rejectsNull_holds (nullOn_pad disj x) h]
  · simp [he]

theorem left_inner (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (on p : Cond)
    (h : rejectsNull R p = true) (ls rs : Rel) :
    innerJoin L (.and on p) ls rs = filter p (leftJoin L on ls rs) := by
  rw [filter_left_inner L R disj on p h, inner_cond]

/-- The same over an inner join tree that has the LEFT join as its left input (`from l left join
r on ..., t where q`): a condition `q` of the tree that rejects NULLs on the right columns of the
LEFT join drops every row built from a padded row, so the LEFT join can be an inner join. Its
columns `R` are among the tree's left columns `L2`. -/
theorem tree_left_inner (L R L2 : List Col) (disj : ∀ c ∈ R, c ∉ L) (sub : ∀ c ∈ R, c ∈ L2)
    (on q : Cond) (h : rejectsNull R q = true) (ls rs ts : Rel) :
    innerJoin L2 q (innerJoin L on ls rs) ts = innerJoin L2 q (leftJoin L on ls rs) ts := by
  unfold innerJoin leftJoin
  rw [List.flatMap_assoc, List.flatMap_assoc]
  congr 1
  funext x
  by_cases he : (matchesOf L on x rs).isEmpty
  · have hnil : matchesOf L on x rs = [] := List.isEmpty_iff.mp he
    have hz : NullOn R (Row.merge L x Row.null) := nullOn_pad disj x
    have hq : ∀ y, q.holds (Row.merge L2 (Row.merge L x Row.null) y) = false := fun y =>
      rejectsNull_holds (fun c hc => by rw [merge_left _ _ (sub c hc)]; exact hz c hc) h
    simp only [hnil]
    simp [matchesOf, hq]
  · simp only [Bool.not_eq_true] at he
    simp only [he, Bool.false_eq_true, ↓reduceIte]

/-- A semi or anti join outputs its left rows themselves, so any conjunct over its output moves
into the left input. -/
theorem semi_left (L : List Col) (on p : Cond) (ls rs : Rel) :
    semiJoin L on (filter p ls) rs = filter p (semiJoin L on ls rs) := by
  unfold semiJoin filter
  rw [List.filter_filter, List.filter_filter]
  congr 1
  funext x
  exact Bool.and_comm _ _

theorem anti_left (L : List Col) (on p : Cond) (ls rs : Rel) :
    antiJoin L on (filter p ls) rs = filter p (antiJoin L on ls rs) := by
  unfold antiJoin filter
  rw [List.filter_filter, List.filter_filter]
  congr 1
  funext x
  exact Bool.and_comm _ _

/-- Every placement of `place` keeps the rows of the filter over the join, for every pair of
inputs and every ON condition, NULLs included. The inputs' columns must be disjoint. -/
theorem rewrite_sound (k : JoinKind) (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (on p : Cond)
    (ls rs : Rel) : rewrite k L R on p ls rs = filter p (join k L on ls rs) := by
  cases k with
  | inner => exact inner_cond L on p ls rs
  | full => rfl
  | right =>
    by_cases h : readsOnly p R = true
    · simpa [rewrite, place, h, join] using right_right L R disj on p h ls rs
    · simp [rewrite, place, h]
  | left =>
    by_cases h : readsOnly p L = true
    · simpa [rewrite, place, h, join] using left_left L on p h ls rs
    · by_cases hr : rejectsNull R p = true
      · simpa [rewrite, place, h, hr, join] using left_inner L R disj on p hr ls rs
      · simp [rewrite, place, h, hr]
  | semi =>
    by_cases h : readsOnly p L = true
    · simpa [rewrite, place, h, join] using semi_left L on p ls rs
    · simp [rewrite, place, h]
  | anti =>
    by_cases h : readsOnly p L = true
    · simpa [rewrite, place, h, join] using anti_left L on p ls rs
    · simp [rewrite, place, h]

end Wvlet.Pushdown
