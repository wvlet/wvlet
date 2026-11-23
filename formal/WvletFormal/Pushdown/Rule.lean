/-
Licensed under the Apache License, Version 2.0.

Where `PushDownFilters` (wvlet-engine runtime) moves a WHERE conjunct that sits directly over a
join, and the proofs that every move keeps the rows.

The pass moves a conjunct into the preserved input of a LEFT or RIGHT join when it reads only that
input, into the left input of a semi or anti join (whose output has only left columns), and into
the condition list of an inner join tree, which `ReorderJoins` places later. A conjunct that reads
no column reads "only" any input, so it moves too. Over a FULL join, and over the null-supplying
input of an outer join, it stays above: there, NULL-padded rows can satisfy it (`c is null`) or a
removed match can turn into a padded row, so moving it changes the result (`Necessity.lean`).
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.Pushdown

open Wvlet.Algebra

/-- Where a conjunct goes. -/
inductive Place where
  /-- A filter over the left input. -/
  | left
  /-- A filter over the right input. -/
  | right
  /-- One more condition of the inner join tree. -/
  | cond
  /-- It stays in a filter above the join. -/
  | above
  deriving DecidableEq, Repr

/-- The conjunct reads only columns of `cols`. -/
def readsOnly (p : Cond) (cols : List Col) : Bool := p.reads.all (· ∈ cols)

/-- `PushDownFilters`' choice for a conjunct `p` over a join of kind `k` whose inputs have the
columns `L` and `R`. -/
def place (k : JoinKind) (L R : List Col) (p : Cond) : Place :=
  match k with
  | .inner => .cond
  | .left | .semi | .anti => if readsOnly p L then .left else .above
  | .right => if readsOnly p R then .right else .above
  | .full => .above

/-- The rows of the plan the pass builds for `filter p (join k L on ls rs)`. -/
def rewrite (k : JoinKind) (L R : List Col) (on p : Cond) (ls rs : Rel) : Rel :=
  match place k L R p with
  | .left => join k L on (filter p ls) rs
  | .right => join k L on ls (filter p rs)
  | .cond => join k L (.and on p) ls rs
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
    · simp [rewrite, place, h]
  | semi =>
    by_cases h : readsOnly p L = true
    · simpa [rewrite, place, h, join] using semi_left L on p ls rs
    · simp [rewrite, place, h]
  | anti =>
    by_cases h : readsOnly p L = true
    · simpa [rewrite, place, h, join] using anti_left L on p ls rs
    · simp [rewrite, place, h]

end Wvlet.Pushdown
