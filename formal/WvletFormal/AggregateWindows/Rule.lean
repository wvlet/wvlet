/-
Licensed under the Apache License, Version 2.0.

Aggregate windows (the engine's `AggregateWindows`, `aggregate_windows`): a join of rows O with a
per-key aggregate of the aggregation input I, read back on the key, becomes a window over O
partitioned by the key, when O is I joined with other rows E on `g = e` alone (`joined`), for I's
group key g and a column e of E:

  `O ⋈ (I group by g agg f(x)) on O.g = g`  ⇒  `O window f(x) over (partition by g)`

O's rows of a non-NULL key v, read at x, are I's rows of v, each repeated once per row of E of key
v, in I's order (`part_joined`). So every value occurs in O's partition exactly `mult` times as
often as in I's group (`total_scales`), and, the key having a row of E, the same values occur
(`mem_iff`): a min or max over the partition is the aggregate over the group, and so is an avg,
whose numerator and denominator both scale by `mult` (`avg_eq`). A sum or a count scales, so the
rule leaves them. Every row of O has a non-NULL key that I's group holds (`covered`), so the join,
inner or left, matches each row of O with exactly its group.

The engine also matches O and I up to column ids, checks that I's conditions are O's and that O's
other conditions read only E or equate g with E, and lets a semi join filter I by a relation that
covers O's keys ([[GroupKeySemiJoins]]); those plan checks are not modeled here.
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.AggregateWindows

open Wvlet.Rewrite (Value Tri)
open Wvlet.Algebra

/-- The rows of `rs` whose column k holds v. -/
def part (k : Col) (v : Value) (rs : Rel) : Rel := rs.filter fun a => decide (a k = v)

/-- How many rows of E hold v in column e. -/
def mult (e : Col) (v : Value) (es : Rel) : Nat := es.countP fun b => decide (b e = v)

/-- O: the rows of I (columns `L`) joined with the rows of E on `g = e`. -/
def joined (L : List Col) (g e : Col) (is es : Rel) : Rel :=
  innerJoin L (.eq (.col g) (.col e)) is es

/-- The key condition `g = e` over a joined row, by value. -/
theorem key_holds (L : List Col) (g e : Col) (hg : g ∈ L) (he : e ∉ L) (a b : Row) :
    (Cond.eq (.col g) (.col e)).holds (Row.merge L a b) = true ↔ a g ≠ .null ∧ a g = b e := by
  have mg : Row.merge L a b g = a g := by simp [Row.merge, hg]
  have me : Row.merge L a b e = b e := by simp [Row.merge, he]
  simp only [Cond.holds, Cond.eval, Term.eval, mg, me, beq_iff_eq, eqTri_eq_true]

/-- The matches of one row of I, read at x, in the partition of key v. -/
theorem matches_part (L : List Col) (g e x : Col) (hg : g ∈ L) (he : e ∉ L) (hx : x ∈ L)
    (v : Value) (hv : v ≠ .null) (a : Row) (es : Rel) :
    (part g v (matchesOf L (.eq (.col g) (.col e)) a es)).map (· x) =
      if a g = v then List.replicate (mult e v es) (a x) else [] := by
  induction es with
  | nil => by_cases h : a g = v <;> simp [part, matchesOf, mult, h]
  | cons b bs ih =>
    have mg : Row.merge L a b g = a g := by simp [Row.merge, hg]
    have mx : Row.merge L a b x = a x := by simp [Row.merge, hx]
    simp only [part, matchesOf, mult] at ih ⊢
    rw [List.filter_cons, List.countP_cons]
    by_cases hon : (Cond.eq (.col g) (.col e)).holds (Row.merge L a b) = true
    · obtain ⟨hn, hab⟩ := (key_holds L g e hg he a b).mp hon
      simp only [hon, ite_true, List.map_cons, List.filter_cons, mg]
      by_cases h : a g = v
      · have hb : b e = v := by rw [← hab, h]
        simp only [h, decide_true, ite_true, List.map_cons, mx, hb] at ih ⊢
        rw [ih]
        simp [List.replicate_succ]
      · have hb : ¬ b e = v := by rw [← hab]; exact h
        simp only [h, decide_false, Bool.false_eq_true, ite_false] at ih ⊢
        exact ih
    · have hb : ¬ (a g = v ∧ b e = v) := by
        rintro ⟨h1, h2⟩
        exact hon ((key_holds L g e hg he a b).mpr ⟨by rw [h1]; exact hv, by rw [h1, h2]⟩)
      simp only [hon, Bool.false_eq_true, ite_false]
      by_cases h : a g = v
      · have hb' : ¬ b e = v := fun h2 => hb ⟨h, h2⟩
        simp only [h, ite_true, hb', decide_false, Bool.false_eq_true, ite_false, Nat.add_zero]
          at ih ⊢
        exact ih
      · simp only [h, ite_false] at ih ⊢
        exact ih

/--
O's partition of a non-NULL key v, read at x, is I's group of v with each row repeated once per row
of E of key v, in I's order.
-/
theorem part_joined (L : List Col) (g e x : Col) (hg : g ∈ L) (he : e ∉ L) (hx : x ∈ L)
    (v : Value) (hv : v ≠ .null) (is es : Rel) :
    (part g v (joined L g e is es)).map (· x) =
      (part g v is).flatMap fun a => List.replicate (mult e v es) (a x) := by
  induction is with
  | nil => simp [part, joined, innerJoin]
  | cons a as ih =>
    have hsplit : part g v (joined L g e (a :: as) es) =
        part g v (matchesOf L (.eq (.col g) (.col e)) a es) ++ part g v (joined L g e as es) := by
      simp [part, joined, innerJoin, List.filter_append]
    rw [hsplit, List.map_append, ih, matches_part L g e x hg he hx v hv a es]
    by_cases h : a g = v <;> simp [part, h]

/-- The total of `f` over a list of values. -/
def total (f : Value → Int) (vs : List Value) : Int := (vs.map f).sum

theorem total_replicate (f : Value → Int) (m : Nat) (y : Value) :
    total f (List.replicate m y) = m * f y := by
  induction m with
  | zero => simp [total]
  | succ n ih =>
    simp only [total, List.replicate_succ, List.map_cons, List.sum_cons] at ih ⊢
    rw [ih, Int.natCast_succ, Int.add_mul, Int.one_mul, Int.add_comm]

theorem total_flatMap (f : Value → Int) (m : Nat) (rs : Rel) (x : Col) :
    total f (rs.flatMap fun a => List.replicate m (a x)) = m * total f (rs.map (· x)) := by
  induction rs with
  | nil => simp [total]
  | cons a as ih =>
    have h1 : total f ((a :: as).flatMap fun a => List.replicate m (a x)) =
        total f (List.replicate m (a x)) + total f (as.flatMap fun a => List.replicate m (a x)) := by
      simp [total, List.flatMap_cons, List.map_append, List.sum_append]
    have h2 : total f ((a :: as).map (· x)) = f (a x) + total f (as.map (· x)) := by
      simp [total]
    rw [h1, h2, ih, total_replicate, Int.mul_add]

/--
Every total over O's partition of a non-NULL key is `mult` times the total over I's group: so is
every value's count, a sum, and the count of non-NULL values.
-/
theorem total_scales (L : List Col) (g e x : Col) (hg : g ∈ L) (he : e ∉ L) (hx : x ∈ L)
    (v : Value) (hv : v ≠ .null) (is es : Rel) (f : Value → Int) :
    total f ((part g v (joined L g e is es)).map (· x)) =
      mult e v es * total f ((part g v is).map (· x)) := by
  rw [part_joined L g e x hg he hx v hv, total_flatMap]

/--
An avg, the total of `num` over the total of `den` (the values and the non-NULL indicator), is the
same over O's partition and over I's group: the cross products are equal.
-/
theorem avg_eq (L : List Col) (g e x : Col) (hg : g ∈ L) (he : e ∉ L) (hx : x ∈ L)
    (v : Value) (hv : v ≠ .null) (is es : Rel) (num den : Value → Int) :
    total num ((part g v (joined L g e is es)).map (· x)) * total den ((part g v is).map (· x)) =
      total num ((part g v is).map (· x)) * total den ((part g v (joined L g e is es)).map (· x)) := by
  rw [total_scales L g e x hg he hx v hv is es num, total_scales L g e x hg he hx v hv is es den,
    Int.mul_assoc, Int.mul_comm (mult e v es : Int), Int.mul_assoc, Int.mul_comm (mult e v es : Int)]

/-- When E holds the key, O's partition and I's group hold the same values: a min or a max agree. -/
theorem mem_iff (L : List Col) (g e x : Col) (hg : g ∈ L) (he : e ∉ L) (hx : x ∈ L)
    (v : Value) (hv : v ≠ .null) (is es : Rel) (hm : 0 < mult e v es) (y : Value) :
    y ∈ (part g v (joined L g e is es)).map (· x) ↔ y ∈ (part g v is).map (· x) := by
  rw [part_joined L g e x hg he hx v hv]
  simp only [List.mem_flatMap, List.mem_replicate, List.mem_map]
  constructor
  · rintro ⟨a, ha, _, rfl⟩
    exact ⟨a, ha, rfl⟩
  · rintro ⟨a, ha, rfl⟩
    exact ⟨a, ha, Nat.pos_iff_ne_zero.mp hm, rfl⟩

/--
Every row of O has a non-NULL key held by a row of I and by a row of E: I's aggregation has its
group, and the partition of its key holds the values of that group (`mem_iff`).
-/
theorem covered (L : List Col) (g e : Col) (hg : g ∈ L) (he : e ∉ L) (is es : Rel) (z : Row)
    (hz : z ∈ joined L g e is es) :
    z g ≠ .null ∧ (∃ a ∈ is, a g = z g) ∧ 0 < mult e (z g) es := by
  simp only [joined, innerJoin, matchesOf, List.mem_flatMap, List.mem_map, List.mem_filter] at hz
  obtain ⟨a, ha, b, ⟨hb, hon⟩, rfl⟩ := hz
  obtain ⟨hn, hab⟩ := (key_holds L g e hg he a b).mp hon
  have mg : Row.merge L a b g = a g := by simp [Row.merge, hg]
  rw [mg]
  refine ⟨hn, ⟨a, ha, rfl⟩, ?_⟩
  exact List.countP_pos_iff.mpr ⟨b, hb, by simp [hab]⟩

end Wvlet.AggregateWindows
