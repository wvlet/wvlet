/-
Licensed under the Apache License, Version 2.0.

Semi-join reduction (the engine's `ReduceSemiJoins`, `reduce_semi_joins`): where only the set of
rows matters, an inner join whose needed columns all come from its left input becomes a semi join
of that input with the right one, on the same ON condition.

The rows of `X ⋈ Y` projected to columns `K` of X are, as a set, the rows of `X ⋉ Y` projected to
`K` (`innerJoin_proj_iff_semiJoin`): a pair whose condition is FALSE or unknown (NULL) is dropped by
both. So a consumer that reads only `K` and asks only whether a row occurs gives the same result
over either input: a semi join (`IN`, `EXISTS`), an anti join (`NOT EXISTS`), and `NOT IN`, whose
NULL handling reads only the set of values (`notIn_reduce`).
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.SemiReduce

open Wvlet.Rewrite (Value Tri)
open Wvlet.Algebra

/-- Some row of `r` agrees with `z` on the columns `K`: `z`'s projection to `K` is in `r`'s. -/
def projHas (K : List Col) (r : Rel) (z : Row) : Prop := ∃ w ∈ r, ∀ c ∈ K, w c = z c

/-- The projection of an inner join to left columns is the projection of the semi join. -/
theorem innerJoin_proj_iff_semiJoin (L K : List Col) (hK : ∀ c ∈ K, c ∈ L) (on : Cond)
    (ls rs : Rel) (z : Row) :
    projHas K (innerJoin L on ls rs) z ↔ projHas K (semiJoin L on ls rs) z := by
  unfold projHas innerJoin semiJoin matchesOf
  constructor
  · rintro ⟨w, hw, hwz⟩
    simp only [List.mem_flatMap, List.mem_map, List.mem_filter] at hw
    obtain ⟨x, hx, y, ⟨hy, hon⟩, rfl⟩ := hw
    refine ⟨x, ?_, fun c hc => ?_⟩
    · simp only [List.mem_filter, List.any_eq_true]
      exact ⟨hx, y, hy, hon⟩
    · rw [← hwz c hc, merge_left x y (hK c hc)]
  · rintro ⟨x, hx, hxz⟩
    simp only [List.mem_filter, List.any_eq_true] at hx
    obtain ⟨hx, y, hy, hon⟩ := hx
    refine ⟨Row.merge L x y, ?_, fun c hc => ?_⟩
    · simp only [List.mem_flatMap, List.mem_map, List.mem_filter]
      exact ⟨x, hx, y, ⟨hy, hon⟩, rfl⟩
    · rw [merge_left x y (hK c hc), hxz c hc]

/-- A test that reads only the columns `K` holds for some row of either input alike. -/
theorem any_reduce (L K : List Col) (hK : ∀ c ∈ K, c ∈ L) (on : Cond) (ls rs : Rel)
    (p : Row → Bool) (hp : ∀ w x : Row, (∀ c ∈ K, w c = x c) → p w = p x) :
    (innerJoin L on ls rs).any p = (semiJoin L on ls rs).any p := by
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true]
  constructor
  · rintro ⟨w, hw, hpw⟩
    obtain ⟨x, hx, hxw⟩ :=
      (innerJoin_proj_iff_semiJoin L K hK on ls rs w).mp ⟨w, hw, fun _ _ => rfl⟩
    exact ⟨x, hx, by rw [hp x w hxw]; exact hpw⟩
  · rintro ⟨x, hx, hpx⟩
    obtain ⟨w, hw, hwx⟩ :=
      (innerJoin_proj_iff_semiJoin L K hK on ls rs x).mpr ⟨x, hx, fun _ _ => rfl⟩
    exact ⟨w, hw, by rw [hp w x hwx]; exact hpx⟩

/-- A consumer's condition over its own columns `L'` and the reduced input's columns `K`. -/
private theorem holds_reduce (L' K : List Col) (on' : Cond)
    (hon : ∀ c ∈ on'.reads, c ∈ L' ∨ c ∈ K) (o w x : Row) (hwx : ∀ c ∈ K, w c = x c) :
    on'.holds (Row.merge L' o w) = on'.holds (Row.merge L' o x) := by
  apply Cond.holds_congr
  intro c hc
  by_cases h : c ∈ L'
  · simp [Row.merge, h]
  · have hk : c ∈ K := (hon c hc).resolve_left h
    simp [Row.merge, h, hwx c hk]

/-- A semi join (`IN`, `EXISTS`) over the reduced input keeps the same rows. -/
theorem semiJoin_reduce (L K L' : List Col) (hK : ∀ c ∈ K, c ∈ L) (on on' : Cond)
    (hon : ∀ c ∈ on'.reads, c ∈ L' ∨ c ∈ K) (os ls rs : Rel) :
    semiJoin L' on' os (innerJoin L on ls rs) = semiJoin L' on' os (semiJoin L on ls rs) := by
  unfold semiJoin
  congr 1
  funext o
  exact any_reduce L K hK on ls rs _ (holds_reduce L' K on' hon o)

/-- An anti join (`NOT EXISTS`) over the reduced input keeps the same rows. -/
theorem antiJoin_reduce (L K L' : List Col) (hK : ∀ c ∈ K, c ∈ L) (on on' : Cond)
    (hon : ∀ c ∈ on'.reads, c ∈ L' ∨ c ∈ K) (os ls rs : Rel) :
    antiJoin L' on' os (innerJoin L on ls rs) = antiJoin L' on' os (semiJoin L on ls rs) := by
  unfold antiJoin
  congr 1
  funext o
  rw [any_reduce L K hK on ls rs _ (holds_reduce L' K on' hon o)]

/-- IN over values depends only on which values occur. -/
theorem inTri_congr (v : Value) (vs ws : List Value) (h : ∀ y, y ∈ vs ↔ y ∈ ws) :
    inTri v vs = inTri v ws := by
  have ht : inTri v vs = .true ↔ inTri v ws = .true := by
    rw [inTri_eq_true, inTri_eq_true]
    exact ⟨fun ⟨y, hy, e⟩ => ⟨y, (h y).mp hy, e⟩, fun ⟨y, hy, e⟩ => ⟨y, (h y).mpr hy, e⟩⟩
  have hf : inTri v vs = .false ↔ inTri v ws = .false := by
    rw [inTri_eq_false, inTri_eq_false]
    exact ⟨fun a y hy => a y ((h y).mpr hy), fun a y hy => a y ((h y).mp hy)⟩
  cases hv : inTri v vs <;> cases hw : inTri v ws <;> simp_all

/-- `NOT IN` over column `c` of the reduced input keeps the same rows, NULLs included. -/
theorem notIn_reduce (L K : List Col) (hK : ∀ c ∈ K, c ∈ L) (c : Col) (hc : c ∈ K) (on : Cond)
    (t : Term) (os ls rs : Rel) :
    notIn t c os (innerJoin L on ls rs) = notIn t c os (semiJoin L on ls rs) := by
  unfold notIn
  congr 1
  funext o
  rw [inTri_congr (t.eval o) _ _ fun v => ?_]
  simp only [List.mem_map]
  constructor
  · rintro ⟨w, hw, rfl⟩
    obtain ⟨x, hx, hxw⟩ :=
      (innerJoin_proj_iff_semiJoin L K hK on ls rs w).mp ⟨w, hw, fun _ _ => rfl⟩
    exact ⟨x, hx, hxw c hc⟩
  · rintro ⟨x, hx, rfl⟩
    obtain ⟨w, hw, hwx⟩ :=
      (innerJoin_proj_iff_semiJoin L K hK on ls rs x).mpr ⟨x, hx, fun _ _ => rfl⟩
    exact ⟨w, hw, hwx c hc⟩

end Wvlet.SemiReduce
