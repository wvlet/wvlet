/-
Licensed under the Apache License, Version 2.0.

One probe for an EXISTS and a NOT EXISTS over the same table (the engine's `SemiAntiProbe`,
`semi_anti_one_probe`). After decorrelation, TPC-H q21 is an anti join over a semi join, both of the
outer rows B with the same table T:

  `Anti(Semi(B, σq(T), on₁), σp(T), on₂)`

where `q` and `p` filter T's rows and read only T's columns. It keeps the rows b of B with a row t
of T such that `q(t) and on₁(b, t)` and with no row t of T such that `p(t) and on₂(b, t)`
(`fused`): one pass over T that marks b matched and excluded is enough (`anti_semi_eq`). The other
nesting, a semi join over an anti join, keeps the same rows (`semi_anti_eq`).

The conditions are three-valued: only TRUE matches or excludes, so a NULL `p` or `on₂` leaves the
row in, as a NOT EXISTS does. The engine's hash join evaluates `on₁` and `on₂` only on pairs with
equal keys, so it fuses only joins on the same keys, which both conditions then include; the
identity holds for any conditions.
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.SemiAntiProbe

open Wvlet.Rewrite (Value Tri)
open Wvlet.Algebra

/--
The fused join: the left rows with a right row satisfying `m` (the EXISTS), and with no right row
satisfying `e` (the NOT EXISTS), in their order and with their duplicates.
-/
def fused (L : List Col) (m e : Cond) (ls rs : Rel) : Rel :=
  ls.filter fun x =>
    (rs.any fun y => m.holds (Row.merge L x y)) && !(rs.any fun y => e.holds (Row.merge L x y))

/-- A filter of the right input becomes a conjunct of the join's condition. -/
theorem any_filter_right (L : List Col) (p on : Cond) (hp : ∀ c ∈ p.reads, c ∉ L) (x : Row)
    (rs : Rel) :
    ((filter p rs).any fun y => on.holds (Row.merge L x y)) =
      rs.any fun y => (Cond.and p on).holds (Row.merge L x y) := by
  unfold filter
  rw [List.any_filter]
  apply congrArg (fun f => rs.any f)
  funext y
  rw [Cond.holds_and]
  have : p.holds (Row.merge L x y) = p.holds y :=
    Cond.holds_congr p fun c hc => by simp [Row.merge, hp c hc]
  rw [this]

/-- An anti join over a semi join of the same right rows, filtered by `q` and `p`, is `fused`. -/
theorem anti_semi_eq (L : List Col) (q p on₁ on₂ : Cond) (hq : ∀ c ∈ q.reads, c ∉ L)
    (hp : ∀ c ∈ p.reads, c ∉ L) (ls rs : Rel) :
    antiJoin L on₂ (semiJoin L on₁ ls (filter q rs)) (filter p rs) =
      fused L (.and q on₁) (.and p on₂) ls rs := by
  unfold antiJoin semiJoin fused
  rw [List.filter_filter]
  apply List.filter_congr
  intro x _
  rw [any_filter_right L q on₁ hq, any_filter_right L p on₂ hp, Bool.and_comm]

/-- A semi join over an anti join of the same right rows keeps the same rows. -/
theorem semi_anti_eq (L : List Col) (q p on₁ on₂ : Cond) (hq : ∀ c ∈ q.reads, c ∉ L)
    (hp : ∀ c ∈ p.reads, c ∉ L) (ls rs : Rel) :
    semiJoin L on₁ (antiJoin L on₂ ls (filter p rs)) (filter q rs) =
      fused L (.and q on₁) (.and p on₂) ls rs := by
  unfold antiJoin semiJoin fused
  rw [List.filter_filter]
  apply List.filter_congr
  intro x _
  rw [any_filter_right L q on₁ hq, any_filter_right L p on₂ hp]

end Wvlet.SemiAntiProbe
