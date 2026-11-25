/-
Licensed under the Apache License, Version 2.0.

Self-join aggregation (the engine's `SelfJoinAggregate`, `self_join_aggregate`): a semi join of
rows of a table T with T itself on an equal key `k` and a different value `x`

  `l ⋉ T on l.k = T.k and l.x <> T.x`

where every left row agrees on (k, x) with some row of T (`Lineage`), keeps the left rows whose x
is not NULL and whose key's group of T holds two different non-NULL values of x (`multi`): the
groups of `T group by k having min(x) <> max(x)`, NULL keys left out (`having`). So the rule
replaces the join with a semi join on k alone over the aggregation (`semiJoin_eq`), and, where only
the distinct keys of the join matter and the left rows are all of T's (`Covers`), with the
aggregation's keys (`keys_eq`). The engine applies it only when the left side is a whole scan of T,
where the aggregation pays off; `semiJoin_eq` also covers a filtered left side.

Why it holds: a row with a NULL x never satisfies `<>`, and when the left row's x is not NULL its
own row of T is in its key's group, so some row of the group differs from it exactly when the group
holds two different values. That `min(x) <> max(x)` over the non-NULL values of a group says the
same (min and max skip NULLs, and are equal only when every value is) is not modeled here.
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.SelfJoinAggregate

open Wvlet.Rewrite (Value Tri)
open Wvlet.Algebra

/-- The group of key `v` (column `k`) of `rs` holds two different non-NULL values of column `x`. -/
def multi (k x : Col) (rs : Rel) (v : Value) : Bool :=
  rs.any fun a => rs.any fun b =>
    decide (a k = v ∧ b k = v ∧ a x ≠ .null ∧ b x ≠ .null ∧ a x ≠ b x)

/--
`T group by k having min(x) <> max(x)`, with NULL keys left out: a row of the group stands for its
key, since the consumers only ask whether a key occurs.
-/
def having (k x : Col) (rs : Rel) : Rel :=
  rs.filter fun a => decide (a k ≠ .null) && multi k x rs (a k)

/-- Every left row (columns `lk`, `lx`) agrees with some row of T (columns `k`, `x`). -/
def Lineage (lk lx k x : Col) (ls rs : Rel) : Prop :=
  ∀ z ∈ ls, ∃ w ∈ rs, w k = z lk ∧ w x = z lx

/-- Every row of T agrees with some left row: the left input reads all of T. -/
def Covers (lk lx k x : Col) (ls rs : Rel) : Prop :=
  ∀ w ∈ rs, ∃ z ∈ ls, z lk = w k ∧ z lx = w x

/-- The ON condition of the self join: `lk = k and not (lx = x)`. -/
def selfOn (lk lx k x : Col) : Cond := .and (.eq (.col lk) (.col k)) (.not (.eq (.col lx) (.col x)))

theorem and_eq_true (a b : Tri) : a.and b = .true ↔ a = .true ∧ b = .true := by
  cases a <;> cases b <;> decide

theorem not_eq_true (p : Tri) : p.not = .true ↔ p = .false := by
  cases p <;> simp [Tri.not]

theorem eqTri_eq_false {a b : Value} :
    eqTri a b = .false ↔ a ≠ .null ∧ b ≠ .null ∧ a ≠ b := by
  cases a <;> cases b <;> simp [eqTri] <;> split <;> simp_all

/-- The self join's condition over a joined row, by value. -/
theorem selfOn_holds (lk lx k x : Col) (hk : k ∉ [lk, lx]) (hx : x ∉ [lk, lx]) (z y : Row) :
    (selfOn lk lx k x).holds (Row.merge [lk, lx] z y) = true ↔
      (z lk ≠ .null ∧ z lk = y k) ∧ (z lx ≠ .null ∧ y x ≠ .null ∧ z lx ≠ y x) := by
  have mk : Row.merge [lk, lx] z y k = y k := by simp [Row.merge, hk]
  have mx : Row.merge [lk, lx] z y x = y x := by simp [Row.merge, hx]
  have ml : Row.merge [lk, lx] z y lk = z lk := by simp [Row.merge]
  have mv : Row.merge [lk, lx] z y lx = z lx := by simp [Row.merge]
  simp only [Cond.holds, selfOn, Cond.eval, Term.eval, mk, mx, ml, mv, beq_iff_eq,
    and_eq_true, not_eq_true, eqTri_eq_true, eqTri_eq_false]

/-- The key condition `lk = k` over a joined row, by value. -/
theorem keyOn_holds (lk lx k : Col) (hk : k ∉ [lk, lx]) (z y : Row) :
    (Cond.eq (.col lk) (.col k)).holds (Row.merge [lk, lx] z y) = true ↔
      z lk ≠ .null ∧ z lk = y k := by
  have mk : Row.merge [lk, lx] z y k = y k := by simp [Row.merge, hk]
  have ml : Row.merge [lk, lx] z y lk = z lk := by simp [Row.merge]
  simp only [Cond.holds, Cond.eval, Term.eval, mk, ml, beq_iff_eq, eqTri_eq_true]

theorem multi_iff (k x : Col) (rs : Rel) (v : Value) :
    multi k x rs v = true ↔
      ∃ a ∈ rs, ∃ b ∈ rs, a k = v ∧ b k = v ∧ a x ≠ .null ∧ b x ≠ .null ∧ a x ≠ b x := by
  simp [multi]

/-- A left row has a match in the self join exactly when it has one in the aggregation. -/
theorem match_iff (lk lx k x : Col) (hk : k ∉ [lk, lx]) (hx : x ∉ [lk, lx]) (rs : Rel) (z : Row)
    (hz : ∃ w ∈ rs, w k = z lk ∧ w x = z lx) :
    (rs.any fun y => (selfOn lk lx k x).holds (Row.merge [lk, lx] z y)) =
      ((Cond.not (.isNull (.col lx))).holds z &&
        (having k x rs).any fun y => (Cond.eq (.col lk) (.col k)).holds (Row.merge [lk, lx] z y)) := by
  have hnn : (Cond.not (.isNull (.col lx))).holds z = decide (z lx ≠ .null) := by
    by_cases h : z lx = .null <;> simp [Cond.holds, Cond.eval, Term.eval, h, Tri.not]
  rw [hnn]
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true, Bool.and_eq_true, decide_eq_true_eq, selfOn_holds lk lx k x hk hx,
    keyOn_holds lk lx k hk, having, List.mem_filter, multi_iff]
  constructor
  · rintro ⟨y, hy, ⟨hkn, hky⟩, hxn, hyx, hne⟩
    obtain ⟨w, hw, hwk, hwx⟩ := hz
    refine ⟨hxn, y, ⟨hy, ?_, w, hw, y, hy, ?_, rfl, ?_, hyx, ?_⟩, hkn, hky⟩
    · rw [← hky]; exact hkn
    · rw [hwk, hky]
    · rw [hwx]; exact hxn
    · rw [hwx]; exact hne
  · rintro ⟨hxn, a, ⟨ha, _, b, hb, c, hc, hbk, hck, hbx, hcx, hbc⟩, hkn, hka⟩
    by_cases h : z lx = b x
    · refine ⟨c, hc, ⟨hkn, ?_⟩, hxn, hcx, ?_⟩
      · rw [hka, hck]
      · rw [h]; exact hbc
    · refine ⟨b, hb, ⟨hkn, ?_⟩, hxn, hbx, h⟩
      rw [hka, hbk]

/--
The self semi join keeps the left rows with a non-NULL x whose key the aggregation holds, in their
order and with their duplicates.
-/
theorem semiJoin_eq (lk lx k x : Col) (hk : k ∉ [lk, lx]) (hx : x ∉ [lk, lx]) (ls rs : Rel)
    (h : Lineage lk lx k x ls rs) :
    semiJoin [lk, lx] (selfOn lk lx k x) ls rs =
      semiJoin [lk, lx] (.eq (.col lk) (.col k)) (filter (.not (.isNull (.col lx))) ls)
        (having k x rs) := by
  unfold semiJoin filter
  rw [List.filter_filter]
  apply List.filter_congr
  intro z hz
  rw [match_iff lk lx k x hk hx rs z (h z hz), Bool.and_comm]

/--
Where only the distinct keys of the self join matter and the left input reads all of T, they are
the keys of the aggregation.
-/
theorem keys_eq (lk lx k x : Col) (hk : k ∉ [lk, lx]) (hx : x ∉ [lk, lx]) (ls rs : Rel)
    (h : Lineage lk lx k x ls rs) (hc : Covers lk lx k x ls rs) (v : Value) :
    (∃ z ∈ semiJoin [lk, lx] (selfOn lk lx k x) ls rs, z lk = v) ↔
      ∃ a ∈ having k x rs, a k = v := by
  rw [semiJoin_eq lk lx k x hk hx ls rs h]
  simp only [semiJoin, filter, List.mem_filter, List.any_eq_true, keyOn_holds lk lx k hk]
  constructor
  · rintro ⟨z, ⟨_, a, ha, _, hza⟩, rfl⟩
    exact ⟨a, ha, hza.symm⟩
  · rintro ⟨a, ha, rfl⟩
    have ha' := ha
    simp only [having, List.mem_filter, Bool.and_eq_true, decide_eq_true_eq, multi_iff] at ha'
    obtain ⟨_, hkn, b, hb, _, _, hbk, _, hbx, _, _⟩ := ha'
    obtain ⟨z, hz, hzk, hzx⟩ := hc b hb
    have hzn : z lx ≠ .null := by rw [hzx]; exact hbx
    refine ⟨z, ⟨⟨hz, ?_⟩, a, ha, ?_, ?_⟩, ?_⟩
    · simp [Cond.holds, Cond.eval, Term.eval, hzn, Tri.not]
    · rw [hzk, hbk]; exact hkn
    · rw [hzk, hbk]
    · rw [hzk, hbk]

end Wvlet.SelfJoinAggregate
