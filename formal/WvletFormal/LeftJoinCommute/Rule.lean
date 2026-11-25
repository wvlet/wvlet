/-
Licensed under the Apache License, Version 2.0.

When `ReorderJoins` (wvlet-engine runtime, `commute_left_joins`) may move a LEFT join above the
inner joins of its preserved input: `(A ⟕ₚ B) ⋈_q C` has the rows of `(A ⋈_q C) ⟕ₚ B` when `q` reads
no column of `B`, so the inner joins can run first and shrink `A` before the LEFT join probes `B`.

The two sides list their rows in different orders (the left side pairs each `A` row with its `B`
matches first, the right side with its `C` matches), so the theorem is a permutation: the same bag.
Each input's rows are NULL outside its columns, as rows of a relation are; the joined rows then
agree on every column.

The engine moves the LEFT join above an inner join tree of several inputs and conditions. That
case is this one with `C` the inner join of the other inputs and `q` the conjunction of the tree's
conditions (inner joins being associative and commutative as bags).
-/
import WvletFormal.Algebra.Bag

namespace Wvlet.LeftJoinCommute

open Wvlet.Algebra
open Wvlet.Rewrite (Value)

/-- The rows of a relation are NULL outside its columns. -/
def NullOutside (cols : List Col) (r : Rel) : Prop := ∀ x ∈ r, ∀ c, c ∉ cols → x c = .null

/-- The condition reads only columns of `cols`. -/
def readsOnly (p : Cond) (cols : List Col) : Bool := p.reads.all (· ∈ cols)

/-- Whether `ReorderJoins` may move the LEFT join of `B` (columns `LB`) above an inner join on
`q`: `q` reads no column of `B`. -/
def commutes (LB : List Col) (q : Cond) : Bool := q.reads.all (· ∉ LB)

/-! ### Permutations of nested flat maps -/

theorem flatMap_cons_perm {α β : Type} (ys : List α) (g : α → β) (h : α → List β) :
    (ys.flatMap fun y => g y :: h y).Perm (ys.map g ++ ys.flatMap h) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
    simp only [List.flatMap_cons, List.map_cons, List.cons_append]
    apply List.Perm.cons
    refine (ih.append_left (h y)).trans ?_
    rw [← List.append_assoc, ← List.append_assoc]
    exact List.perm_append_comm.append_right _

/-- Pairing every `x` with every `y` gives the same bag in either loop order. -/
theorem flatMap_map_comm {α β γ : Type} (xs : List α) (ys : List β) (f : α → β → γ) :
    (xs.flatMap fun x => ys.map (f x)).Perm (ys.flatMap fun y => xs.map fun x => f x y) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    simp only [List.flatMap_cons, List.map_cons]
    exact ((ih.append_left _).trans (flatMap_cons_perm ys (f x) _).symm)

theorem flatMap_perm_congr {α β : Type} {xs : List α} {f g : α → List β}
    (h : ∀ x ∈ xs, (f x).Perm (g x)) : (xs.flatMap f).Perm (xs.flatMap g) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    simp only [List.flatMap_cons]
    exact (h x (by simp)).append (ih fun y hy => h y (by simp [hy]))

theorem flatMap_singleton_eq {α β : Type} (l : List α) (g : α → β) :
    (l.flatMap fun x => [g x]) = l.map g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

/-- A list's flat map over members, with each member's list changed equally. -/
theorem flatMap_congr_mem {α β : Type} {l : List α} {f g : α → List β}
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.flatMap_cons]
    rw [h x (by simp), ih fun y hy => h y (by simp [hy])]

/-! ### The commute rule -/

section
variable {LA LB LC : List Col}

/-- Joined rows read each column from the input that has it. -/
theorem merge_merge {a b c : Row}
    (dBC : ∀ x ∈ LB, x ∉ LC) (hb : ∀ x, x ∉ LB → b x = .null)
    (hc : ∀ x, x ∉ LC → c x = .null) :
    Row.merge (LA ++ LB) (Row.merge LA a b) c = Row.merge (LA ++ LC) (Row.merge LA a c) b := by
  funext x
  simp only [Row.merge, List.mem_append]
  by_cases ha : x ∈ LA
  · simp [ha]
  · by_cases hB : x ∈ LB
    · simp [ha, hB, dBC x hB]
    · by_cases hC : x ∈ LC
      · simp [ha, hB, hC]
      · simp [ha, hB, hC, hb x hB, hc x hC]

/-- An inner join over a LEFT join, on a condition `q` that reads no column of the LEFT join's
right input, has the rows of the LEFT join over the inner join. -/
theorem commute (dAB : ∀ x ∈ LB, x ∉ LA) (dAC : ∀ x ∈ LC, x ∉ LA) (dBC : ∀ x ∈ LB, x ∉ LC)
    (p q : Cond) (hp : readsOnly p (LA ++ LB) = true) (hq : readsOnly q (LA ++ LC) = true)
    (as bs cs : Rel) (nb : NullOutside LB bs) (nc : NullOutside LC cs) :
    (innerJoin (LA ++ LB) q (leftJoin LA p as bs) cs).Perm
      (leftJoin (LA ++ LC) p (innerJoin LA q as cs) bs) := by
  have hp' : ∀ c ∈ p.reads, c ∈ LA ∨ c ∈ LB := fun c hc => by
    simpa [readsOnly] using (List.all_eq_true.mp hp) c hc
  have hq' : ∀ c ∈ q.reads, c ∈ LA ∨ c ∈ LC := fun c hc => by
    simpa [readsOnly] using (List.all_eq_true.mp hq) c hc
  -- `q` on a row built from `a` sees `a` and the `C` row
  have qa : ∀ a b c : Row, q.holds (Row.merge (LA ++ LB) (Row.merge LA a b) c) =
      q.holds (Row.merge LA a c) := fun a b c => q.holds_congr fun x hx => by
    rcases hq' x hx with h | h
    · simp [Row.merge, h]
    · have hnb : x ∉ LB := fun hB => dBC x hB h
      simp [Row.merge, hnb, dAC x h]
  -- `p` on a row built from `a` sees `a` and the `B` row
  have pa : ∀ a c b : Row, p.holds (Row.merge (LA ++ LC) (Row.merge LA a c) b) =
      p.holds (Row.merge LA a b) := fun a c b => p.holds_congr fun x hx => by
    rcases hp' x hx with h | h
    · simp [Row.merge, h]
    · simp [Row.merge, dAB x h, dBC x h]
  unfold innerJoin leftJoin
  rw [List.flatMap_assoc, List.flatMap_assoc]
  apply flatMap_perm_congr
  intro a _
  simp only [matchesOf, List.flatMap_map]
  have hbs : ∀ c, (bs.filter fun b => p.holds (Row.merge (LA ++ LC) (Row.merge LA a c) b)) =
      (bs.filter fun b => p.holds (Row.merge LA a b)) := fun c =>
    List.filter_congr fun b _ => pa a c b
  simp only [hbs, List.isEmpty_map]
  have memB : ∀ b ∈ (bs.filter fun b => p.holds (Row.merge LA a b)), b ∈ bs := fun b hb =>
    (List.mem_filter.mp hb).1
  have memC : ∀ c ∈ (cs.filter fun c => q.holds (Row.merge LA a c)), c ∈ cs := fun c hc =>
    (List.mem_filter.mp hc).1
  generalize (bs.filter fun b => p.holds (Row.merge LA a b)) = mb at memB ⊢
  cases mb with
  | nil =>
    simp only [List.isEmpty_nil, ↓reduceIte, List.flatMap_cons, List.flatMap_nil,
      List.append_nil, flatMap_singleton_eq, qa]
    generalize (cs.filter fun c => q.holds (Row.merge LA a c)) = mc at memC ⊢
    rw [List.map_congr_left fun c hc =>
      merge_merge (b := Row.null) dBC (fun _ _ => rfl) (nc c (memC c hc))]
  | cons b0 bs0 =>
    simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte, List.flatMap_map, qa]
    generalize (cs.filter fun c => q.holds (Row.merge LA a c)) = mc at memC ⊢
    refine (flatMap_map_comm (b0 :: bs0) mc fun b c => Row.merge (LA ++ LB) (Row.merge LA a b) c).trans ?_
    rw [flatMap_congr_mem fun c hc => List.map_congr_left fun b hb =>
      merge_merge dBC (nb b (memB b hb)) (nc c (memC c hc))]

end

end Wvlet.LeftJoinCommute
