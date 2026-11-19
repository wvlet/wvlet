/-
Licensed under the Apache License, Version 2.0.

Soundness of SELECT-block folding: evaluating the blocks that `fold` builds gives exactly the
rows of running the pipeline step by step (`fold_sound`).
-/
import WvletFormal.SqlFold.Model

namespace Wvlet.SqlFold

variable {R : Type}

/-! ## List lemmas: filters commute with sorting and duplicate elimination -/

theorem mem_insertBy (k : R → Nat) (x z : R) (l : List R) :
    z ∈ insertBy k x l ↔ z = x ∨ z ∈ l := by
  induction l with
  | nil => simp [insertBy]
  | cons y ys ih =>
    unfold insertBy
    split <;> simp_all [or_left_comm]

/-- Sorted by `k`: every row's key is at most the keys after it. -/
def Sorted (k : R → Nat) : List R → Prop
  | [] => True
  | y :: ys => (∀ z ∈ ys, k y ≤ k z) ∧ Sorted k ys

theorem insertBy_of_le (k : R → Nat) (x : R) :
    ∀ l : List R, (∀ z ∈ l, k x ≤ k z) → insertBy k x l = x :: l
  | [], _ => rfl
  | y :: ys, h => by simp [insertBy, h y (by simp)]

theorem sorted_insertBy (k : R → Nat) (x : R) :
    ∀ l : List R, Sorted k l → Sorted k (insertBy k x l)
  | [], _ => by simp [insertBy, Sorted]
  | y :: ys, ⟨hy, hs⟩ => by
    unfold insertBy
    split
    · rename_i hxy
      refine ⟨?_, hy, hs⟩
      intro z hz
      simp at hz
      rcases hz with rfl | hz
      · exact hxy
      · exact Nat.le_trans hxy (hy z hz)
    · rename_i hxy
      refine ⟨?_, sorted_insertBy k x ys hs⟩
      intro z hz
      rcases (mem_insertBy k x z ys).1 hz with rfl | hz
      · exact Nat.le_of_lt (Nat.lt_of_not_le hxy)
      · exact hy z hz

theorem sorted_sortBy (k : R → Nat) : ∀ l : List R, Sorted k (sortBy k l)
  | [] => trivial
  | x :: xs => sorted_insertBy k x _ (sorted_sortBy k xs)

theorem filter_insertBy (k : R → Nat) (p : R → Bool) (x : R) :
    ∀ l : List R, Sorted k l →
      (insertBy k x l).filter p = if p x then insertBy k x (l.filter p) else l.filter p
  | [], _ => by cases h : p x <;> simp [insertBy, h]
  | y :: ys, ⟨hy, hs⟩ => by
    by_cases hxy : k x ≤ k y
    · have hle : ∀ z ∈ ys.filter p, k x ≤ k z := fun z hz =>
        Nat.le_trans hxy (hy z (List.mem_filter.1 hz).1)
      simp only [insertBy, hxy, ↓reduceIte]
      cases hpx : p x <;> cases hpy : p y <;>
        simp [hpx, hpy, insertBy, hxy, insertBy_of_le k x _ hle]
    · have ih := filter_insertBy k p x ys hs
      simp only [insertBy, hxy, ↓reduceIte]
      cases hpx : p x <;> cases hpy : p y <;>
        simp [hpx, hpy, insertBy, hxy, ih]

/-- Sorting commutes with a filter. -/
theorem filter_sortBy (k : R → Nat) (p : R → Bool) :
    ∀ l : List R, (sortBy k l).filter p = sortBy k (l.filter p)
  | [] => rfl
  | x :: xs => by
    rw [sortBy, filter_insertBy k p x _ (sorted_sortBy k xs), filter_sortBy k p xs]
    cases h : p x <;> simp [h, sortBy]

theorem map_insertBy (k : R → Nat) (f : R → R) (x : R) :
    ∀ l : List R, (insertBy (fun r => k (f r)) x l).map f = insertBy k (f x) (l.map f)
  | [] => rfl
  | y :: ys => by
    by_cases h : k (f x) ≤ k (f y)
    · simp [insertBy, h]
    · simp [insertBy, h, map_insertBy k f x ys]

/-- Sorting by a key that the projection preserves commutes with the projection. -/
theorem sortBy_map (k : R → Nat) (f : R → R) (hk : ∀ r, k (f r) = k r) (l : List R) :
    sortBy k (l.map f) = (sortBy k l).map f := by
  have hfun : (fun r => k (f r)) = k := funext hk
  induction l with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.map_cons, sortBy, ih]
    rw [← map_insertBy k f x, hfun]

/-! ## Block helpers -/

theorem allWhere_map (wh : List ((R → Bool) × Bool)) (f : R → R)
    (h : ∀ q ∈ wh, ∀ r, q.1 (f r) = q.1 r) (r : R) : allWhere wh (f r) = allWhere wh r := by
  induction wh with
  | nil => rfl
  | cons q qs ih =>
    simp only [allWhere]
    rw [h q (by simp), ih (fun q' hq' => h q' (by simp [hq']))]

theorem safe_of_allSafe (wh : List ((R → Bool) × Bool)) (h : allSafe wh = true) :
    ∀ q ∈ wh, q.2 = true := by
  induction wh with
  | nil => simp
  | cons q qs ih =>
    simp only [allSafe, Bool.and_eq_true] at h
    intro q' hq'
    simp at hq'
    rcases hq' with rfl | hq'
    · exact h.1
    · exact ih h.2 q' hq'

theorem filter_true' (l : List R) : l.filter (fun _ => true) = l := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [List.filter, ih]

theorem empty_wh {b : Block R} (h : (!b.wh.isEmpty) = false) : b.wh = [] := by
  cases hw : b.wh <;> simp_all

theorem empty_hav {b : Block R} (h : (!b.hav.isEmpty) = false) : b.hav = [] := by
  cases hw : b.hav <;> simp_all

theorem none_of_isSome {α : Type} {o : Option α} (h : o.isSome = false) : o = none := by
  cases o <;> simp_all

theorem allWhere_nil : (allWhere ([] : List ((R → Bool) × Bool))) = fun _ => true := rfl
theorem allHaving_nil : (allHaving ([] : List (R → Bool))) = fun _ => true := rfl

variable [DecidableEq R]


theorem dedupFrom_congr :
    ∀ (l s s' : List R), (∀ z ∈ l, z ∈ s ↔ z ∈ s') → dedupFrom s l = dedupFrom s' l
  | [], _, _, _ => rfl
  | x :: xs, s, s', h => by
    have hx := h x (by simp)
    have hxs : ∀ z ∈ xs, z ∈ s ↔ z ∈ s' := fun z hz => h z (by simp [hz])
    by_cases hs : x ∈ s
    · have hs' : x ∈ s' := hx.1 hs
      simp [dedupFrom, hs, hs', dedupFrom_congr xs s s' hxs]
    · have hs' : x ∉ s' := fun h' => hs (hx.2 h')
      simp only [dedupFrom, hs, hs', ite_false]
      congr 1
      apply dedupFrom_congr
      intro z hz
      simp [hxs z hz]

/-- Duplicate elimination commutes with a filter. -/
theorem filter_dedupFrom (p : R → Bool) :
    ∀ (l s : List R), (dedupFrom s l).filter p = dedupFrom s (l.filter p)
  | [], _ => rfl
  | x :: xs, s => by
    by_cases hs : x ∈ s
    · cases hp : p x <;> simp [dedupFrom, hs, hp, filter_dedupFrom p xs s]
    · cases hp : p x
      · -- `x` fails `p`, so no row of `xs.filter p` equals it and `x` in `seen` changes nothing
        simp only [dedupFrom, hs, ite_false, List.filter_cons, hp, filter_dedupFrom p xs]
        apply dedupFrom_congr
        intro z hz
        have hz' : p z = true := (List.mem_filter.1 hz).2
        constructor
        · intro hzm
          simp at hzm
          rcases hzm with rfl | hzm
          · simp [hp] at hz'
          · exact hzm
        · intro hzm; simp [hzm]
      · simp [dedupFrom, hs, hp, filter_dedupFrom p xs]

theorem filter_dedup (p : R → Bool) (l : List R) : (dedup l).filter p = dedup (l.filter p) :=
  filter_dedupFrom p l []

/-! ## One merge -/

/-- The core lemma: an accepted operator's clause in the block computes the operator. -/
theorem merge_sound (b : Block R) (op : Op R) (ha : accepts b.shape op.kind = true)
    (hs : b.scopeOk op) (xs : List R) : (b.add op).eval xs = b.eval (op.step xs) := by
  cases b with
  | mk wh grp aggSel hav sel dist ord off lim =>
  cases op with
  | filter p s =>
    have hf : xs.filter (allWhere ((p, s) :: wh)) = (xs.filter p).filter (allWhere wh) := by
      rw [List.filter_filter]
      congr 1
      funext r
      simp only [allWhere, Bool.and_comm]
    simp only [Block.add, Block.eval, Block.select, Block.finish, Op.step, hf]
  | having p =>
    simp only [accepts, Block.shape, Op.kind, Bool.and_eq_true, Bool.not_eq_true'] at ha
    obtain ⟨hw, hg⟩ := ha
    have hw := empty_wh (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hw
    have hg := none_of_isSome hg
    subst hw hg
    have hf : xs.filter (allHaving (p :: hav)) = (xs.filter p).filter (allHaving hav) := by
      rw [List.filter_filter]
      congr 1
      funext r
      simp only [allHaving, Bool.and_comm]
    simp only [Block.add, Block.eval, Block.select, Block.finish, Op.step, allWhere_nil,
      filter_true', hf]
  | project f =>
    simp only [accepts, Block.shape, Op.kind, Bool.and_eq_true, Bool.not_eq_true',
      Bool.or_eq_true, Bool.or_eq_false_iff] at ha
    obtain ⟨⟨⟨⟨hsel, _⟩, hg⟩, hh⟩, hws⟩ := ha
    have hsel := none_of_isSome hsel
    have hg := none_of_isSome hg
    have hh := empty_hav (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hh
    subst hsel hg hh
    have hsafe : ∀ q ∈ wh, ∀ r, q.1 (f r) = q.1 r := by
      intro q hq
      rcases hws with hw | hw
      · have := empty_wh (b := ⟨wh, none, aggSel, [], none, dist, ord, off, lim⟩) hw
        simp_all
      · exact hs q hq (safe_of_allSafe wh hw q hq)
    have hmap : (xs.map f).filter (allWhere wh) = (xs.filter (allWhere wh)).map f := by
      rw [List.filter_map]
      congr 2
      funext r
      exact allWhere_map wh f hsafe r
    simp only [Block.add, Block.eval, Block.select, Block.finish, Op.step, allHaving_nil,
      filter_true', hmap]
  | distinct f =>
    simp only [accepts, Block.shape, Op.kind, Bool.and_eq_true, Bool.not_eq_true',
      Bool.or_eq_true, Bool.or_eq_false_iff] at ha
    obtain ⟨⟨⟨⟨⟨hsel, _⟩, hd⟩, hg⟩, hh⟩, hws⟩ := ha
    have hsel := none_of_isSome hsel
    have hg := none_of_isSome hg
    have hh := empty_hav (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hh
    subst hsel hg hh hd
    have hsafe : ∀ q ∈ wh, ∀ r, q.1 (f r) = q.1 r := by
      intro q hq
      rcases hws with hw | hw
      · have := empty_wh (b := ⟨wh, none, aggSel, [], none, false, ord, off, lim⟩) hw
        simp_all
      · exact hs q hq (safe_of_allSafe wh hw q hq)
    have hmap : (xs.map f).filter (allWhere wh) = (xs.filter (allWhere wh)).map f := by
      rw [List.filter_map]
      congr 2
      funext r
      exact allWhere_map wh f hsafe r
    simp only [Block.add, Block.eval, Block.select, Block.finish, Op.step, allHaving_nil,
      filter_true', ite_true, Bool.false_eq_true, ite_false, filter_dedup, hmap]
  | dedup =>
    simp only [accepts, Block.shape, Op.kind, Bool.and_eq_true, Bool.not_eq_true',
      Bool.or_eq_false_iff] at ha
    obtain ⟨⟨⟨⟨hsel, _⟩, hd⟩, hg⟩, hh⟩ := ha
    have hsel := none_of_isSome hsel
    have hg := none_of_isSome hg
    have hh := empty_hav (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hh
    subst hsel hg hh hd
    simp only [Block.add, Block.eval, Block.select, Block.finish, Op.step, allHaving_nil,
      filter_true', ite_true, Bool.false_eq_true, ite_false, filter_dedup]
  | group g =>
    simp only [accepts, Block.shape, Op.kind, Bool.and_eq_true, Bool.not_eq_true'] at ha
    obtain ⟨hw, hg⟩ := ha
    have hw := empty_wh (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hw
    have hg := none_of_isSome hg
    subst hw hg
    simp only [Block.add, Block.eval, Block.select, Block.finish, Op.step, allWhere_nil,
      filter_true']
  | agg g =>
    simp only [accepts, Block.shape, Op.kind, Bool.and_eq_true, Bool.not_eq_true'] at ha
    obtain ⟨⟨hw, hg⟩, _⟩ := ha
    have hw := empty_wh (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hw
    have hg := none_of_isSome hg
    subst hw hg
    simp only [Block.add, Block.eval, Block.select, Block.finish, Op.step, allWhere_nil,
      filter_true']
  | sort k s =>
    simp only [accepts, Block.shape, Op.kind, Bool.and_eq_true, Bool.not_eq_true',
      Bool.or_eq_true, Bool.or_eq_false_iff] at ha
    obtain ⟨⟨⟨ho, hg⟩, hd⟩, hsel⟩ := ha
    have ho := none_of_isSome ho
    have hg := none_of_isSome hg
    subst ho hg hd
    cases sel with
    | none =>
      simp only [Block.add, Block.eval, Block.select, Block.finish, Op.step, Bool.false_eq_true,
        ite_false, filter_sortBy]
    | some f =>
      have hk : ∀ r, k (f r) = k r := by
        rcases hsel with h | h
        · simp at h
        · subst h; exact hs f rfl
      simp only [Block.add, Block.eval, Block.select, Block.finish, Op.step, Bool.false_eq_true,
        ite_false, filter_sortBy, sortBy_map k f hk]
  | limit n =>
    simp only [accepts, Block.shape, Op.kind, Bool.and_eq_true, Bool.not_eq_true'] at ha
    obtain ⟨⟨⟨⟨⟨⟨hw, hg⟩, hh⟩, hd⟩, ho⟩, hoff⟩, hl⟩ := ha
    have hw := empty_wh (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hw
    have hh := empty_hav (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hh
    have hg := none_of_isSome hg
    have ho := none_of_isSome ho
    have hoff := none_of_isSome hoff
    have hl := none_of_isSome hl
    subst hw hh hg ho hoff hl hd
    cases sel <;> simp [Block.add, Block.eval, Block.select, Block.finish, Op.step,
      allWhere_nil, allHaving_nil, filter_true', List.map_take]
  | offset n =>
    simp only [accepts, Block.shape, Op.kind, Bool.and_eq_true, Bool.not_eq_true'] at ha
    obtain ⟨⟨⟨⟨⟨hw, hg⟩, hh⟩, hd⟩, ho⟩, hoff⟩ := ha
    have hw := empty_wh (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hw
    have hh := empty_hav (b := ⟨wh, grp, aggSel, hav, sel, dist, ord, off, lim⟩) hh
    have hg := none_of_isSome hg
    have ho := none_of_isSome ho
    have hoff := none_of_isSome hoff
    subst hw hh hg ho hoff hd
    cases sel <;> cases lim <;> simp [Block.add, Block.eval, Block.select, Block.finish, Op.step,
      allWhere_nil, allHaving_nil, filter_true', List.map_drop]

/-! ## The fold -/

theorem eval_empty (xs : List R) : ({} : Block R).eval xs = xs := by
  simp [Block.eval, Block.select, Block.finish, allWhere_nil, allHaving_nil]

omit [DecidableEq R] in
theorem accepts_empty (k : Kind) : accepts ({} : Block R).shape k = true := by
  cases k <;> simp [accepts, Block.shape]

omit [DecidableEq R] in
theorem scopeOk_empty (op : Op R) : ({} : Block R).scopeOk op := by
  cases op with
  | sort k s => cases s <;> simp [Block.scopeOk]
  | _ => simp [Block.scopeOk]

/-- Run a pipeline given last operator first. -/
def runRev (rs : List (Op R)) (xs : List R) : List R := rs.foldr (fun op acc => op.step acc) xs

theorem fold_eval (rs : List (Op R)) :
    ∀ (b : Block R), Scoped b rs → ∀ xs, evalBlocks (fold b rs) xs = b.eval (runRev rs xs) := by
  induction rs with
  | nil => intro b _ xs; simp [fold, evalBlocks, runRev]
  | cons op rest ih =>
    intro b hsc xs
    by_cases ha : accepts b.shape op.kind = true
    · simp only [Scoped, ha, ite_true] at hsc
      simp only [fold, ha, ite_true]
      rw [ih _ hsc.2 xs, merge_sound b op ha hsc.1]
      rfl
    · simp only [Scoped, ha, ite_false, Bool.false_eq_true] at hsc
      simp only [fold, ha, ite_false, Bool.false_eq_true]
      show b.eval (evalBlocks (fold (({} : Block R).add op) rest) xs) = _
      rw [ih _ hsc xs, merge_sound _ op (accepts_empty _) (scopeOk_empty op), eval_empty]
      rfl

/--
**Soundness.** Folding a pipeline into nested SELECT blocks never changes its rows, provided the
scope flags the fold relies on are truthful. The blocks are evaluated as nested subqueries that
keep their row order (see `README.md` on ORDER BY in subqueries).
-/
theorem fold_sound (ops : List (Op R)) (h : Scoped {} ops.reverse) (xs : List R) :
    evalBlocks (fold {} ops.reverse) xs = run ops xs := by
  rw [fold_eval _ {} h, eval_empty, runRev, List.foldr_reverse]
  rfl

/-! ## The exported fold computes the same blocks -/

omit [DecidableEq R] in
theorem shape_add (b : Block R) (op : Op R) : (b.add op).shape = b.shape.add op.kind := by
  cases op <;> simp [Block.add, Block.shape, Shape.add, Op.kind, allSafe]

omit [DecidableEq R] in
theorem shape_empty : ({} : Block R).shape = ({} : Shape) := rfl

omit [DecidableEq R] in
/-- `foldShapes`, which the conformance traces export, decides exactly as `fold` does. -/
theorem fold_shapes (rs : List (Op R)) :
    ∀ b : Block R, (fold b rs).map Block.shape = foldShapes b.shape (rs.map Op.kind) := by
  induction rs with
  | nil => intro b; simp [fold, foldShapes]
  | cons op rest ih =>
    intro b
    by_cases ha : accepts b.shape op.kind = true
    · simp [fold, foldShapes, ha, ih, shape_add]
    · simp [fold, foldShapes, ha, ih, shape_add, shape_empty]

end Wvlet.SqlFold
