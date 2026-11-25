/-
Licensed under the Apache License, Version 2.0.

The filter an `or` of branches implies on one join input (wvlet-engine's `PushDownFilters`, with
`or_implied_filters`): the `or`, over the branches, of each branch's conjuncts that read only that
input's columns, when every branch has one. In
`(l.a = 1 and r.s = 'x') or (l.a < 2 and r.v)` the left input gets `l.a = 1 or l.a < 2` and the
right one `r.s = 'x' or r.v`, while the original `or` stays over the join.

Whenever the `or` is TRUE, some branch is TRUE, so each of its conjuncts is TRUE, the local ones
among them (`implied_true`). A WHERE keeps the rows where a condition is TRUE, so adding the implied
filter as one more conjunct keeps the same rows (`filter_implied`), NULLs included: on a row where
the `or` is unknown the implied filter may be FALSE, but the row is dropped either way. This is a
filter rewrite, not a value rewrite: the engine adds the implied filter only beside the `or` as a
conjunct of an inner join's conditions, never under a NOT or into an outer join's ON. Its columns
are those of the input (`implied_reads`), so the filter pushdown can move it there.
-/
import WvletFormal.FactorOr.Rule

namespace Wvlet.FactorOr

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

/-- A conjunct is local to an input with columns `cols` when it reads some column, and only theirs
(the engine: `refs.nonEmpty && refs.subsetOf(ids)`). -/
def isLocal (cols : List Col) (c : Cond) : Bool :=
  !c.reads.isEmpty && c.reads.all fun i => decide (i ∈ cols)

/-- The conjuncts of a branch local to the input. -/
def localConjuncts (cols : List Col) (b : List Cond) : List Cond := b.filter (isLocal cols)

/-- The filter the `or` of the branches `bs` implies on the input with columns `cols`: none when a
branch has no local conjunct, or when the `or` reads only the input's columns already (it moves
there itself). -/
def implied (cols : List Col) (bs : List (List Cond)) : Option Cond :=
  if bs.isEmpty || bs.any (fun b => (localConjuncts cols b).isEmpty) then none
  else if (disj (bs.map conj)).reads.all (fun i => decide (i ∈ cols)) then none
  else some (disj (bs.map fun b => conj (localConjuncts cols b)))

/-- The implied filter of a conjunct that is an `or`. -/
def impliedCond (cols : List Col) : Cond → Option Cond
  | .or p q => implied cols ((disjuncts (.or p q)).map conjuncts)
  | _ => none

/-! ### Soundness -/

/-- The implied filter is TRUE wherever the `or` is. -/
theorem implied_true (cols : List Col) (bs : List (List Cond)) (i : Cond)
    (h : implied cols bs = some i) (x : Row) (ho : (disj (bs.map conj)).eval x = .true) :
    i.eval x = .true := by
  simp only [implied] at h
  split at h
  · contradiction
  split at h
  · contradiction
  cases h
  rw [disj_eval, List.map_map, orT_map_true] at ho ⊢
  obtain ⟨b, hb, hbt⟩ := ho
  refine ⟨b, hb, ?_⟩
  simp only [Function.comp_def, conj_eval, andT_map_true] at hbt ⊢
  intro c hc
  exact hbt c (List.mem_filter.mp hc).1

/-- `impliedCond` gives a filter TRUE wherever the conjunct is. -/
theorem impliedCond_true (cols : List Col) (e i : Cond) (h : impliedCond cols e = some i)
    (x : Row) (he : e.eval x = .true) : i.eval x = .true := by
  cases e with
  | or p q =>
    refine implied_true cols _ i h x ?_
    rw [List.map_map, ← he, ← disj_disjuncts x (.or p q), disj_eval, disj_eval, List.map_map]
    congr 1
    apply List.map_congr_left
    intro b _
    simp only [Function.comp_def]
    exact conj_conjuncts x b
  | _ => simp [impliedCond] at h

/-- A WHERE over a conjunct and the filter it implies keeps the conjunct's rows. -/
theorem filter_implied (cols : List Col) (e i : Cond) (h : impliedCond cols e = some i)
    (r : Rel) : filter (.and e i) r = filter e r := by
  unfold filter
  apply List.filter_congr
  intro x _
  rw [Cond.holds_and]
  cases he : e.holds x
  · rfl
  · simp only [Cond.holds, beq_iff_eq] at he ⊢
    simp [impliedCond_true cols e i h x he]

/-! ### Locality -/

theorem reads_foldl_and (ps : List Cond) (p : Cond) (c : Col) (hc : c ∈ (ps.foldl .and p).reads) :
    c ∈ p.reads ∨ ∃ q ∈ ps, c ∈ q.reads := by
  induction ps generalizing p with
  | nil => exact Or.inl hc
  | cons q qs ih =>
    rcases ih (.and p q) hc with h | ⟨q', hq', h⟩
    · simp only [Cond.reads, List.mem_append] at h
      rcases h with h | h
      · exact Or.inl h
      · exact Or.inr ⟨q, List.mem_cons_self, h⟩
    · exact Or.inr ⟨q', List.mem_cons_of_mem _ hq', h⟩

theorem reads_foldl_or (ps : List Cond) (p : Cond) (c : Col) (hc : c ∈ (ps.foldl .or p).reads) :
    c ∈ p.reads ∨ ∃ q ∈ ps, c ∈ q.reads := by
  induction ps generalizing p with
  | nil => exact Or.inl hc
  | cons q qs ih =>
    rcases ih (.or p q) hc with h | ⟨q', hq', h⟩
    · simp only [Cond.reads, List.mem_append] at h
      rcases h with h | h
      · exact Or.inl h
      · exact Or.inr ⟨q, List.mem_cons_self, h⟩
    · exact Or.inr ⟨q', List.mem_cons_of_mem _ hq', h⟩

/-- A conjunction of a non-empty list reads only what its conjuncts read. -/
theorem reads_conj (ps : List Cond) (hne : ps ≠ []) (c : Col) (hc : c ∈ (conj ps).reads) :
    ∃ q ∈ ps, c ∈ q.reads := by
  match ps, hne with
  | p :: ps, _ =>
    rcases reads_foldl_and ps p c hc with h | ⟨q, hq, h⟩
    · exact ⟨p, List.mem_cons_self, h⟩
    · exact ⟨q, List.mem_cons_of_mem _ hq, h⟩

/-- A disjunction of a non-empty list reads only what its branches read. -/
theorem reads_disj (ps : List Cond) (hne : ps ≠ []) (c : Col) (hc : c ∈ (disj ps).reads) :
    ∃ q ∈ ps, c ∈ q.reads := by
  match ps, hne with
  | p :: ps, _ =>
    rcases reads_foldl_or ps p c hc with h | ⟨q, hq, h⟩
    · exact ⟨p, List.mem_cons_self, h⟩
    · exact ⟨q, List.mem_cons_of_mem _ hq, h⟩

/-- The implied filter reads only the input's columns, so it can filter that input. -/
theorem implied_reads (cols : List Col) (bs : List (List Cond)) (i : Cond)
    (h : implied cols bs = some i) (c : Col) (hc : c ∈ i.reads) : c ∈ cols := by
  simp only [implied] at h
  split at h
  · contradiction
  rename_i hb
  split at h
  · contradiction
  cases h
  have hne : bs ≠ [] := by
    intro he; simp [he] at hb
  obtain ⟨q, hq, hcq⟩ := reads_disj _ (by simpa using hne) c hc
  obtain ⟨b, hb', rfl⟩ := List.mem_map.mp hq
  have hl : localConjuncts cols b ≠ [] := by
    intro he
    apply hb
    simp only [Bool.or_eq_true, List.any_eq_true]
    exact Or.inr ⟨b, hb', by simp [he]⟩
  obtain ⟨d, hd, hcd⟩ := reads_conj _ hl c hcq
  have := (List.mem_filter.mp hd).2
  simp only [isLocal, Bool.and_eq_true, Bool.not_eq_true', List.isEmpty_eq_false_iff,
    List.all_eq_true, decide_eq_true_eq] at this
  exact this.2 c hcd

end Wvlet.FactorOr
