/-
Licensed under the Apache License, Version 2.0.

The graph search shared by `ModelOrder.closure` (selector closures) and `ModelOrder.upstreamsOf`
(edges through ephemeral models) in
wvlet-lang/src/main/scala/wvlet/lang/compiler/pipeline/ModelOrder.scala. Both pop a work list, skip
visited nodes, mark the node visited, and push its neighbors when the node may be expanded; they
differ only in where neighbors are pushed (front: depth-first; back: breadth-first).
-/

namespace Wvlet.ModelBuild

variable {α : Type}

/-! ## Counting lemmas for termination -/

theorem filter_length_mono {p q : α → Bool} :
    ∀ (l : List α), (∀ x ∈ l, q x = true → p x = true) →
      (l.filter q).length ≤ (l.filter p).length
  | [], _ => by simp
  | a :: l, h => by
    have ih := filter_length_mono l (fun x hx => h x (List.mem_cons_of_mem a hx))
    have ha := h a (List.mem_cons_self ..)
    by_cases hq : q a = true
    · simp [hq, ha hq]; omega
    · by_cases hp : p a = true
      · simp [hq, hp]; omega
      · simp [hq, hp]; omega

theorem filter_length_lt {p q : α → Bool} :
    ∀ (l : List α), (∀ x ∈ l, q x = true → p x = true) →
      ∀ a ∈ l, p a = true → q a = false → (l.filter q).length < (l.filter p).length
  | [], _, a, ha, _, _ => by simp at ha
  | b :: l, h, a, ha, hpa, hqa => by
    have hmono := filter_length_mono l (fun x hx => h x (List.mem_cons_of_mem b hx))
    rcases List.mem_cons.mp ha with rfl | ha'
    · simp [hpa, hqa]; omega
    · have ih := filter_length_lt l (fun x hx => h x (List.mem_cons_of_mem b hx)) a ha' hpa hqa
      have hb := h b (List.mem_cons_self ..)
      by_cases hq : q b = true
      · simp [hq, hb hq]; omega
      · by_cases hp : p b = true
        · simp [hq, hp]; omega
        · simp [hq, hp]; omega

section Decidable

variable [DecidableEq α]

/-- Nodes of `univ` not yet visited: the first component of every termination measure. -/
def unvisited (univ visited : List α) : Nat :=
  (univ.filter (fun x => decide (x ∉ visited))).length

theorem unvisited_le (univ visited : List α) (n : α) :
    unvisited univ (visited ++ [n]) ≤ unvisited univ visited :=
  filter_length_mono univ (fun x _ h => by simp_all)

theorem unvisited_lt (univ visited : List α) (n : α) (hu : n ∈ univ) (hv : n ∉ visited) :
    unvisited univ (visited ++ [n]) < unvisited univ visited :=
  filter_length_lt univ (fun x _ h => by simp_all) n hu (by simp [hv]) (by simp)

theorem lex_of_le_of_lt {a a' b b' : Nat} (ha : a ≤ a') (hb : b < b') :
    Prod.Lex (· < ·) (· < ·) (a, b) (a', b') := by
  rcases Nat.lt_or_eq_of_le ha with h | rfl
  · exact Prod.Lex.left _ _ h
  · exact Prod.Lex.right _ hb

/-! ## The search -/

/--
Visit every node reachable from the work list. A node is expanded (its `next` nodes pushed) only
when `expand` holds and it belongs to `univ`; `push new rest` places the new nodes in the work
list. Returns the visited nodes in visiting order. Termination: each step either visits a new node
of `univ` or shortens the work list without expanding.
-/
def explore (univ : List α) (expand : α → Bool) (next : α → List α)
    (push : List α → List α → List α) : List α → List α → List α
  | [], visited => visited
  | n :: q, visited =>
    if n ∈ visited then explore univ expand next push q visited
    else if expand n && decide (n ∈ univ) then
      explore univ expand next push (push (next n) q) (visited ++ [n])
    else explore univ expand next push q (visited ++ [n])
termination_by q visited => (unvisited univ visited, q.length)
decreasing_by
  · exact lex_of_le_of_lt (Nat.le_refl _) (by simp)
  · rename_i h1 h2
    simp only [Bool.and_eq_true, decide_eq_true_eq] at h2
    exact Prod.Lex.left _ _ (unvisited_lt univ visited n h2.2 h1)
  · exact lex_of_le_of_lt (unvisited_le univ visited n) (by simp)

end Decidable

/-- One expansion step of the search. -/
def Step (univ : List α) (expand : α → Bool) (next : α → List α) (x y : α) : Prop :=
  expand x = true ∧ x ∈ univ ∧ y ∈ next x

/-- Reflexive-transitive closure. -/
inductive Star (R : α → α → Prop) : α → α → Prop where
  | refl (a : α) : Star R a a
  | tail {a b c : α} : Star R a b → R b c → Star R a c

theorem Star.single {R : α → α → Prop} {a b : α} (h : R a b) : Star R a b :=
  Star.tail (Star.refl a) h

theorem Star.trans {R : α → α → Prop} {a b c : α} (h1 : Star R a b) (h2 : Star R b c) :
    Star R a c := by
  induction h2 with
  | refl => exact h1
  | tail _ hr ih => exact Star.tail ih hr

theorem Star.head {R : α → α → Prop} {a b c : α} (h1 : R a b) (h2 : Star R b c) : Star R a c :=
  Star.trans (Star.single h1) h2

theorem Star.flip {R : α → α → Prop} {a b : α} (h : Star R a b) : Star (fun x y => R y x) b a := by
  induction h with
  | refl => exact Star.refl _
  | tail _ hr ih => exact Star.head hr ih

theorem Star.mono {R S : α → α → Prop} (hrs : ∀ x y, R x y → S x y) {a b : α} (h : Star R a b) :
    Star S a b := by
  induction h with
  | refl => exact Star.refl _
  | tail _ hr ih => exact Star.tail ih (hrs _ _ hr)

section Search

variable [DecidableEq α] (univ : List α) (expand : α → Bool) (next : α → List α)
  (push : List α → List α → List α)
  (hpush : ∀ x a b, x ∈ push a b ↔ x ∈ a ∨ x ∈ b)

include hpush in
/-- Nothing visited or queued is lost. -/
theorem explore_superset (q visited : List α) :
    (∀ x ∈ visited, x ∈ explore univ expand next push q visited) ∧
      (∀ x ∈ q, x ∈ explore univ expand next push q visited) := by
  fun_induction explore univ expand next push q visited with
  | case1 => simp
  | case2 n q visited hn ih =>
    refine ⟨ih.1, fun x hx => ?_⟩
    rcases List.mem_cons.mp hx with rfl | hx
    · exact ih.1 _ hn
    · exact ih.2 _ hx
  | case3 n q visited hn hexp ih =>
    refine ⟨fun x hx => ih.1 _ (List.mem_append_left _ hx), fun x hx => ?_⟩
    rcases List.mem_cons.mp hx with rfl | hx
    · exact ih.1 _ (by simp)
    · exact ih.2 _ ((hpush _ _ _).mpr (Or.inr hx))
  | case4 n q visited hn hexp ih =>
    refine ⟨fun x hx => ih.1 _ (List.mem_append_left _ hx), fun x hx => ?_⟩
    rcases List.mem_cons.mp hx with rfl | hx
    · exact ih.1 _ (by simp)
    · exact ih.2 _ hx

include hpush in
/-- Soundness: a visited node was visited before or is reachable from the work list. -/
theorem explore_sound (q visited : List α) :
    ∀ x ∈ explore univ expand next push q visited,
      x ∈ visited ∨ ∃ s ∈ q, Star (Step univ expand next) s x := by
  fun_induction explore univ expand next push q visited with
  | case1 => intro x hx; exact Or.inl hx
  | case2 n q visited hn ih =>
    intro x hx
    rcases ih x hx with h | ⟨s, hs, hr⟩
    · exact Or.inl h
    · exact Or.inr ⟨s, List.mem_cons_of_mem _ hs, hr⟩
  | case3 n q visited hn hexp ih =>
    intro x hx
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hexp
    rcases ih x hx with h | ⟨s, hs, hr⟩
    · rcases List.mem_append.mp h with h | h
      · exact Or.inl h
      · simp at h; subst h; exact Or.inr ⟨x, List.mem_cons_self .., Star.refl _⟩
    · rcases (hpush _ _ _).mp hs with hs | hs
      · exact Or.inr ⟨n, List.mem_cons_self .., Star.head ⟨hexp.1, hexp.2, hs⟩ hr⟩
      · exact Or.inr ⟨s, List.mem_cons_of_mem _ hs, hr⟩
  | case4 n q visited hn hexp ih =>
    intro x hx
    rcases ih x hx with h | ⟨s, hs, hr⟩
    · rcases List.mem_append.mp h with h | h
      · exact Or.inl h
      · simp at h; subst h; exact Or.inr ⟨x, List.mem_cons_self .., Star.refl _⟩
    · exact Or.inr ⟨s, List.mem_cons_of_mem _ hs, hr⟩

/-- Every visited node had its successors visited or queued. -/
def Frontier (visited q : List α) : Prop :=
  ∀ v ∈ visited, ∀ y, Step univ expand next v y → y ∈ visited ∨ y ∈ q

include hpush in
/-- Completeness: the result is closed under steps. -/
theorem explore_closed (q visited : List α) (hinv : Frontier univ expand next visited q) :
    ∀ v ∈ explore univ expand next push q visited, ∀ y, Step univ expand next v y →
      y ∈ explore univ expand next push q visited := by
  fun_induction explore univ expand next push q visited with
  | case1 visited =>
    intro v hv y hs
    rcases hinv v hv y hs with h | h
    · exact h
    · simp at h
  | case2 n q visited hn ih =>
    apply ih
    intro v hv y hs
    rcases hinv v hv y hs with h | h
    · exact Or.inl h
    · rcases List.mem_cons.mp h with rfl | h
      · exact Or.inl hn
      · exact Or.inr h
  | case3 n q visited hn hexp ih =>
    apply ih
    intro v hv y hs
    rcases List.mem_append.mp hv with hv | hv
    · rcases hinv v hv y hs with h | h
      · exact Or.inl (List.mem_append_left _ h)
      · rcases List.mem_cons.mp h with rfl | h
        · exact Or.inl (by simp)
        · exact Or.inr ((hpush _ _ _).mpr (Or.inr h))
    · simp at hv; subst hv
      exact Or.inr ((hpush _ _ _).mpr (Or.inl hs.2.2))
  | case4 n q visited hn hexp ih =>
    apply ih
    intro v hv y hs
    rcases List.mem_append.mp hv with hv | hv
    · rcases hinv v hv y hs with h | h
      · exact Or.inl (List.mem_append_left _ h)
      · rcases List.mem_cons.mp h with rfl | h
        · exact Or.inl (by simp)
        · exact Or.inr h
    · simp at hv; subst hv
      simp [hs.1, hs.2.1] at hexp

/-- Each node is visited once. -/
theorem explore_nodup (q visited : List α) (h : visited.Nodup) :
    (explore univ expand next push q visited).Nodup := by
  fun_induction explore univ expand next push q visited with
  | case1 => exact h
  | case2 n q visited hn ih => exact ih h
  | case3 n q visited hn hexp ih =>
    exact ih (List.nodup_append.mpr ⟨h, by simp, by simp; intro a ha he; exact hn (he ▸ ha)⟩)
  | case4 n q visited hn hexp ih =>
    exact ih (List.nodup_append.mpr ⟨h, by simp, by simp; intro a ha he; exact hn (he ▸ ha)⟩)

include hpush in
/-- Starting from nothing visited, the search visits exactly the nodes reachable from the work list. -/
theorem mem_explore_iff (q : List α) (x : α) :
    x ∈ explore univ expand next push q [] ↔ ∃ s ∈ q, Star (Step univ expand next) s x := by
  constructor
  · intro hx
    rcases explore_sound univ expand next push hpush q [] x hx with h | h
    · simp at h
    · exact h
  · rintro ⟨s, hs, hr⟩
    have hclosed := explore_closed univ expand next push hpush q [] (by intro v hv; simp at hv)
    have hs' := (explore_superset univ expand next push hpush q []).2 s hs
    induction hr with
    | refl => exact hs'
    | tail _ hstep ih => exact hclosed _ ih _ hstep

end Search

theorem mem_front_push (x : α) (a b : List α) : x ∈ a ++ b ↔ x ∈ a ∨ x ∈ b := List.mem_append

theorem mem_back_push (x : α) (a b : List α) : x ∈ b ++ a ↔ x ∈ a ∨ x ∈ b := by
  rw [List.mem_append]; exact Or.comm

end Wvlet.ModelBuild
