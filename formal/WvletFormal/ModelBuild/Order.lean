/-
Licensed under the Apache License, Version 2.0.

Build order and cycle detection of `wvlet build` (`ModelOrder.buildOrder` and
`ModelOrder.findCycle` in wvlet-lang/src/main/scala/wvlet/lang/compiler/pipeline/ModelOrder.scala).
-/
import WvletFormal.ModelBuild.Graph

namespace Wvlet.ModelBuild

variable {α : Type} [DecidableEq α]

/-! ## Rounds -/

/-- A model is ready when every upstream model among `names` is already built. -/
def isReady (ups : α → List α) (names done : List α) (m : α) : Bool :=
  (ups m).all fun u => decide (u ∉ names) || decide (u ∈ done)

omit [DecidableEq α] in
theorem filter_not_length_lt (p : α → Bool) (l : List α) (h : l.filter p ≠ []) :
    (l.filter (fun x => !p x)).length < l.length := by
  have hlen := (List.filter_append_perm p l).length_eq
  rw [List.length_append] at hlen
  have : 0 < (l.filter p).length := List.length_pos_iff.mpr h
  omega

/-- The models of `rest` that can be built once `done` is built, in input order. -/
def readyOf (ups : α → List α) (names done rest : List α) : List α :=
  rest.filter (isReady ups names done)

/-- The models of `rest` that must wait. -/
def blockedOf (ups : α → List α) (names done rest : List α) : List α :=
  rest.filter fun m => !isReady ups names done m

/--
The rounds of `ModelOrder.buildOrder`: each round is every remaining model whose upstream models
among `names` are built, in input order. When no model is ready, the remaining models are
returned as an error.
-/
def rounds (ups : α → List α) (names : List α) (done rest : List α) :
    Except (List α) (List (List α)) :=
  if rest = [] then .ok []
  else if _h : readyOf ups names done rest = [] then .error rest
  else
    (rounds ups names (done ++ readyOf ups names done rest) (blockedOf ups names done rest)).map
      (readyOf ups names done rest :: ·)
termination_by rest.length
decreasing_by exact filter_not_length_lt _ _ (by assumption)

/-- Models no round could build: every one of them reads another one. -/
def Closed (ups : α → List α) (s : List α) : Prop := ∀ m ∈ s, ∃ u ∈ ups m, u ∈ s

theorem rounds_perm (ups : α → List α) (names done rest : List α) :
    ∀ rs, rounds ups names done rest = .ok rs → rs.flatten.Perm rest := by
  fun_induction rounds ups names done rest with
  | case1 => intro rs hr; cases hr; simp
  | case2 => intro rs hr; cases hr
  | case3 done rest h1 h2 ih =>
    intro rs hr
    cases hrec : rounds ups names (done ++ readyOf ups names done rest)
        (blockedOf ups names done rest) with
    | error b => rw [hrec] at hr; cases hr
    | ok rs' =>
      rw [hrec] at hr; cases hr
      simp only [List.flatten_cons]
      exact ((ih rs' hrec).append_left _).trans (List.filter_append_perm _ rest)

theorem rounds_sublist (ups : α → List α) (names done rest : List α) :
    ∀ rs, rounds ups names done rest = .ok rs → ∀ r ∈ rs, r.Sublist rest := by
  fun_induction rounds ups names done rest with
  | case1 => intro rs hr; cases hr; simp
  | case2 => intro rs hr; cases hr
  | case3 done rest h1 h2 ih =>
    intro rs hr
    cases hrec : rounds ups names (done ++ readyOf ups names done rest)
        (blockedOf ups names done rest) with
    | error b => rw [hrec] at hr; cases hr
    | ok rs' =>
      rw [hrec] at hr; cases hr
      intro r hr
      rcases List.mem_cons.mp hr with rfl | hr
      · exact List.filter_sublist
      · exact (ih rs' hrec r hr).trans List.filter_sublist

/-- Topological: an upstream model among `names` is built before, or was already built. -/
theorem rounds_topological (ups : α → List α) (names done rest : List α) :
    ∀ rs, rounds ups names done rest = .ok rs →
      ∀ o1 m o2, rs.flatten = o1 ++ m :: o2 → ∀ u ∈ ups m, u ∈ names → u ∈ done ∨ u ∈ o1 := by
  fun_induction rounds ups names done rest with
  | case1 => intro rs hr; cases hr; simp
  | case2 => intro rs hr; cases hr
  | case3 done rest h1 h2 ih =>
    intro rs hr
    cases hrec : rounds ups names (done ++ readyOf ups names done rest)
        (blockedOf ups names done rest) with
    | error b => rw [hrec] at hr; cases hr
    | ok rs' =>
      rw [hrec] at hr; cases hr
      intro o1 m o2 ho u hu hun
      simp only [List.flatten_cons] at ho
      rcases List.append_eq_append_iff.mp ho.symm with ⟨as, hready, hrest⟩ | ⟨bs, ho1, hflat⟩
      · cases as with
        | nil =>
          simp at hready hrest
          have := ih rs' hrec [] m o2 (by simp [hrest]) u hu hun
          simp at this
          rcases this with h | h
          · exact Or.inl h
          · exact Or.inr (hready ▸ h)
        | cons a as =>
          simp at hrest
          obtain ⟨rfl, -⟩ := hrest
          have hm : m ∈ readyOf ups names done rest := by rw [hready]; simp
          have := (List.all_eq_true.mp (List.mem_filter.mp hm).2) u hu
          simp [hun] at this
          exact Or.inl this
      · have := ih rs' hrec bs m o2 hflat u hu hun
        rcases this with h | h
        · rcases List.mem_append.mp h with h | h
          · exact Or.inl h
          · exact Or.inr (ho1 ▸ List.mem_append_left _ h)
        · exact Or.inr (ho1 ▸ List.mem_append_right _ h)

/-- Level order: a model of a later round reads a model of the round just before it. -/
theorem rounds_level (ups : α → List α) (names done rest : List α) :
    ∀ rs, rounds ups names done rest = .ok rs →
      ∀ pre r1 r2 post, rs = pre ++ r1 :: r2 :: post →
        ∀ m ∈ r2, ∃ u ∈ ups m, u ∈ names ∧ u ∈ r1 := by
  fun_induction rounds ups names done rest with
  | case1 => intro rs hr; cases hr; simp
  | case2 => intro rs hr; cases hr
  | case3 done rest h1 h2 ih =>
    intro rs hr
    cases hrec : rounds ups names (done ++ readyOf ups names done rest)
        (blockedOf ups names done rest) with
    | error b => rw [hrec] at hr; cases hr
    | ok rs' =>
      rw [hrec] at hr; cases hr
      intro pre r1 r2 post hrs m hm
      cases pre with
      | nil =>
        simp at hrs
        obtain ⟨rfl, hrs'⟩ := hrs
        -- `m` is in the first round of the recursive call: ready now, not ready before
        subst hrs'
        have hsub := rounds_sublist ups names _ _ _ hrec r2 (by simp)
        have hblocked := List.mem_filter.mp (hsub.subset hm)
        have hnot : isReady ups names done m = false := by simpa using hblocked.2
        unfold isReady at hnot
        rw [List.all_eq_false] at hnot
        obtain ⟨u, hu, hu'⟩ := hnot
        simp at hu'
        refine ⟨u, hu, hu'.1, ?_⟩
        -- ready in the next round, so `u` was built in this round
        have hready := rounds_first ups names _ _ _ hrec r2 _ rfl m hm u hu hu'.1
        rcases List.mem_append.mp hready with h | h
        · exact absurd h hu'.2
        · exact h
      | cons p pre =>
        simp at hrs
        obtain ⟨-, hrs'⟩ := hrs
        exact ih rs' hrec pre r1 r2 post hrs' m hm
where
  /-- Every model of the first round has its upstream models among `names` built. -/
  rounds_first (ups : α → List α) (names done rest : List α) :
      ∀ rs, rounds ups names done rest = .ok rs → ∀ r1 tail, rs = r1 :: tail →
        ∀ m ∈ r1, ∀ u ∈ ups m, u ∈ names → u ∈ done := by
    intro rs hr r1 tail hrs m hm u hu hun
    unfold rounds at hr
    split at hr
    · cases hr; cases hrs
    · split at hr
      · cases hr
      · cases hrec : rounds ups names (done ++ readyOf ups names done rest)
            (blockedOf ups names done rest) with
        | error b => rw [hrec] at hr; cases hr
        | ok rs' =>
          rw [hrec] at hr; cases hr
          simp at hrs
          obtain ⟨rfl, -⟩ := hrs
          have := (List.all_eq_true.mp (List.mem_filter.mp hm).2) u hu
          simpa [hun] using this

/-- When no round can build the rest, the rest is a nonempty set of models that read each other. -/
theorem rounds_error (ups : α → List α) (names done rest : List α)
    (hcover : ∀ x ∈ names, x ∈ done ∨ x ∈ rest) :
    ∀ b, rounds ups names done rest = .error b → b ≠ [] ∧ b.Sublist rest ∧ Closed ups b := by
  fun_induction rounds ups names done rest with
  | case1 => intro b hb; cases hb
  | case2 done rest h1 h2 =>
    intro b hb
    cases hb
    refine ⟨h1, List.Sublist.refl _, fun m hm => ?_⟩
    have hnot : isReady ups names done m = false := by
      cases hc : isReady ups names done m
      · rfl
      · have : m ∈ readyOf ups names done rest := List.mem_filter.mpr ⟨hm, hc⟩
        rw [h2] at this; simp at this
    unfold isReady at hnot
    rw [List.all_eq_false] at hnot
    obtain ⟨u, hu, hu'⟩ := hnot
    simp at hu'
    rcases hcover u hu'.1 with h | h
    · exact absurd h hu'.2
    · exact ⟨u, hu, h⟩
  | case3 done rest h1 h2 ih =>
    intro b hb
    cases hrec : rounds ups names (done ++ readyOf ups names done rest)
        (blockedOf ups names done rest) with
    | ok rs' => rw [hrec] at hb; cases hb
    | error b' =>
      rw [hrec] at hb; cases hb
      have hcover' : ∀ x ∈ names, x ∈ done ++ readyOf ups names done rest ∨
          x ∈ blockedOf ups names done rest := by
        intro x hx
        rcases hcover x hx with h | h
        · exact Or.inl (List.mem_append_left _ h)
        · cases hxr : isReady ups names done x
          · exact Or.inr (List.mem_filter.mpr ⟨h, by simp [hxr]⟩)
          · exact Or.inl (List.mem_append_right _ (List.mem_filter.mpr ⟨h, hxr⟩))
      obtain ⟨hne, hsub, hcl⟩ := ih hcover' _ hrec
      exact ⟨hne, hsub.trans List.filter_sublist, hcl⟩

/-! ## Build order -/

/-- `ModelOrder.buildOrder` before the error is turned into a cycle path. -/
def buildRounds (ups : α → List α) (sel : List α) : Except (List α) (List (List α)) :=
  rounds ups sel [] sel

/-- Every model of the build order comes after every selected model it reads. -/
def Topological (ups : α → List α) (sel o : List α) : Prop :=
  ∀ o1 m o2, o = o1 ++ m :: o2 → ∀ u ∈ ups m, u ∈ sel → u ∈ o1

/-- On success the build order is a topological permutation of the selection. -/
theorem buildRounds_ok (ups : α → List α) (sel : List α) (rs : List (List α))
    (h : buildRounds ups sel = .ok rs) :
    rs.flatten.Perm sel ∧ Topological ups sel rs.flatten := by
  refine ⟨rounds_perm ups sel [] sel rs h, fun o1 m o2 ho u hu hus => ?_⟩
  rcases rounds_topological ups sel [] sel rs h o1 m o2 ho u hu hus with h | h
  · simp at h
  · exact h

/-- A topological order of a list leaves no closed subset in it. -/
theorem not_closed_of_topological (ups : α → List α) (sel o : List α) (hp : o.Perm sel)
    (ht : Topological ups sel o) (s : List α) (hne : s ≠ []) (hs : ∀ x ∈ s, x ∈ sel) :
    ¬ Closed ups s := by
  intro hcl
  -- the first model of `o` in `s` reads a model of `s` built after it
  have key : ∀ o' pre, o = pre ++ o' → (∀ y ∈ pre, y ∉ s) → (∃ x ∈ o', x ∈ s) → False := by
    intro o'
    induction o' with
    | nil => intro _ _ _ h; simp at h
    | cons a o' ih =>
      intro pre ho hpre hex
      by_cases ha : a ∈ s
      · obtain ⟨u, hu, hus⟩ := hcl a ha
        exact hpre u (ht pre a o' ho u hu (hs u hus)) hus
      · exact ih (pre ++ [a]) (by simp [ho]) (by
          intro y hy; rcases List.mem_append.mp hy with hy | hy
          · exact hpre y hy
          · simp at hy; exact hy ▸ ha) (by
          obtain ⟨x, hx, hxs⟩ := hex
          rcases List.mem_cons.mp hx with rfl | hx
          · exact absurd hxs ha
          · exact ⟨x, hx, hxs⟩)
  obtain ⟨x, hx⟩ := List.exists_mem_of_ne_nil s hne
  exact key o [] (by simp) (by simp) ⟨x, hp.mem_iff.mpr (hs x hx), hx⟩

/--
The build order fails exactly when the selection contains a nonempty set of models that all read
another model of the set, i.e., a dependency cycle among selected models (see `cycle_iff_closed`).
-/
theorem buildRounds_error_iff (ups : α → List α) (sel : List α) :
    (∃ b, buildRounds ups sel = .error b) ↔
      ∃ s, s ≠ [] ∧ (∀ x ∈ s, x ∈ sel) ∧ Closed ups s := by
  constructor
  · rintro ⟨b, hb⟩
    obtain ⟨hne, hsub, hcl⟩ := rounds_error ups sel [] sel (fun x hx => Or.inr hx) b hb
    exact ⟨b, hne, fun x hx => hsub.subset hx, hcl⟩
  · rintro ⟨s, hne, hs, hcl⟩
    cases h : buildRounds ups sel with
    | error b => exact ⟨b, rfl⟩
    | ok rs =>
      obtain ⟨hp, ht⟩ := buildRounds_ok ups sel rs h
      exact absurd hcl (not_closed_of_topological ups sel _ hp ht s hne hs)

/-! ## Cycle paths -/

/-- A walk along edges: each element reads the next one. -/
inductive Path (ups : α → List α) : List α → Prop where
  | single (x : α) : Path ups [x]
  | cons {x y : α} {l : List α} : y ∈ ups x → Path ups (y :: l) → Path ups (x :: y :: l)

/-- A dependency cycle: a walk of at least one edge that ends where it starts. -/
def IsCycle (ups : α → List α) (p : List α) : Prop :=
  Path ups p ∧ 2 ≤ p.length ∧ p.head? = p.getLast?

omit [DecidableEq α] in
theorem Path.ne_nil {ups : α → List α} {l : List α} (h : Path ups l) : l ≠ [] := by
  cases h <;> simp

omit [DecidableEq α] in
theorem Path.snoc {ups : α → List α} :
    ∀ {l : List α} {z y : α}, Path ups l → l.getLast? = some z → y ∈ ups z → Path ups (l ++ [y])
  | [x], z, y, _, hz, hy => by
    simp at hz; subst hz; exact Path.cons hy (Path.single y)
  | x :: x' :: l, z, y, hp, hz, hy => by
    cases hp with
    | cons hxy hrest =>
      have := Path.snoc hrest (by simpa using hz) hy
      exact Path.cons hxy (by simpa using this)

/-- A suffix of a walk that starts at `x`. -/
theorem Path.dropWhile {ups : α → List α} (x : α) :
    ∀ {l : List α}, Path ups l → x ∈ l →
      Path ups (l.dropWhile (fun a => decide (a ≠ x))) ∧
      (l.dropWhile (fun a => decide (a ≠ x))).head? = some x ∧
      (l.dropWhile (fun a => decide (a ≠ x))).getLast? = l.getLast?
  | [a], _, hx => by simp at hx; subst hx; simp [Path.single]
  | a :: b :: l, hp, hx => by
    by_cases hax : a = x
    · subst hax; simp [hp]
    · have hx' : x ∈ b :: l := by
        rcases List.mem_cons.mp hx with h | h
        · exact absurd h.symm hax
        · exact h
      cases hp with
      | cons _ hrest =>
        have := Path.dropWhile x hrest hx'
        have hd : (a :: b :: l).dropWhile (fun a => decide (a ≠ x)) =
            (b :: l).dropWhile (fun a => decide (a ≠ x)) := by simp [List.dropWhile_cons, hax]
        rw [hd]
        refine ⟨this.1, this.2.1, ?_⟩
        rw [this.2.2]; simp

omit [DecidableEq α] in
/-- Every element of a cycle reads another element of it. -/
theorem IsCycle.closed {ups : α → List α} {p : List α} (h : IsCycle ups p) : Closed ups p := by
  obtain ⟨hp, hlen, hends⟩ := h
  have step : ∀ {l : List α}, Path ups l → ∀ x ∈ l, l.getLast? = some x ∨ ∃ y ∈ ups x, y ∈ l := by
    intro l hl
    induction hl with
    | single x => intro y hy; simp at hy; simp [hy]
    | @cons x y l hxy _ ih =>
      intro z hz
      rcases List.mem_cons.mp hz with rfl | hz
      · exact Or.inr ⟨y, hxy, by simp⟩
      · rcases ih z hz with h | ⟨w, hw, hzw⟩
        · exact Or.inl (by simpa using h)
        · exact Or.inr ⟨w, hw, List.mem_cons_of_mem _ hzw⟩
  intro x hx
  rcases step hp x hx with hlast | h
  · -- the last element is the first, which reads the second
    cases hp with
    | single => simp at hlen
    | @cons a b l hab _ =>
      have : x = a := by
        rw [← hends] at hlast; simp at hlast; exact hlast.symm
      subst this
      exact ⟨b, hab, by simp⟩
  · exact h

/-- The successor `ModelOrder.findCycle` follows: the first upstream model in `s`. -/
def nextIn (ups : α → List α) (s : List α) (x : α) : Option α :=
  (ups x).find? fun u => decide (u ∈ s)

/--
Follow `next` from `cur` until a model repeats, then return the walk from its first visit.
Terminates because each step adds a model of `s` not yet on the walk.
-/
def walk (s : List α) (next : α → Option α) (path : List α) (cur : α) : Option (List α) :=
  if cur ∈ path then some (path.dropWhile (fun a => decide (a ≠ cur)) ++ [cur])
  else if _hs : cur ∈ s then
    match next cur with
    | some n => walk s next (path ++ [cur]) n
    | none => none
  else none
termination_by unvisited s path
decreasing_by exact unvisited_lt s path cur (by assumption) (by assumption)

/-- `ModelOrder.cycleIn`: a cycle through the models that no round could build. -/
def cycleIn (ups : α → List α) (s : List α) : Option (List α) :=
  match s with
  | [] => none
  | b :: _ => (nextIn ups s b).bind (walk s (nextIn ups s) [b])

theorem walk_some (s : List α) (next : α → Option α)
    (hnext : ∀ x ∈ s, ∃ y, next x = some y ∧ y ∈ s) (path : List α) (cur : α) (hcur : cur ∈ s) :
    ∃ p, walk s next path cur = some p := by
  fun_induction walk s next path cur with
  | case1 => exact ⟨_, rfl⟩
  | case2 path cur hin hs n hn ih =>
    obtain ⟨y, hy, hys⟩ := hnext cur hs
    rw [hn] at hy; cases hy
    exact ih hys
  | case3 path cur hin hs hn =>
    obtain ⟨y, hy, _⟩ := hnext cur hs
    rw [hn] at hy; cases hy
  | case4 => exact absurd hcur (by assumption)

theorem walk_cycle (ups : α → List α) (s : List α) (next : α → Option α)
    (hnext : ∀ x y, next x = some y → y ∈ ups x) (path : List α) (cur : α)
    (hpath : Path ups path) (hlast : ∀ z, path.getLast? = some z → cur ∈ ups z)
    (hsub : ∀ x ∈ path, x ∈ s) :
    ∀ p, walk s next path cur = some p → IsCycle ups p ∧ ∀ x ∈ p, x ∈ s := by
  fun_induction walk s next path cur with
  | case1 path cur hin =>
    intro p hp
    cases hp
    obtain ⟨hd, hhead, hdlast⟩ := Path.dropWhile cur hpath hin
    obtain ⟨z, hz⟩ : ∃ z, path.getLast? = some z := by
      cases h : path.getLast? with
      | none => simp at h; exact absurd h hpath.ne_nil
      | some z => exact ⟨z, rfl⟩
    refine ⟨⟨Path.snoc hd (hdlast.trans hz) (hlast z hz), ?_, ?_⟩, ?_⟩
    · have := hd.ne_nil
      cases h : path.dropWhile (fun a => decide (a ≠ cur)) with
      | nil => exact absurd h this
      | cons a l => simp
    · cases h : path.dropWhile (fun a => decide (a ≠ cur)) with
      | nil => exact absurd h hd.ne_nil
      | cons a l =>
        rw [h] at hhead; simp at hhead; subst hhead
        rw [List.getLast?_append]; simp
    · intro x hx
      rcases List.mem_append.mp hx with hx | hx
      · exact hsub x ((List.dropWhile_sublist _).subset hx)
      · simp at hx; subst hx; exact hsub _ hin
  | case2 path cur hin hs n hn ih =>
    apply ih
    · obtain ⟨z, hz⟩ : ∃ z, path.getLast? = some z := by
        cases h : path.getLast? with
        | none => simp at h; exact absurd h hpath.ne_nil
        | some z => exact ⟨z, rfl⟩
      exact Path.snoc hpath hz (hlast z hz)
    · intro z hz; simp at hz; subst hz; exact hnext _ _ hn
    · intro x hx
      rcases List.mem_append.mp hx with hx | hx
      · exact hsub x hx
      · simp at hx; subst hx; exact hs
  | case3 => intro p hp; cases hp
  | case4 => intro p hp; cases hp

theorem nextIn_spec (ups : α → List α) (s : List α) :
    ∀ x y, nextIn ups s x = some y → y ∈ ups x ∧ y ∈ s := by
  intro x y h
  exact ⟨List.mem_of_find?_eq_some h, by simpa using List.find?_some h⟩

/-- A closed nonempty set always yields a cycle, and the cycle stays inside the set. -/
theorem cycleIn_of_closed (ups : α → List α) (s : List α) (hne : s ≠ []) (hcl : Closed ups s) :
    ∃ p, cycleIn ups s = some p ∧ IsCycle ups p ∧ ∀ x ∈ p, x ∈ s := by
  have hnext : ∀ x ∈ s, ∃ y, nextIn ups s x = some y ∧ y ∈ s := by
    intro x hx
    obtain ⟨u, hu, hus⟩ := hcl x hx
    cases h : nextIn ups s x with
    | none =>
      exact absurd (by simpa using hus) ((List.find?_eq_none.mp h) u hu)
    | some y => exact ⟨y, rfl, (nextIn_spec ups s x y h).2⟩
  cases s with
  | nil => exact absurd rfl hne
  | cons b rest =>
    obtain ⟨c, hc, hcs⟩ := hnext b (by simp)
    obtain ⟨p, hp⟩ := walk_some _ _ hnext [b] c hcs
    refine ⟨p, by simp [cycleIn, hc, hp], ?_⟩
    exact walk_cycle ups _ _ (fun x y h => (nextIn_spec ups _ x y h).1) [b] c (Path.single b)
      (by intro z hz; simp at hz; subst hz; exact (nextIn_spec ups _ b c hc).1)
      (by simp) p hp

/-- Cycles and closed sets are the same thing. -/
theorem cycle_iff_closed (ups : α → List α) (univ : List α) :
    (∃ p, IsCycle ups p ∧ ∀ x ∈ p, x ∈ univ) ↔
      ∃ s, s ≠ [] ∧ (∀ x ∈ s, x ∈ univ) ∧ Closed ups s := by
  constructor
  · rintro ⟨p, hp, hpu⟩
    exact ⟨p, hp.1.ne_nil, hpu, hp.closed⟩
  · rintro ⟨s, hne, hsu, hcl⟩
    obtain ⟨p, -, hp, hps⟩ := cycleIn_of_closed ups s hne hcl
    exact ⟨p, hp, fun x hx => hsu x (hps x hx)⟩

/-- `ModelOrder.buildOrder`: the build order, or the dependency cycle that prevents it. -/
def buildOrder (ups : α → List α) (sel : List α) : Except (List α) (List α) :=
  match buildRounds ups sel with
  | .ok rs => .ok rs.flatten
  | .error b => .error ((cycleIn ups b).getD b)

/-- `ModelOrder.findCycle`: a dependency cycle among `models`, if any. -/
def findCycle (ups : α → List α) (models : List α) : Option (List α) :=
  match buildOrder ups models with
  | .ok _ => none
  | .error p => some p

/--
The build order of a selection: a topological permutation of the selection when the selection
has no cycle, and a real cycle among the selected models otherwise.
-/
theorem buildOrder_spec (ups : α → List α) (sel : List α) :
    (∀ o, buildOrder ups sel = .ok o → o.Perm sel ∧ Topological ups sel o) ∧
    (∀ p, buildOrder ups sel = .error p → IsCycle ups p ∧ ∀ x ∈ p, x ∈ sel) ∧
    ((∃ p, buildOrder ups sel = .error p) ↔ ∃ p, IsCycle ups p ∧ ∀ x ∈ p, x ∈ sel) := by
  have herr : ∀ b, buildRounds ups sel = .error b →
      ∃ p, cycleIn ups b = some p ∧ IsCycle ups p ∧ ∀ x ∈ p, x ∈ sel := by
    intro b hb
    obtain ⟨hne, hsub, hcl⟩ := rounds_error ups sel [] sel (fun x hx => Or.inr hx) b hb
    obtain ⟨p, hp, hc, hps⟩ := cycleIn_of_closed ups b hne hcl
    exact ⟨p, hp, hc, fun x hx => hsub.subset (hps x hx)⟩
  refine ⟨fun o ho => ?_, fun p hp => ?_, ?_⟩
  · unfold buildOrder at ho
    split at ho
    · rename_i rs hrs; cases ho; exact buildRounds_ok ups sel rs hrs
    · cases ho
  · unfold buildOrder at hp
    split at hp
    · cases hp
    · rename_i b hb
      cases hp
      obtain ⟨q, hq, hc, hqs⟩ := herr b hb
      rw [hq]; exact ⟨hc, hqs⟩
  · constructor
    · rintro ⟨p, hp⟩
      unfold buildOrder at hp
      split at hp
      · cases hp
      · rename_i b hb
        obtain ⟨q, -, hc, hqs⟩ := herr b hb
        exact ⟨q, hc, hqs⟩
    · intro hcyc
      obtain ⟨b, hb⟩ := (buildRounds_error_iff ups sel).mpr ((cycle_iff_closed ups sel).mp hcyc)
      exact ⟨(cycleIn ups b).getD b, by simp [buildOrder, hb]⟩

/-- Cycle detection is sound and complete: it reports a real cycle exactly when one exists. -/
theorem findCycle_spec (ups : α → List α) (models : List α) :
    (∀ p, findCycle ups models = some p → IsCycle ups p ∧ ∀ x ∈ p, x ∈ models) ∧
    (findCycle ups models = none ↔ ¬ ∃ p, IsCycle ups p ∧ ∀ x ∈ p, x ∈ models) := by
  obtain ⟨_, herr, hiff⟩ := buildOrder_spec ups models
  refine ⟨fun p hp => ?_, ?_⟩
  · unfold findCycle at hp
    split at hp
    · cases hp
    · rename_i q hq; cases hp; exact herr _ hq
  · rw [← hiff]
    unfold findCycle
    split <;> simp_all

/--
Once a project passes cycle detection, no selection fails to order: every subset of the models
gets a topological build order.
-/
theorem buildOrder_ok_of_acyclic (ups : α → List α) (models sel : List α)
    (hacyclic : findCycle ups models = none) (hsel : ∀ x ∈ sel, x ∈ models) :
    ∃ o, buildOrder ups sel = .ok o := by
  have hno := (findCycle_spec ups models).2.mp hacyclic
  cases h : buildOrder ups sel with
  | ok o => exact ⟨o, rfl⟩
  | error p =>
    obtain ⟨hc, hps⟩ := (buildOrder_spec ups sel).2.1 p h
    exact absurd ⟨p, hc, fun x hx => hsel x (hps x hx)⟩ hno

end Wvlet.ModelBuild
