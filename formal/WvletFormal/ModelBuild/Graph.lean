/-
Licensed under the Apache License, Version 2.0.

The model graph of `wvlet build` (`ModelOrder` in
wvlet-lang/src/main/scala/wvlet/lang/compiler/pipeline/ModelOrder.scala): edges derived through
ephemeral models (`upstreamsOf`), upstream and downstream closures (`closure`), and dbt-style
selectors (`parseSelectors`, `select`).
-/
import WvletFormal.ModelBuild.Explore

namespace Wvlet.ModelBuild

variable {α : Type} [DecidableEq α]

/-! ## Edges through ephemeral models -/

/--
`ModelOrder.upstreamsOf`: the materialized models a model reads, given its direct references.
`defs` are the defined model names, `refs` the direct references of each defined model. The work
list is breadth-first; ephemeral models are expanded, materialized models and unknown names are
leaves. The model's own name is not pre-visited, so a model that reads itself (directly or through
ephemeral models) gets a self-edge.
-/
def upstreamsOf (defs : List α) (isMat : α → Bool) (refs : α → List α) (direct : List α) :
    List α :=
  (explore defs (fun n => !isMat n) refs (fun new rest => rest ++ new) direct []).filter isMat

/-- Reading through an ephemeral model: `x` is a defined ephemeral model that references `y`. -/
def ReadsVia (defs : List α) (isMat : α → Bool) (refs : α → List α) (x y : α) : Prop :=
  Step defs (fun n => !isMat n) refs x y

/--
The edges are exactly the materialized models reachable from a direct reference through ephemeral
models only.
-/
theorem mem_upstreamsOf (defs : List α) (isMat : α → Bool) (refs : α → List α)
    (direct : List α) (y : α) :
    y ∈ upstreamsOf defs isMat refs direct ↔
      isMat y = true ∧ ∃ s ∈ direct, Star (ReadsVia defs isMat refs) s y := by
  unfold upstreamsOf ReadsVia
  rw [List.mem_filter, mem_explore_iff _ _ _ _ mem_back_push]
  exact And.comm

theorem upstreamsOf_nodup (defs : List α) (isMat : α → Bool) (refs : α → List α)
    (direct : List α) : (upstreamsOf defs isMat refs direct).Nodup :=
  List.Sublist.nodup List.filter_sublist (explore_nodup _ _ _ _ _ _ List.nodup_nil)

/-! ## The graph of materialized models -/

/-- Materialized models in definition order, and the upstream edges of each. -/
structure Graph (α : Type) where
  models : List α
  ups : α → List α

/-- Edges point to models of the graph (what `ModelBuildPlanner.graph` derives). -/
def Graph.WellFormed (g : Graph α) : Prop := ∀ x ∈ g.models, ∀ y ∈ g.ups x, y ∈ g.models

/-- `x` reads `y` directly. -/
def Graph.Reads (g : Graph α) (x y : α) : Prop := x ∈ g.models ∧ y ∈ g.models ∧ y ∈ g.ups x

/-- `ModelGraph.downstreams`: the models reading `m` directly, in definition order. -/
def Graph.downs (g : Graph α) (m : α) : List α := g.models.filter (fun d => decide (m ∈ g.ups d))

/-- `ModelOrder.closure`: depth-first, so new nodes are pushed in front. -/
def closure (univ : List α) (next : α → List α) (start : α) : List α :=
  explore univ (fun _ => true) next (· ++ ·) [start] []

theorem mem_closure (univ : List α) (next : α → List α) (start x : α) :
    x ∈ closure univ next start ↔ Star (Step univ (fun _ => true) next) start x := by
  unfold closure
  rw [mem_explore_iff _ _ _ _ mem_front_push]
  simp

/-- Upstream closure: `start` and every model it reads, transitively. -/
theorem mem_closure_ups (g : Graph α) (hw : g.WellFormed) (start x : α) : x ∈ closure g.models g.ups start ↔ Star g.Reads start x := by
  rw [mem_closure]
  constructor
  · intro h
    induction h with
    | refl => exact Star.refl _
    | tail h1 hstep ih =>
      exact Star.tail ih ⟨hstep.2.1, hw _ hstep.2.1 _ hstep.2.2, hstep.2.2⟩
  · intro h
    exact Star.mono (fun x y hr => ⟨rfl, hr.1, hr.2.2⟩) h

/-- Downstream closure: `start` and every model reading it, transitively. -/
theorem mem_closure_downs (g : Graph α) (start x : α) :
    x ∈ closure g.models g.downs start ↔ Star g.Reads x start := by
  rw [mem_closure]
  constructor
  · intro h
    have : Star (fun a b => g.Reads b a) start x := by
      refine Star.mono (fun a b hr => ?_) h
      have hb := List.mem_filter.mp hr.2.2
      exact ⟨hb.1, hr.2.1, by simpa using hb.2⟩
    have hf := this.flip
    exact Star.mono (fun a b hr => hr) hf
  · intro h
    have hf := h.flip
    refine Star.mono (fun a b hr => ?_) hf
    exact ⟨rfl, hr.2.1, List.mem_filter.mpr ⟨hr.1, by simpa using hr.2.2⟩⟩

/-! ## Selectors -/

/-- A parsed selector: `+name` adds upstream models, `name+` downstream models. -/
structure Selector (α : Type) where
  name : α
  up : Bool
  down : Bool
  deriving DecidableEq, Repr

/-- Why `select` fails. -/
inductive SelectError (α : Type) where
  /-- The selector names no materialized model. -/
  | notFound (name : α)
  /-- Selectors were given but none survived splitting on commas. -/
  | empty
  deriving DecidableEq, Repr

/-- The models one selector adds. -/
def selectOne (g : Graph α) (s : Selector α) : List α :=
  s.name :: ((if s.up then closure g.models g.ups s.name else []) ++
    (if s.down then closure g.models g.downs s.name else []))

/-- `ModelOrder.select` on parsed selectors. An empty list selects every model. -/
def selectParsed (g : Graph α) (sels : List (Selector α)) : Except (SelectError α) (List α) :=
  match sels.find? (fun s => decide (s.name ∉ g.models)) with
  | some s => .error (.notFound s.name)
  | none => .ok (g.models.filter (fun m => sels.any (fun s => decide (m ∈ selectOne g s))))

/-- What a selector selects, as the selector syntax defines it. -/
def Selects (g : Graph α) (s : Selector α) (m : α) : Prop :=
  m = s.name ∨ (s.up = true ∧ Star g.Reads s.name m) ∨ (s.down = true ∧ Star g.Reads m s.name)

/--
`select` returns, in definition order and without duplicates, exactly the models some selector
selects: the named model, plus every model it reads transitively for `+m`, plus every model
reading it transitively for `m+`. An unknown name is an error.
-/
theorem selectParsed_spec (g : Graph α) (hw : g.WellFormed) (hn : g.models.Nodup)
    (sels : List (Selector α)) :
    (∀ r, selectParsed g sels = .ok r →
      r.Sublist g.models ∧ r.Nodup ∧
        ∀ m, m ∈ r ↔ m ∈ g.models ∧ ∃ s ∈ sels, Selects g s m) ∧
    (∀ e, selectParsed g sels = .error e → ∃ s ∈ sels, e = .notFound s.name ∧ s.name ∉ g.models) := by
  unfold selectParsed
  split
  · rename_i s hs
    refine ⟨fun r h => (by cases h), fun e h => ?_⟩
    cases h
    exact ⟨s, List.mem_of_find?_eq_some hs, rfl, by simpa using List.find?_some hs⟩
  · rename_i hnone
    have hall : ∀ s ∈ sels, s.name ∈ g.models := by
      intro s hs
      have := (List.find?_eq_none.mp hnone) s hs
      simpa using this
    refine ⟨fun r h => ?_, fun e h => (by cases h)⟩
    cases h
    refine ⟨List.filter_sublist, List.Sublist.nodup List.filter_sublist hn, fun m => ?_⟩
    rw [List.mem_filter]
    simp only [List.any_eq_true, decide_eq_true_eq]
    apply and_congr_right
    intro hm
    apply exists_congr; intro s; apply and_congr_right; intro hs
    unfold selectOne Selects
    simp only [List.mem_cons, List.mem_append]
    apply or_congr_right
    apply or_congr
    · cases s.up <;> simp [mem_closure_ups g hw]
    · cases s.down <;> simp [mem_closure_downs g]

/-! ## Selector parsing (the string layer, replayed by the fixture rather than proved) -/

/-- Java's `String.trim`: drop leading and trailing characters up to U+0020. -/
def trimChars (cs : List Char) : List Char :=
  let isSpace := fun (c : Char) => decide (c.toNat ≤ 32)
  ((cs.dropWhile isSpace).reverse.dropWhile isSpace).reverse

/-- Parse one selector token: one leading `+` and one trailing `+` are stripped. -/
def parseToken (cs : List Char) : Selector String :=
  let up := cs.head? == some '+'
  let down := cs.getLast? == some '+' && decide (cs.length > 1)
  let n1 := if up then cs.tail else cs
  let n2 := if n1.getLast? == some '+' then n1.dropLast else n1
  ⟨String.ofList n2, up, down⟩

/-- `ModelOrder.parseSelectors`: split each argument on commas, trim, and drop empty tokens. -/
def parseSelectors (args : List String) : List (Selector String) :=
  (args.flatMap fun a => (a.toList.splitOn ',').map trimChars).filter (· ≠ []) |>.map parseToken

/--
`ModelOrder.select`: no arguments select every model; arguments that parse to no selector are an
error, not an empty build.
-/
def select (g : Graph String) (args : List String) : Except (SelectError String) (List String) :=
  if args.isEmpty then .ok g.models
  else
    let sels := parseSelectors args
    if sels.isEmpty then .error .empty else selectParsed g sels

end Wvlet.ModelBuild
