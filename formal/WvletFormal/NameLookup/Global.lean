/-
Licensed under the Apache License, Version 2.0.

The global symbol index (`GlobalSymbolIndex` in
wvlet-lang/src/main/scala/wvlet/lang/compiler/GlobalSymbolIndex.scala): which top-level definitions
a compilation unit sees (`visibleEntries`), in which order, and the import-by-name fallback
(`findFirstAnywhere`).
-/
import WvletFormal.NameLookup.Scope

namespace Wvlet.NameLookup

/-- A package path: `[]` is the default package, `["q", "r"]` is `q.r`. -/
abbrev Pkg := List String

/--
A compilation unit as the index sees it. `file` ranks the source file basename (`f0.wv` sorts
before `f1.wv`; two units may share a basename in different folders). `pkg = none` marks a preset
(standard library) unit, which every package sees. `defs` are the unit's top-level term names in
registration order.
-/
structure Unit where
  file : Nat
  pkg : Option Pkg
  defs : List Name
deriving Repr

/-- A top-level definition: the `idx`-th definition of unit `unit`. -/
structure GEntry where
  unit : Nat
  idx : Nat
  file : Nat
  pkg : Option Pkg
  name : Name
deriving Repr, DecidableEq

/-- Every definition, in registration order (units in list order, definitions in order). -/
def entriesOf (units : List Unit) : List GEntry :=
  units.zipIdx.flatMap fun (u, ui) => u.defs.zipIdx.map fun (n, di) => ⟨ui, di, u.file, u.pkg, n⟩

/-- `PackageIndex.get`: the definitions of a name in one bucket, newest first. -/
def bucket (units : List Unit) (b : Option Pkg) (n : Name) : List GEntry :=
  ((entriesOf units).filter fun e => e.pkg == b && e.name == n).reverse

/-- An import of `a.b.c` makes `a.b.c`, `a.b`, and `a` visible, in that order. -/
def prefixes (r : Pkg) : List Pkg := (List.range r.length).reverse.map fun k => r.take (k + 1)

/-- `List.distinct`: keep the first occurrence of each element. -/
def dedup {α : Type} [DecidableEq α] (l : List α) : List α :=
  l.foldl (fun acc x => if x ∈ acc then acc else acc ++ [x]) []

/-- The packages `visibleEntries` consults: the default package, the unit's own package, imports. -/
def visiblePackages (cur : Pkg) (imports : List Pkg) : List Pkg :=
  dedup ([[]] ++ (if cur.isEmpty then [] else [cur]) ++ imports.flatMap prefixes)

/-- `entriesIn`: the preset bucket, then the bucket of each given package. -/
def entriesIn (units : List Unit) (n : Name) (pkgs : List Pkg) : List GEntry :=
  bucket units none n ++ pkgs.flatMap fun p => bucket units (some p) n

/-- Stable insertion by basename: `e` goes before the first entry whose basename is not smaller. -/
def insertByFile (e : GEntry) : List GEntry → List GEntry
  | [] => [e]
  | y :: ys => if y.file < e.file then y :: insertByFile e ys else e :: y :: ys

/-- `sortedByFileName`: a stable sort by basename, as Scala's `sortBy`. -/
def sortByFile : List GEntry → List GEntry
  | [] => []
  | e :: es => insertByFile e (sortByFile es)

/-- `GlobalSymbolIndex.visibleEntries`. Its head is the definition `findSymbolByName` uses. -/
def visibleEntries (units : List Unit) (n : Name) (cur : Pkg) (imports : List Pkg) : List GEntry :=
  sortByFile (entriesIn units n (visiblePackages cur imports))

/--
`findFirstAnywhere` ignores package visibility and takes the first definition with the smallest
basename, iterating package buckets in hash-map order. The model keeps every definition it may
pick: those of the name with the smallest basename.
-/
def anywhereCandidates (units : List Unit) (n : Name) : List GEntry :=
  let es := (entriesOf units).filter (·.name == n)
  es.filter fun e => es.all fun y => e.file ≤ y.file

/-! ## Sorting -/

theorem mem_insertByFile (e x : GEntry) (l : List GEntry) :
    x ∈ insertByFile e l ↔ x = e ∨ x ∈ l := by
  induction l with
  | nil => simp [insertByFile]
  | cons y ys ih =>
    simp only [insertByFile]; split
    · simp only [List.mem_cons, ih]
      constructor
      · rintro (h | h | h) <;> simp [h]
      · rintro (h | h | h) <;> simp [h]
    · simp only [List.mem_cons]

theorem mem_sortByFile (x : GEntry) (l : List GEntry) : x ∈ sortByFile l ↔ x ∈ l := by
  induction l with
  | nil => simp [sortByFile]
  | cons e es ih => simp [sortByFile, mem_insertByFile, ih]

theorem insertByFile_head (e : GEntry) (l : List GEntry) :
    ∃ w, (insertByFile e l).head? = some w ∧ w.file ≤ e.file ∧
      ∀ h, l.head? = some h → w.file ≤ h.file := by
  cases l with
  | nil => exact ⟨e, rfl, Nat.le_refl _, by simp⟩
  | cons y ys =>
    simp only [insertByFile]; split
    · rename_i hlt
      cases ys with
      | nil => exact ⟨y, rfl, Nat.le_of_lt hlt, by simp⟩
      | cons z zs =>
        refine ⟨y, ?_, Nat.le_of_lt hlt, by simp⟩
        cases h : insertByFile e (z :: zs) <;> simp
    · rename_i hge
      exact ⟨e, rfl, Nat.le_refl _, by simp; omega⟩

/-- The head of a sorted list has the smallest basename. -/
theorem sortByFile_head_le (l : List GEntry) (w : GEntry) (hw : (sortByFile l).head? = some w) :
    ∀ e ∈ l, w.file ≤ e.file := by
  induction l generalizing w with
  | nil => simp [sortByFile] at hw
  | cons e es ih =>
    obtain ⟨w', hw', hle, hhead⟩ := insertByFile_head e (sortByFile es)
    simp only [sortByFile] at hw
    rw [hw] at hw'; cases hw'
    intro x hx
    simp only [List.mem_cons] at hx
    rcases hx with rfl | hx
    · exact hle
    · cases hs : (sortByFile es).head? with
      | none =>
        have : sortByFile es = [] := by cases h : sortByFile es <;> simp_all
        have := (mem_sortByFile x es).2 hx; simp_all
      | some h => exact Nat.le_trans (hhead h hs) (ih h hs x hx)

/-! ## Visibility -/

theorem mem_dedup {α : Type} [DecidableEq α] (l : List α) (x : α) : x ∈ dedup l ↔ x ∈ l := by
  suffices ∀ acc : List α, x ∈ l.foldl (fun acc x => if x ∈ acc then acc else acc ++ [x]) acc ↔
      x ∈ acc ∨ x ∈ l by unfold dedup; simpa using this []
  induction l with
  | nil => simp
  | cons y ys ih =>
    intro acc
    simp only [List.foldl_cons]; rw [ih]; split
    · rename_i hy
      simp only [List.mem_cons]
      constructor
      · rintro (h | h)
        · exact Or.inl h
        · exact Or.inr (Or.inr h)
      · rintro (h | rfl | h)
        · exact Or.inl h
        · exact Or.inl hy
        · exact Or.inr h
    · simp only [List.mem_append, List.mem_cons, List.not_mem_nil, false_or, or_assoc]

theorem mem_prefixes (r p : Pkg) : p ∈ prefixes r ↔ ∃ k, k < r.length ∧ p = r.take (k + 1) := by
  simp [prefixes, eq_comm]

/--
The visible packages: the default package, the unit's own package, and every non-empty prefix of
each import reference. Nothing else, in particular no sub-package of an imported package.
-/
theorem mem_visiblePackages (cur : Pkg) (imports : List Pkg) (p : Pkg) :
    p ∈ visiblePackages cur imports ↔
      p = [] ∨ (p = cur) ∨ ∃ r ∈ imports, ∃ k, k < r.length ∧ p = r.take (k + 1) := by
  unfold visiblePackages
  rw [mem_dedup]
  simp only [List.mem_append, List.mem_singleton, List.mem_flatMap, mem_prefixes]
  constructor
  · rintro ((h | h) | h)
    · exact Or.inl h
    · split at h
      · simp at h
      · simp at h; exact Or.inr (Or.inl h)
    · exact Or.inr (Or.inr h)
  · rintro (h | h | h)
    · exact Or.inl (Or.inl h)
    · by_cases hc : cur.isEmpty
      · left; left; simp at hc; rw [h, hc]
      · left; right; simp [hc, h]
    · exact Or.inr h

theorem mem_bucket (units : List Unit) (b : Option Pkg) (n : Name) (e : GEntry) :
    e ∈ bucket units b n ↔ e ∈ entriesOf units ∧ e.pkg = b ∧ e.name = n := by
  simp [bucket]

/--
A definition is visible exactly when it has the name and is a preset definition or lives in a
visible package.
-/
theorem mem_entriesIn (units : List Unit) (n : Name) (pkgs : List Pkg) (e : GEntry) :
    e ∈ entriesIn units n pkgs ↔
      e ∈ entriesOf units ∧ e.name = n ∧ (e.pkg = none ∨ ∃ p ∈ pkgs, e.pkg = some p) := by
  simp only [entriesIn, List.mem_append, List.mem_flatMap, mem_bucket]
  constructor
  · rintro (⟨h1, h2, h3⟩ | ⟨p, hp, h1, h2, h3⟩)
    · exact ⟨h1, h3, Or.inl h2⟩
    · exact ⟨h1, h3, Or.inr ⟨p, hp, h2⟩⟩
  · rintro ⟨h1, h3, h2 | ⟨p, hp, h2⟩⟩
    · exact Or.inl ⟨h1, h2, h3⟩
    · exact Or.inr ⟨p, hp, h1, h2, h3⟩

/-! ## The definition `findSymbolByName` uses -/

/-- The winner is a visible definition with the smallest basename among the visible ones. -/
theorem visibleEntries_head (units : List Unit) (n : Name) (cur : Pkg) (imports : List Pkg)
    (w : GEntry) (hw : (visibleEntries units n cur imports).head? = some w) :
    w ∈ entriesIn units n (visiblePackages cur imports) ∧
      ∀ e ∈ entriesIn units n (visiblePackages cur imports), w.file ≤ e.file := by
  refine ⟨?_, sortByFile_head_le _ w hw⟩
  rw [← mem_sortByFile]; exact List.mem_of_mem_head? hw

/--
Order independence: when one visible definition has a strictly smallest basename, it wins,
whatever order the units were registered in. Registration order (newest first) breaks ties only
among definitions with the same basename: in one file, or in same-named files of different
folders.
-/
theorem visibleEntries_unique_min (units : List Unit) (n : Name) (cur : Pkg)
    (imports : List Pkg) (w : GEntry) (hw : w ∈ entriesIn units n (visiblePackages cur imports))
    (hmin : ∀ e ∈ entriesIn units n (visiblePackages cur imports), e ≠ w → w.file < e.file) :
    (visibleEntries units n cur imports).head? = some w := by
  cases h : (visibleEntries units n cur imports).head? with
  | none =>
    have : visibleEntries units n cur imports = [] := by
      cases h' : visibleEntries units n cur imports <;> simp_all
    have := (mem_sortByFile w _).2 hw
    simp_all [visibleEntries]
  | some v =>
    obtain ⟨hv, hle⟩ := visibleEntries_head units n cur imports v h
    by_cases hvw : v = w
    · rw [hvw]
    · have := hmin v hv hvw; have := hle w hw; omega

/-- `findFirstAnywhere` picks a definition of the name with the smallest basename. -/
theorem mem_anywhereCandidates (units : List Unit) (n : Name) (e : GEntry) :
    e ∈ anywhereCandidates units n ↔
      e ∈ entriesOf units ∧ e.name = n ∧
        ∀ y ∈ entriesOf units, y.name = n → e.file ≤ y.file := by
  simp only [anywhereCandidates, List.mem_filter, List.all_eq_true, decide_eq_true_eq, beq_iff_eq]
  constructor
  · rintro ⟨⟨h1, h2⟩, h3⟩; exact ⟨h1, h2, fun y hy hn => h3 y ⟨hy, hn⟩⟩
  · rintro ⟨h1, h2, h3⟩; exact ⟨⟨h1, h2⟩, fun y ⟨hy, hn⟩ => h3 y hy hn⟩

end Wvlet.NameLookup
