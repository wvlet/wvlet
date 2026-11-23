/-
Licensed under the Apache License, Version 2.0.

How the engine decorrelates a correlated scalar subquery (wvlet-engine's
`Planner.decorrelateScalar`), and the proof that every outer row gets SQL's value of the subquery,
NULL keys, duplicate outer rows, and groups without rows included.

The subquery is `from rs where i1 = o1 and ... and in = on and lws select agg`, evaluated for each
outer row: the aggregate over the inner rows for which its WHERE is TRUE. The correlated conditions
must be equalities `inner = outer` (the planner rejects others); the conditions over the
subquery's own columns (`lws`) filter its input, as `planCorrelated` splits them
(`Decorrelate.holds_split`).

The plan groups the filtered body by the inner keys (GROUP BY: NULL keys form a group of their
own), left-joins the outer input with the groups on the keys (NULL never matches), and, when the
subquery's value over no rows is not NULL (`count` = 0), replaces the value of the outer rows
whose first joined key is NULL, the unmatched ones, by that value (`emptyGroupValue`).

The aggregate is any function of the group's rows (`Agg`), so `count`, `sum`, `max`, and
expressions over aggregate calls (`case when count(*) > 0 then ... end`) are all instances; its
value over no rows is `agg []`.
-/
import WvletFormal.Decorrelate.Rule

namespace Wvlet.Scalar

open Wvlet.Algebra
open Wvlet.Decorrelate
open Wvlet.Rewrite (Value Tri)

/-- The subquery's value as a function of the rows it aggregates. -/
abbrev Agg := Rel → Value

/-! ### SQL's meaning -/

/-- `(from rs where w select agg)` for the outer row `x`: the aggregate over the inner rows that
make `w` TRUE. -/
def scalarSql (L : List Col) (w : Cond) (agg : Agg) (x : Row) (rs : Rel) : Value :=
  agg (rs.filter fun y => w.holds (Row.merge L x y))

/-! ### The plan -/

/-- GROUP BY the inner keys: the body rows with the key `d`. NULL keys are equal here, so the rows
with a NULL key form a group too. -/
def group (ks : Keys) (body : Rel) (d : List Value) : Rel :=
  body.filter fun y => decide (innerKey ks y = d)

/-- The left join of the outer rows with the groups on `outer key = inner key`: for each outer
row, its matching group (the row, the group's key, and its value), or one row padded with NULL
keys and a NULL value. -/
def leftJoined (ks : Keys) (agg : Agg) (body ls : Rel) : List (Row × List Value × Value) :=
  let ds := dedup (body.map (innerKey ks))
  ls.flatMap fun x =>
    let m := ds.filter (keysMatch (outerKey ks x))
    if m.isEmpty then [(x, ks.map fun _ => .null, .null)]
    else m.map fun d => (x, d, agg (group ks body d))

/-- `CASE WHEN key IS NULL THEN empty ELSE v END` over the first joined key. -/
def fixEmpty (e : Value) : Row × List Value × Value → Row × Value
  | (x, k, v) => (x, if k.headD .null = .null then e else v)

/-- The joined rows without the projection: the left join's value column as is. -/
def noFix : Row × List Value × Value → Row × Value
  | (x, _, v) => (x, v)

/-- `decorrelateScalar`: the projection only when the value over no rows is not NULL. -/
def planScalar (ks : Keys) (agg : Agg) (body ls : Rel) : List (Row × Value) :=
  if agg [] = .null then (leftJoined ks agg body ls).map noFix
  else (leftJoined ks agg body ls).map (fixEmpty (agg []))

/-! ### Soundness -/

/-- The distinct keys matching `k`: `k` itself when a body row matches it, else none. -/
theorem matches_eq (ks : Keys) (body : Rel) (k : List Value) :
    (dedup (body.map (innerKey ks))).filter (keysMatch k) =
      if body.any (fun y => keysMatch k (innerKey ks y)) then [k] else [] := by
  have hfilt : (dedup (body.map (innerKey ks))).filter (keysMatch k) =
      (dedup (body.map (innerKey ks))).filter (fun d => decide (d = k) && keysMatch k k) := by
    congr 1; funext d; exact keysMatch_iff k d
  have hany : (body.any fun y => keysMatch k (innerKey ks y)) =
      (decide (k ∈ body.map (innerKey ks)) && keysMatch k k) := by
    apply Bool.eq_iff_iff.mpr
    simp only [List.any_eq_true, Bool.and_eq_true, decide_eq_true_eq, List.mem_map]
    constructor
    · rintro ⟨y, hy, hm⟩
      have := keysMatch_eq hm
      exact ⟨⟨y, hy, this⟩, this ▸ hm⟩
    · rintro ⟨⟨y, hy, rfl⟩, hm⟩; exact ⟨y, hy, hm⟩
  rw [hfilt, hany]
  by_cases hkk : keysMatch k k = true
  · simp only [hkk, Bool.and_true]
    rw [filter_eq_dedup]
    by_cases hm : k ∈ body.map (innerKey ks) <;> simp [hm]
  · simp only [Bool.not_eq_true] at hkk
    simp [hkk]

/-- A matched key's group is the body rows whose keys are TRUE-equal to it. -/
theorem group_eq (ks : Keys) (body : Rel) (k : List Value) (hkk : keysMatch k k = true) :
    group ks body k = body.filter fun y => keysMatch k (innerKey ks y) := by
  unfold group
  congr 1; funext y
  rw [keysMatch_iff, hkk, Bool.and_true]

/-- One outer row of the left join: its group's row when a body row matches its key, else the
padded row. -/
theorem joinedRow (ks : Keys) (agg : Agg) (body : Rel) (x : Row) :
    (let m := (dedup (body.map (innerKey ks))).filter (keysMatch (outerKey ks x))
     if m.isEmpty then [(x, ks.map fun _ => Value.null, Value.null)]
     else m.map fun d => (x, d, agg (group ks body d))) =
      if body.any (fun y => keysMatch (outerKey ks x) (innerKey ks y)) then
        [(x, outerKey ks x, agg (body.filter fun y => keysMatch (outerKey ks x) (innerKey ks y)))]
      else [(x, ks.map fun _ => Value.null, Value.null)] := by
  simp only
  rw [matches_eq]
  by_cases h : body.any (fun y => keysMatch (outerKey ks x) (innerKey ks y)) = true
  · have hkk : keysMatch (outerKey ks x) (outerKey ks x) = true := by
      obtain ⟨y, _, hm⟩ := List.any_eq_true.mp h
      have := keysMatch_eq hm
      rwa [this] at hm
    simp [h, group_eq ks body _ hkk]
  · simp only [Bool.not_eq_true] at h
    simp [h]

/-- Without a matching body row, the filtered rows are none. -/
theorem filter_none {p : Row → Bool} {body : Rel} (h : body.any p = false) : body.filter p = [] := by
  rw [List.filter_eq_nil_iff]
  intro y hy hp
  have : body.any p = true := List.any_eq_true.mpr ⟨y, hy, hp⟩
  simp_all

/-- SQL's value is the aggregate over the filtered body's rows whose keys match. -/
theorem scalarSql_split (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ks : Keys)
    (hk : ∀ k ∈ ks, (∀ c ∈ k.1.reads, c ∈ L) ∧ (∀ c ∈ k.2.reads, c ∈ R)) (lws : List Cond)
    (hl : ∀ w ∈ lws, readsOnly w R = true) (agg : Agg) (x : Row) (rs : Rel) :
    scalarSql L (andAll (keyConds ks ++ lws)) agg x rs =
      agg ((filter (andAll lws) rs).filter fun y => keysMatch (outerKey ks x) (innerKey ks y)) := by
  unfold scalarSql filter
  rw [List.filter_filter]
  congr 2
  funext y
  rw [holds_andAll, List.all_append, all_holds_merge_right L R disj lws hl x y,
    ← holds_andAll, holds_keyConds L R disj ks hk x y, holds_andAll]

/-- One outer row of the plan: itself with SQL's value, with or without the projection. -/
theorem planRow (ks : Keys) (hne : ks ≠ []) (agg : Agg) (body : Rel) (x : Row) (fix : Bool)
    (hfix : fix = false → agg [] = .null) :
    (if body.any (fun y => keysMatch (outerKey ks x) (innerKey ks y)) then
        [(x, outerKey ks x, agg (body.filter fun y => keysMatch (outerKey ks x) (innerKey ks y)))]
      else [(x, ks.map fun _ => Value.null, Value.null)]).map
        (if fix then fixEmpty (agg []) else noFix) =
      [(x, agg (body.filter fun y => keysMatch (outerKey ks x) (innerKey ks y)))] := by
  obtain ⟨⟨o, i⟩, ks', rfl⟩ := List.exists_cons_of_ne_nil hne
  by_cases h : body.any (fun y => keysMatch (outerKey ((o, i) :: ks') x) (innerKey ((o, i) :: ks') y))
      = true
  · -- matched: the first key is not NULL, so the projection keeps the group's value
    have hkk : keysMatch (outerKey ((o, i) :: ks') x) (outerKey ((o, i) :: ks') x) = true := by
      obtain ⟨y, _, hm⟩ := List.any_eq_true.mp h
      have := keysMatch_eq hm
      rwa [this] at hm
    have h0 : o.eval x ≠ .null := keysMatch_head (by simpa [outerKey] using hkk)
    simp only [h, ite_true]
    cases fix <;> simp [fixEmpty, noFix, outerKey, h0]
  · -- unmatched: no rows, so SQL's value is the value over no rows
    simp only [Bool.not_eq_true] at h
    simp only [h, Bool.false_eq_true, ite_false]
    rw [filter_none h]
    cases fix
    · simp [noFix, hfix rfl]
    · simp [fixEmpty]

/-- The decorrelated plan keeps every outer row once, in order, with SQL's value of the
subquery. The subquery's WHERE is the key equalities `outer = inner` (at least one) and conditions
over its own columns `lws`, which filter the body. -/
theorem planScalar_sound (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ks : Keys) (hne : ks ≠ [])
    (hk : ∀ k ∈ ks, (∀ c ∈ k.1.reads, c ∈ L) ∧ (∀ c ∈ k.2.reads, c ∈ R)) (lws : List Cond)
    (hl : ∀ w ∈ lws, readsOnly w R = true) (agg : Agg) (ls rs : Rel) :
    planScalar ks agg (filter (andAll lws) rs) ls =
      ls.map fun x => (x, scalarSql L (andAll (keyConds ks ++ lws)) agg x rs) := by
  have key : ∀ fix : Bool, (fix = false → agg [] = .null) →
      (leftJoined ks agg (filter (andAll lws) rs) ls).map
          (if fix then fixEmpty (agg []) else noFix) =
        ls.map fun x => (x, scalarSql L (andAll (keyConds ks ++ lws)) agg x rs) := by
    intro fix hfix
    unfold leftJoined
    induction ls with
    | nil => rfl
    | cons x xs ih =>
      rw [List.flatMap_cons, List.map_append, ih, joinedRow, planRow ks hne agg _ x fix hfix,
        List.map_cons, scalarSql_split L R disj ks hk lws hl agg x rs]
      rfl
  unfold planScalar
  by_cases he : agg [] = .null
  · simpa [he] using key false (fun _ => he)
  · simpa [he] using key true (fun h => nomatch h)

/-- So any condition over an outer row and the value (the subquery in a WHERE) keeps SQL's
rows. -/
theorem scalarFilter_sound (L R : List Col) (disj : ∀ c ∈ R, c ∉ L) (ks : Keys) (hne : ks ≠ [])
    (hk : ∀ k ∈ ks, (∀ c ∈ k.1.reads, c ∈ L) ∧ (∀ c ∈ k.2.reads, c ∈ R)) (lws : List Cond)
    (hl : ∀ w ∈ lws, readsOnly w R = true) (agg : Agg) (f : Row → Value → Tri) (ls rs : Rel) :
    ((planScalar ks agg (filter (andAll lws) rs) ls).filter (fun (x, v) => f x v == .true)).map
        (·.1) =
      ls.filter fun x => f x (scalarSql L (andAll (keyConds ks ++ lws)) agg x rs) == .true := by
  rw [planScalar_sound L R disj ks hne hk lws hl, List.filter_map, List.map_map]
  induction ls with
  | nil => rfl
  | cons x xs ih =>
    by_cases h : f x (scalarSql L (andAll (keyConds ks ++ lws)) agg x rs) == .true <;>
      simp_all [Function.comp_def]

end Wvlet.Scalar
