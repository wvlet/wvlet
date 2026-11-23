/-
Licensed under the Apache License, Version 2.0.

A core of relational algebra over bags with SQL three-valued logic, shared by the models of the
engine's NULL-sensitive optimizer rules.

A row maps column ids to SQL values (`Rewrite.Value`); a relation is a list of rows, so it keeps
duplicates (a bag) and an order that the rules never rely on. A condition evaluates to a `Tri`
(Kleene logic), and a filter keeps only the rows where it is TRUE: FALSE and unknown (NULL) both
drop the row.

`a IN (v1, ..., vn)` over constants (`Cond.inList`, the engine's `InList`) has the value of
`inTri` below, so a NULL among the constants makes a non-matching row unknown.

Joins take the column ids of their left input (`L`) and a general ON condition, evaluated on the
joined row, so equality keys are a special case. A joined row reads `L` from the left row and
every other column from the right row; outer joins pad the missing side with `Row.null`, a row
whose every column is NULL. Semi and anti joins keep left rows with (without) a TRUE match: the
latter is `NOT EXISTS`, not the NULL-aware anti join of `NOT IN`.

`x IN (subquery)` is the Kleene OR of `x = y` over the subquery's values (`inTri`), FALSE over no
values. So `x NOT IN (subquery)` is TRUE only when every `x = y` is FALSE: a NULL on either side
makes the condition unknown, and an empty subquery makes it TRUE, even for a NULL `x`. The
null-aware anti join (`nullAwareAntiJoin`) keeps exactly those left rows (`nullAwareAntiJoin_eq`).
-/
import WvletFormal.Rewrite.Value

namespace Wvlet.Algebra

open Wvlet.Rewrite (Value Tri)

/-- Column ids. -/
abbrev Col := Nat

/-- A row: the value of every column. Columns a relation does not have are never read. -/
abbrev Row := Col → Value

/-- A bag of rows. -/
abbrev Rel := List Row

/-- The row of NULLs that outer joins pad with. -/
def Row.null : Row := fun _ => .null

/-- The joined row of a left row `x` (columns `L`) and a right row `y`. -/
def Row.merge (L : List Col) (x y : Row) : Row := fun c => if c ∈ L then x c else y c

/-- Operands of a condition. -/
inductive Term where
  | col (c : Col)
  | lit (v : Value)
  deriving DecidableEq, Repr

def Term.eval (x : Row) : Term → Value
  | .col c => x c
  | .lit v => v

def Term.reads : Term → List Col
  | .col c => [c]
  | .lit _ => []

/-- Conditions: comparisons, NULL tests, IN over constants, and the Kleene connectives. -/
inductive Cond where
  | eq (a b : Term)
  | lt (a b : Term)
  | isNull (a : Term)
  /-- `a IN (v1, ..., vn)` over constants, NULL among them (the engine's `InList`) -/
  | inList (a : Term) (vs : List Value)
  | not (p : Cond)
  | and (p q : Cond)
  | or (p q : Cond)
  deriving DecidableEq, Repr

/-- `=`: unknown when either side is NULL. -/
def eqTri : Value → Value → Tri
  | .null, _ => .unknown
  | _, .null => .unknown
  | a, b => if a = b then .true else .false

/-- `<` over integers; unknown when either side is NULL. Other values are outside the model and
also give unknown. -/
def ltTri : Value → Value → Tri
  | .int a, .int b => if a < b then .true else .false
  | _, _ => .unknown

/-- `v IN (vs)`: the Kleene OR of `v = y`; FALSE for no values. -/
def inTri (v : Value) (vs : List Value) : Tri := vs.foldr (fun y acc => (eqTri v y).or acc) .false

def Cond.eval (x : Row) : Cond → Tri
  | .eq a b => eqTri (a.eval x) (b.eval x)
  | .lt a b => ltTri (a.eval x) (b.eval x)
  | .isNull a => if a.eval x = .null then .true else .false
  | .inList a vs => inTri (a.eval x) vs
  | .not p => (p.eval x).not
  | .and p q => (p.eval x).and (q.eval x)
  | .or p q => (p.eval x).or (q.eval x)

/-- The columns a condition reads. -/
def Cond.reads : Cond → List Col
  | .eq a b => a.reads ++ b.reads
  | .lt a b => a.reads ++ b.reads
  | .isNull a => a.reads
  | .inList a _ => a.reads
  | .not p => p.reads
  | .and p q => p.reads ++ q.reads
  | .or p q => p.reads ++ q.reads

/-- The condition is TRUE on the row. -/
def Cond.holds (p : Cond) (x : Row) : Bool := p.eval x == .true

theorem Term.eval_congr {x y : Row} (t : Term) (h : ∀ c ∈ t.reads, x c = y c) :
    t.eval x = t.eval y := by
  cases t <;> simp_all [Term.eval, Term.reads]

/-- A condition sees only the columns it reads. -/
theorem Cond.eval_congr {x y : Row} (p : Cond) (h : ∀ c ∈ p.reads, x c = y c) :
    p.eval x = p.eval y := by
  induction p with
  | eq a b =>
    simp only [Cond.reads, List.mem_append] at h
    simp only [Cond.eval, Term.eval_congr a (fun c hc => h c (Or.inl hc)),
      Term.eval_congr b (fun c hc => h c (Or.inr hc))]
  | lt a b =>
    simp only [Cond.reads, List.mem_append] at h
    simp only [Cond.eval, Term.eval_congr a (fun c hc => h c (Or.inl hc)),
      Term.eval_congr b (fun c hc => h c (Or.inr hc))]
  | isNull a =>
    simp only [Cond.eval, Term.eval_congr a h]
  | inList a vs =>
    simp only [Cond.eval, Term.eval_congr a h]
  | not p ih => simp only [Cond.eval, ih h]
  | and p q ihp ihq =>
    simp only [Cond.reads, List.mem_append] at h
    simp only [Cond.eval, ihp (fun c hc => h c (Or.inl hc)), ihq (fun c hc => h c (Or.inr hc))]
  | or p q ihp ihq =>
    simp only [Cond.reads, List.mem_append] at h
    simp only [Cond.eval, ihp (fun c hc => h c (Or.inl hc)), ihq (fun c hc => h c (Or.inr hc))]

theorem Cond.holds_congr {x y : Row} (p : Cond) (h : ∀ c ∈ p.reads, x c = y c) :
    p.holds x = p.holds y := by
  simp [Cond.holds, p.eval_congr h]

/-- `p and q` holds exactly when both hold: unknown is not TRUE. -/
theorem Cond.holds_and (p q : Cond) (x : Row) :
    (Cond.and p q).holds x = (p.holds x && q.holds x) := by
  simp only [Cond.holds, Cond.eval]
  cases p.eval x <;> cases q.eval x <;> rfl

/-- WHERE: the rows where the condition is TRUE. -/
def filter (p : Cond) (r : Rel) : Rel := r.filter p.holds

/-- The joined rows of a left row with the right rows it matches. -/
def matchesOf (L : List Col) (on : Cond) (x : Row) (rs : Rel) : Rel :=
  (rs.filter fun y => on.holds (Row.merge L x y)).map (Row.merge L x)

/-- The joined rows of a right row with the left rows it matches. -/
def matchedBy (L : List Col) (on : Cond) (ls : Rel) (y : Row) : Rel :=
  (ls.filter fun x => on.holds (Row.merge L x y)).map (fun x => Row.merge L x y)

def innerJoin (L : List Col) (on : Cond) (ls rs : Rel) : Rel :=
  ls.flatMap fun x => matchesOf L on x rs

/-- Every left row with its matches, or padded with NULLs when it has none. -/
def leftJoin (L : List Col) (on : Cond) (ls rs : Rel) : Rel :=
  ls.flatMap fun x =>
    let m := matchesOf L on x rs
    if m.isEmpty then [Row.merge L x Row.null] else m

/-- Every right row with its matches, or padded with NULLs when it has none. -/
def rightJoin (L : List Col) (on : Cond) (ls rs : Rel) : Rel :=
  rs.flatMap fun y =>
    let m := matchedBy L on ls y
    if m.isEmpty then [Row.merge L Row.null y] else m

/-- The left join plus the right rows without a match, padded with NULLs. -/
def fullJoin (L : List Col) (on : Cond) (ls rs : Rel) : Rel :=
  leftJoin L on ls rs ++
    (rs.filter fun y => (matchedBy L on ls y).isEmpty).map (Row.merge L Row.null)

/-- The left rows with a TRUE match (`EXISTS`). -/
def semiJoin (L : List Col) (on : Cond) (ls rs : Rel) : Rel :=
  ls.filter fun x => rs.any fun y => on.holds (Row.merge L x y)

/-- The left rows without a TRUE match (`NOT EXISTS`). -/
def antiJoin (L : List Col) (on : Cond) (ls rs : Rel) : Rel :=
  ls.filter fun x => !(rs.any fun y => on.holds (Row.merge L x y))

/-- Join kinds of the engine's runtime plan. -/
inductive JoinKind where
  | inner
  | left
  | right
  | full
  | semi
  | anti
  deriving DecidableEq, Repr

def join (k : JoinKind) (L : List Col) (on : Cond) (ls rs : Rel) : Rel :=
  match k with
  | .inner => innerJoin L on ls rs
  | .left => leftJoin L on ls rs
  | .right => rightJoin L on ls rs
  | .full => fullJoin L on ls rs
  | .semi => semiJoin L on ls rs
  | .anti => antiJoin L on ls rs

/-! ### IN and NOT IN over a subquery -/

/-- `x NOT IN (subquery)` as a WHERE: the left rows for which it is TRUE, the subquery's values
being column `c` of `rs`. -/
def notIn (t : Term) (c : Col) (ls rs : Rel) : Rel :=
  ls.filter fun x => (inTri (t.eval x) (rs.map (· c))).not == .true

/-- The null-aware anti join: the left rows for which `x = y` is FALSE for every right row. -/
def nullAwareAntiJoin (t : Term) (c : Col) (ls rs : Rel) : Rel :=
  ls.filter fun x => rs.all fun y => eqTri (t.eval x) (y c) == .false

theorem Tri.or_eq_true (a b : Tri) : a.or b = .true ↔ a = .true ∨ b = .true := by
  cases a <;> cases b <;> decide

theorem Tri.or_eq_false (a b : Tri) : a.or b = .false ↔ a = .false ∧ b = .false := by
  cases a <;> cases b <;> decide

/-- IN is TRUE exactly when some `v = y` is TRUE. -/
theorem inTri_eq_true (v : Value) (vs : List Value) :
    inTri v vs = .true ↔ ∃ y ∈ vs, eqTri v y = .true := by
  induction vs with
  | nil => simp [inTri]
  | cons y ys ih =>
    simp only [inTri, List.foldr_cons] at ih ⊢
    rw [Tri.or_eq_true, ih]
    simp

/-- IN is FALSE exactly when every `v = y` is FALSE. -/
theorem inTri_eq_false (v : Value) (vs : List Value) :
    inTri v vs = .false ↔ ∀ y ∈ vs, eqTri v y = .false := by
  induction vs with
  | nil => simp [inTri]
  | cons y ys ih =>
    simp only [inTri, List.foldr_cons] at ih ⊢
    rw [Tri.or_eq_false, ih]
    simp

/-- NOT IN is TRUE exactly when every `v = y` is FALSE. -/
theorem notIn_tri_true (v : Value) (vs : List Value) :
    (inTri v vs).not = .true ↔ ∀ y ∈ vs, eqTri v y = .false := by
  rw [← inTri_eq_false]
  cases inTri v vs <;> decide

/-- The null-aware anti join is `NOT IN`. -/
theorem nullAwareAntiJoin_eq (t : Term) (c : Col) (ls rs : Rel) :
    nullAwareAntiJoin t c ls rs = notIn t c ls rs := by
  unfold nullAwareAntiJoin notIn
  congr 1
  funext x
  have h := notIn_tri_true (t.eval x) (rs.map (· c))
  apply Bool.eq_iff_iff.mpr
  simp only [List.all_eq_true, beq_iff_eq, h, List.mem_map]
  constructor
  · rintro hall _ ⟨y, hy, rfl⟩; exact hall y hy
  · intro hall y hy; exact hall _ ⟨y, hy, rfl⟩

/-- `=` is TRUE only on equal non-NULL values. -/
theorem eqTri_eq_true {a b : Value} : eqTri a b = .true ↔ a ≠ .null ∧ a = b := by
  cases a <;> cases b <;> simp [eqTri] <;> split <;> simp_all

/-- `=` is unknown exactly when a side is NULL. -/
theorem eqTri_eq_unknown {a b : Value} : eqTri a b = .unknown ↔ a = .null ∨ b = .null := by
  cases a <;> cases b <;> simp [eqTri] <;> split <;> simp

/-- A joined row agrees with the left row on the left columns. -/
theorem merge_left {L : List Col} {c : Col} (x y : Row) (h : c ∈ L) :
    Row.merge L x y c = x c := by
  simp [Row.merge, h]

/-- A joined row agrees with the right row off the left columns. -/
theorem merge_right {L : List Col} {c : Col} (x y : Row) (h : c ∉ L) :
    Row.merge L x y c = y c := by
  simp [Row.merge, h]

end Wvlet.Algebra
