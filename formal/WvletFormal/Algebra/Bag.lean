/-
Licensed under the Apache License, Version 2.0.

A core of relational algebra over bags with SQL three-valued logic, shared by the models of the
engine's NULL-sensitive optimizer rules.

A row maps column ids to SQL values (`Rewrite.Value`); a relation is a list of rows, so it keeps
duplicates (a bag) and an order that the rules never rely on. A condition evaluates to a `Tri`
(Kleene logic), and a filter keeps only the rows where it is TRUE: FALSE and unknown (NULL) both
drop the row.

Joins take the column ids of their left input (`L`) and a general ON condition, evaluated on the
joined row, so equality keys are a special case. A joined row reads `L` from the left row and
every other column from the right row; outer joins pad the missing side with `Row.null`, a row
whose every column is NULL. Semi and anti joins keep left rows with (without) a TRUE match: the
latter is `NOT EXISTS`, not the NULL-aware anti join of `NOT IN`.
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
  deriving Repr

def Term.eval (x : Row) : Term → Value
  | .col c => x c
  | .lit v => v

def Term.reads : Term → List Col
  | .col c => [c]
  | .lit _ => []

/-- Conditions: comparisons, NULL tests, and the Kleene connectives. -/
inductive Cond where
  | eq (a b : Term)
  | lt (a b : Term)
  | isNull (a : Term)
  | not (p : Cond)
  | and (p q : Cond)
  | or (p q : Cond)
  deriving Repr

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

def Cond.eval (x : Row) : Cond → Tri
  | .eq a b => eqTri (a.eval x) (b.eval x)
  | .lt a b => ltTri (a.eval x) (b.eval x)
  | .isNull a => if a.eval x = .null then .true else .false
  | .not p => (p.eval x).not
  | .and p q => (p.eval x).and (q.eval x)
  | .or p q => (p.eval x).or (q.eval x)

/-- The columns a condition reads. -/
def Cond.reads : Cond → List Col
  | .eq a b => a.reads ++ b.reads
  | .lt a b => a.reads ++ b.reads
  | .isNull a => a.reads
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

/-- A joined row agrees with the left row on the left columns. -/
theorem merge_left {L : List Col} {c : Col} (x y : Row) (h : c ∈ L) :
    Row.merge L x y c = x c := by
  simp [Row.merge, h]

/-- A joined row agrees with the right row off the left columns. -/
theorem merge_right {L : List Col} {c : Col} (x y : Row) (h : c ∉ L) :
    Row.merge L x y c = y c := by
  simp [Row.merge, h]

end Wvlet.Algebra
