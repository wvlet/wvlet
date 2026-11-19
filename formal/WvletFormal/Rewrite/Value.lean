/-
Licensed under the Apache License, Version 2.0.

SQL values with NULL, the static types the expression rewrite rules look at, and three-valued
logic (`Tri`) for conditions. `Tri` is kept independent of the rewrite rules so that later models
(e.g. 3VL simplification rules of an optimizer) can reuse it.
-/

namespace Wvlet.Rewrite

/-- Static types of the modeled expressions. `other` is any type no rule distinguishes, and
`null` is the type of the NULL literal. `int` stands for Wvlet's integral types (`LongType`). -/
inductive Ty where
  | string
  | int
  | bool
  | null
  | other
  deriving DecidableEq, Repr

/-- The least type that holds both: NULL fits every type, and distinct types meet at `other`. -/
def Ty.join : Ty → Ty → Ty
  | .null, b => b
  | a, .null => a
  | a, b => if a = b then a else .other

@[simp] theorem Ty.join_null (a : Ty) : a.join .null = a := by cases a <;> rfl
@[simp] theorem Ty.null_join (a : Ty) : Ty.null.join a = a := by cases a <;> rfl
@[simp] theorem Ty.join_self (a : Ty) : a.join a = a := by cases a <;> rfl

/-- SQL values. -/
inductive Value where
  | null
  | str (s : String)
  | int (n : Int)
  | bool (b : Bool)
  deriving DecidableEq, Repr

namespace Value

/-- The static type of a literal. -/
def ty : Value → Ty
  | null => .null
  | str _ => .string
  | int _ => .int
  | bool _ => .bool

/-- `v` is a value of type `t`. NULL is a value of every type; every value is an `other`. -/
def fits : Value → Ty → Bool
  | null, _ => true
  | _, .other => true
  | str _, .string => true
  | int _, .int => true
  | bool _, .bool => true
  | _, _ => false

def isNull : Value → Bool
  | null => true
  | _ => false

/-- The text of a value in a string context. NULL is the empty string: Wvlet's string `+` and
`s"..."` skip NULL operands instead of following SQL's NULL-propagating `||`. -/
def text : Value → String
  | null => ""
  | str s => s
  | int n => toString n
  | bool b => if b then "true" else "false"

/-- `cast(v as varchar)`: NULL stays NULL. -/
def toVarchar : Value → Value
  | null => null
  | v => str v.text

@[simp] theorem text_str (s : String) : (str s).text = s := rfl

@[simp] theorem fits_ty (v : Value) : v.fits v.ty = true := by cases v <;> rfl
@[simp] theorem null_fits (t : Ty) : Value.null.fits t = true := rfl
@[simp] theorem fits_other (v : Value) : v.fits .other = true := by cases v <;> rfl

theorem fits_null_iff {v : Value} : v.fits .null = true ↔ v = null := by
  cases v <;> simp [fits]

theorem fits_string {v : Value} (h : v.fits .string = true) : v = null ∨ ∃ s, v = str s := by
  cases v <;> simp_all [fits]

theorem fits_join_left {v : Value} {a : Ty} (b : Ty) (h : v.fits a = true) :
    v.fits (a.join b) = true := by
  cases v <;> cases a <;> cases b <;> simp_all [fits, Ty.join]

theorem fits_join_right {v : Value} (a : Ty) {b : Ty} (h : v.fits b = true) :
    v.fits (a.join b) = true := by
  cases v <;> cases a <;> cases b <;> simp_all [fits, Ty.join]

@[simp] theorem toVarchar_fits (v : Value) : v.toVarchar.fits .string = true := by
  cases v <;> rfl

theorem toVarchar_of_fits_string {v : Value} (h : v.fits .string = true) : v.toVarchar = v := by
  rcases fits_string h with rfl | ⟨s, rfl⟩ <;> rfl

theorem toVarchar_of_ne_null {v : Value} (h : v ≠ null) : v.toVarchar = str v.text := by
  cases v <;> simp_all [toVarchar]

@[simp] theorem toVarchar_eq_null {v : Value} : v.toVarchar = null ↔ v = null := by
  cases v <;> simp [toVarchar]

@[simp] theorem text_toVarchar (v : Value) : v.toVarchar.text = v.text := by
  cases v <;> rfl

end Value

/-- Kleene's three-valued logic: SQL conditions are true, false, or unknown (NULL). -/
inductive Tri where
  | true
  | false
  | unknown
  deriving DecidableEq, Repr

namespace Tri

def not : Tri → Tri
  | .true => .false
  | .false => .true
  | .unknown => .unknown

def and : Tri → Tri → Tri
  | .false, _ => .false
  | _, .false => .false
  | .true, .true => .true
  | _, _ => .unknown

def or : Tri → Tri → Tri
  | .true, _ => .true
  | _, .true => .true
  | .false, .false => .false
  | _, _ => .unknown

/-- A condition of a boolean value; `none` for a value that is not a boolean. -/
def ofValue : Value → Option Tri
  | .bool b => some (if b then .true else .false)
  | .null => some .unknown
  | _ => none

/-- Back to a SQL value: unknown is NULL. -/
def toValue : Tri → Value
  | .true => .bool Bool.true
  | .false => .bool Bool.false
  | .unknown => .null

theorem not_not (a : Tri) : a.not.not = a := by cases a <;> rfl
theorem and_comm (a b : Tri) : a.and b = b.and a := by cases a <;> cases b <;> rfl
theorem or_comm (a b : Tri) : a.or b = b.or a := by cases a <;> cases b <;> rfl
theorem and_assoc (a b c : Tri) : (a.and b).and c = a.and (b.and c) := by
  cases a <;> cases b <;> cases c <;> rfl
theorem or_assoc (a b c : Tri) : (a.or b).or c = a.or (b.or c) := by
  cases a <;> cases b <;> cases c <;> rfl
theorem not_and (a b : Tri) : (a.and b).not = a.not.or b.not := by
  cases a <;> cases b <;> rfl
theorem not_or (a b : Tri) : (a.or b).not = a.not.and b.not := by
  cases a <;> cases b <;> rfl
theorem ofValue_toValue (a : Tri) : ofValue a.toValue = some a := by cases a <;> rfl

end Tri

/-- `if(c, t, e)` / `CASE WHEN c THEN t ELSE e END`: only a true condition takes the first branch;
false and unknown (NULL) take the second. A non-boolean condition is an error (`none`). -/
def ite3 (c : Value) (t e : Option Value) : Option Value :=
  match Tri.ofValue c with
  | some .true => t
  | some _ => e
  | none => none

theorem ite3_cases (c : Value) (t e : Option Value) :
    ite3 c t e = t ∨ ite3 c t e = e ∨ ite3 c t e = none := by
  unfold ite3
  split <;> simp

end Wvlet.Rewrite
