/-
Licensed under the Apache License, Version 2.0.

Model of the operator typing rules of `wvlet.lang.compiler.typer.TyperRules`
(wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/TyperRules.scala): `arithmeticType`,
`equalityType`, `comparisonType`, and `logicalType`, which `binaryOpRules` applies to arithmetic,
`=` / `!=`, `<` / `<=` / `>` / `>=`, and `and` / `or`. Each definition mirrors the stages of the
Scala function: errors first, then deferral, then the rules over resolved operand types.

The operand domain reuses the coercion model's `Ty` and `unify` (Coercion.lean), adding only what
the operator rules distinguish beyond it.
-/
import WvletFormal.Typer.Coercion

namespace Wvlet.Typer.Operators

open Wvlet.Typer
open Wvlet.Typer.Ty

/--
Operand and result types. `ty t` is a type of the coercion model; there `other n` stands for the
types no operator rule mentions (short, date, varchar, ...). `decimal p s` is a decimal with
known precision and scale, `decimalUnresolved` one whose precision or scale is an unbound type
parameter, `unresolved` any other data type that is not resolved yet (an unknown column, an
unbound generic type), and `fnRef` a reference to a function definition (a `FunctionType`, not a
value type).
-/
inductive Operand where
  | ty (t : Ty)
  | decimal (p s : Nat)
  | decimalUnresolved
  | unresolved
  | fnRef
  deriving DecidableEq, Repr, Inhabited

/-- `BinaryExprType`: the arithmetic operators. Only `add` has a rule of its own. -/
inductive ArithOp where
  | add
  | subtract
  | multiply
  | divide
  | divideInt
  | modulus
  deriving DecidableEq, Repr, Inhabited

namespace Operand

def isError : Operand → Bool
  | ty error => true
  | _ => false

/-- Operands whose type is not known yet: the operator result must wait for them. -/
def isDeferred : Operand → Bool
  | ty noType | decimalUnresolved | unresolved | fnRef => true
  | _ => false

/-- Operands an ordering comparison accepts as numbers. -/
def isOrderedNumeric : Operand → Bool
  | ty t => t.isNumeric
  | decimal _ _ | decimalUnresolved => true
  | _ => false

end Operand

open Operand

/-- Numeric types and NULL: the operands on which arithmetic is `unify`. -/
def numOrNull (t : Ty) : Bool := t.isNumeric || t = null

/-- The largest decimal precision of the supported engines. -/
def maxPrecision : Nat := 38

/-- The smallest decimal holding both `decimal(p, s)` and `decimal(q, t)`, capped at 38 digits. -/
def decimalLub (p s q t : Nat) : Operand :=
  decimal (min maxPrecision (max (p - s) (q - t) + max s t)) (max s t)

/-- The string-concatenation rule of `+`, or a type error. -/
def concatOrError (op : ArithOp) (l r : Operand) : Operand :=
  if op = .add && (l = ty string || r = ty string) then ty string else ty error

/-- Arithmetic over operands that are neither errors nor deferred. -/
def arithValue (op : ArithOp) : Operand → Operand → Operand
  | ty a, ty b => if numOrNull a && numOrNull b then ty (unify a b) else concatOrError op (ty a) (ty b)
  | decimal p s, decimal q t => decimalLub p s q t
  | decimal p s, ty b => if numOrNull b then decimal p s else concatOrError op (decimal p s) (ty b)
  | ty a, decimal p s => if numOrNull a then decimal p s else concatOrError op (ty a) (decimal p s)
  | l, r => concatOrError op l r

/-- `TyperRules.arithmeticType`. -/
def arith (op : ArithOp) (l r : Operand) : Operand :=
  if l.isError || r.isError then ty error
  else if l.isDeferred || r.isDeferred then ty noType
  else arithValue op l r

/-- `TyperRules.equalityType`: `=` and `!=` accept any operands. -/
def equality (_ _ : Operand) : Operand := ty boolean

/-- Operand pairs an ordering comparison accepts. -/
def comparable (l r : Operand) : Bool :=
  (l.isOrderedNumeric && r.isOrderedNumeric) || (l = ty string && r = ty string) ||
    (l = r && l ≠ ty boolean)

/-- `TyperRules.comparisonType`, for each of `<`, `<=`, `>`, `>=`. -/
def compare (l r : Operand) : Operand :=
  if l.isError || r.isError then ty error
  else if comparable l r || l.isDeferred || r.isDeferred then ty boolean
  else ty error

/-- `TyperRules.logicalType`, for `and` and `or`. -/
def logical (l r : Operand) : Operand :=
  if l.isError || r.isError then ty error
  else if l.isDeferred || r.isDeferred then ty noType
  else if l = ty boolean && r = ty boolean then ty boolean
  else ty error

/-- The sign prefix (`ArithmeticUnaryExpr`) keeps its operand type. -/
def unary (t : Operand) : Operand := t

/-! ## Operand order does not matter -/

theorem decimalLub_comm (p s q t : Nat) : decimalLub p s q t = decimalLub q t p s := by
  simp [decimalLub, Nat.max_comm]

theorem concatOrError_comm (op : ArithOp) (l r : Operand) :
    concatOrError op l r = concatOrError op r l := by
  simp [concatOrError, Bool.or_comm]

theorem arithValue_comm (op : ArithOp) (l r : Operand) :
    arithValue op l r = arithValue op r l := by
  cases l <;> cases r <;> simp [arithValue, decimalLub_comm, unify_comm, Bool.and_comm] <;>
    (try split) <;> simp_all [concatOrError_comm]

theorem arith_comm (op : ArithOp) (l r : Operand) : arith op l r = arith op r l := by
  simp [arith, Bool.or_comm, arithValue_comm]

theorem equality_comm (l r : Operand) : equality l r = equality r l := rfl

theorem comparable_comm (l r : Operand) : comparable l r = comparable r l := by
  unfold comparable
  by_cases h : l = r
  · subst h; rfl
  · have h' : r ≠ l := fun e => h e.symm
    simp [h, h', Bool.and_comm]

theorem compare_comm (l r : Operand) : compare l r = compare r l := by
  unfold compare
  rw [Bool.or_comm l.isError, comparable_comm, Bool.or_assoc, Bool.or_comm l.isDeferred,
    ← Bool.or_assoc]

theorem logical_comm (l r : Operand) : logical l r = logical r l := by
  unfold logical
  simp only [Bool.or_comm]
  split <;> (try split) <;> simp_all [Bool.and_comm]

/-! ## Numeric arithmetic is `unify` -/

/-- On numeric and NULL operands, every arithmetic operator types as `unify`. -/
theorem arith_numeric (op : ArithOp) (a b : Ty) (ha : numOrNull a = true)
    (hb : numOrNull b = true) : arith op (ty a) (ty b) = ty (unify a b) := by
  cases a <;> cases b <;> simp_all [arith, arithValue, numOrNull, isError, isDeferred, isNumeric,
    numRank]

/-- Hence the arithmetic result is the least upper bound of the operands in the widening order. -/
theorem arith_numeric_isLub (op : ArithOp) (a b : Ty) (ha : numOrNull a = true)
    (hb : numOrNull b = true) : ∃ r, arith op (ty a) (ty b) = ty r ∧ IsLub r a b :=
  ⟨unify a b, arith_numeric op a b ha hb, unify_isLub a b⟩

theorem isError_of_isDeferred (l : Operand) (h : l.isDeferred = true) : l.isError = false := by
  rcases l with t | _ | _ | _ | _ <;> (try cases t) <;> simp_all [isDeferred, isError]

/-! ## Errors propagate -/

theorem arith_error_left (op : ArithOp) (r : Operand) : arith op (ty error) r = ty error := by
  simp [arith, isError]

theorem arith_error_right (op : ArithOp) (l : Operand) : arith op l (ty error) = ty error := by
  rw [arith_comm]; exact arith_error_left op l

theorem compare_error_left (r : Operand) : compare (ty error) r = ty error := by
  simp [compare, isError]

theorem compare_error_right (l : Operand) : compare l (ty error) = ty error := by
  rw [compare_comm]; exact compare_error_left l

theorem logical_error_left (r : Operand) : logical (ty error) r = ty error := by
  simp [logical, isError]

theorem logical_error_right (l : Operand) : logical l (ty error) = ty error := by
  rw [logical_comm]; exact logical_error_left l

theorem unary_error : unary (ty error) = ty error := rfl

/-! ## Deferral: an untyped operand never commits the result -/

/-- An untyped or unresolved operand (and no error) leaves arithmetic untyped. -/
theorem arith_deferred (op : ArithOp) (l r : Operand) (hl : l.isDeferred = true)
    (hr : r.isError = false) : arith op l r = ty noType := by
  simp [arith, isError_of_isDeferred l hl, hr, hl]

theorem concatOrError_ne_noType (op : ArithOp) (l r : Operand) :
    concatOrError op l r ≠ ty noType := by
  unfold concatOrError; split <;> simp

theorem arithValue_ne_noType (op : ArithOp) (l r : Operand) (hl : l.isDeferred = false) :
    arithValue op l r ≠ ty noType := by
  rcases l with a | _ | _ | _ | _ <;> rcases r with b | _ | _ | _ | _ <;>
    simp only [arithValue, decimalLub] <;> (try split) <;>
    first
    | exact concatOrError_ne_noType _ _ _
    | (intro h
       exact unify_ne_noType a b (by rintro rfl; simp [isDeferred] at hl) (Operand.ty.inj h))
    | simp

/-- Conversely, arithmetic is untyped only while an operand is. -/
theorem arith_noType_iff (op : ArithOp) (l r : Operand) :
    arith op l r = ty noType ↔
      (l.isError = false ∧ r.isError = false ∧ (l.isDeferred = true ∨ r.isDeferred = true)) := by
  constructor
  · intro h
    unfold arith at h
    split at h
    · simp at h
    · rename_i he
      simp only [Bool.or_eq_true, not_or, Bool.not_eq_true] at he
      split at h
      · rename_i hd
        simp only [Bool.or_eq_true] at hd
        exact ⟨he.1, he.2, hd⟩
      · rename_i hd
        simp only [Bool.or_eq_true, not_or, Bool.not_eq_true] at hd
        exact absurd h (arithValue_ne_noType op l r hd.1)
  · rintro ⟨h1, h2, h3⟩
    rcases h3 with h | h <;> simp [arith, h1, h2, h]

theorem logical_deferred (l r : Operand) (hl : l.isDeferred = true) (hr : r.isError = false) :
    logical l r = ty noType := by
  simp [logical, isError_of_isDeferred l hl, hr, hl]

/-- Comparisons are boolean whatever their operand types: an untyped operand makes them boolean,
never an error. -/
theorem compare_deferred (l r : Operand) (hl : l.isDeferred = true) (hr : r.isError = false) :
    compare l r = ty boolean := by
  simp [compare, isError_of_isDeferred l hl, hr, hl]

/-- A comparison is boolean or an error, never untyped. -/
theorem compare_cases (l r : Operand) : compare l r = ty boolean ∨ compare l r = ty error := by
  unfold compare; split <;> (try split) <;> simp

/-! ## Decimal results hold both operands -/

/-- A decimal result keeps the larger scale, and, below the 38-digit cap, the larger number of
integer digits; past the cap the integer digits are what is given up. -/
theorem arith_decimal (op : ArithOp) (p s q t : Nat) :
    ∃ P S, arith op (decimal p s) (decimal q t) = decimal P S ∧ s ≤ S ∧ t ≤ S ∧ P ≤ maxPrecision ∧
      (max (p - s) (q - t) + max s t ≤ maxPrecision → p - s ≤ P - S ∧ q - t ≤ P - S) := by
  refine ⟨min maxPrecision (max (p - s) (q - t) + max s t), max s t, ?_, ?_, ?_, ?_, ?_⟩
  · simp [arith, arithValue, decimalLub, isError, isDeferred]
  · exact Nat.le_max_left s t
  · exact Nat.le_max_right s t
  · exact Nat.min_le_left _ _
  · intro h
    rw [Nat.min_eq_right h]
    omega

/-- A decimal and a number or NULL type as the decimal. -/
theorem arith_decimal_numeric (op : ArithOp) (p s : Nat) (b : Ty) (hb : numOrNull b = true) :
    arith op (decimal p s) (ty b) = decimal p s := by
  cases b <;> simp_all [arith, arithValue, numOrNull, isError, isDeferred, isNumeric, numRank]

end Wvlet.Typer.Operators
