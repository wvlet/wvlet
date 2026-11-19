/-
Licensed under the Apache License, Version 2.0.

The expression shapes `RewriteExpr` touches, their static types (as the Typer assigns them), and
one evaluator that gives Wvlet-only forms (string `+`, `s"..."`, the `if(...)` call) their Wvlet
meaning and SQL forms (`concat`, `coalesce`, `IfExpr`, `cast`) the meaning of the target engine.
-/
import WvletFormal.Rewrite.Value

namespace Wvlet.Rewrite

mutual
/-- Expressions. Function arguments and interpolation parts are `Args`. -/
inductive Expr where
  /-- A literal (`StringLiteral`, `LongLiteral`, `TrueLiteral`, `NullLiteral`, ...). -/
  | lit (v : Value)
  /-- A column reference typed by the Typer. -/
  | col (name : String) (ty : Ty)
  /-- `ArithmeticBinaryExpr(Add, l, r)`. -/
  | add (l r : Expr)
  /-- `InterpolatedString(prefix, parts)`. -/
  | interp (pfx : String) (parts : Args)
  /-- `StringPart`: the literal text between interpolated expressions. -/
  | part (s : String)
  /-- `FunctionApply(name, args)`. Names are matched case-insensitively in Scala; the model takes
  them lower-cased. -/
  | call (f : String) (args : Args)
  /-- `IfExpr(cond, onTrue, onFalse)`, printed as SQL `if(c, t, e)`. -/
  | ifExpr (c t e : Expr)
  /-- `Cast(e, varchar)`. -/
  | cast (e : Expr)

inductive Args where
  | nil
  | cons (e : Expr) (rest : Args)
end

namespace Args

def length : Args → Nat
  | nil => 0
  | cons _ rest => rest.length + 1

def ofList : List Expr → Args
  | [] => nil
  | e :: es => cons e (ofList es)

end Args

/-- What an engine's `concat` and `if` do. -/
structure Engine where
  name : String
  /-- `concat` skips NULL arguments (DuckDB); otherwise any NULL argument gives NULL (Trino,
  Snowflake, Hive). -/
  concatSkipsNull : Bool
  /-- `concat` accepts non-string arguments (DuckDB casts them); Trino rejects them. -/
  concatCastsArgs : Bool
  /-- The engine has a three-argument `if` function (DuckDB, Trino, Hive). -/
  hasIf : Bool

def Engine.duckdb : Engine := ⟨"duckdb", true, true, true⟩
def Engine.trino : Engine := ⟨"trino", false, false, true⟩

/-- Concatenation of texts (a right fold, so the lemmas are definitional). -/
def cat : List String → String
  | [] => ""
  | s :: ss => s ++ cat ss

/-- All values, or an error if any is an error. -/
def seq : List (Option Value) → Option (List Value)
  | [] => some []
  | o :: os => o.bind fun v => (seq os).map (v :: ·)

/-- SQL `concat(vs)` on an engine. -/
def concatValues (eng : Engine) (vs : List Value) : Option Value :=
  if !eng.concatCastsArgs && vs.any (fun v => !v.fits .string) then none
  else if eng.concatSkipsNull then some (.str (cat (vs.map Value.text)))
  else if vs.any Value.isNull then some .null
  else some (.str (cat (vs.map Value.text)))

/-- SQL `coalesce(vs)`. -/
def coalesceValues : List Value → Value
  | [] => .null
  | .null :: vs => coalesceValues vs
  | v :: _ => v

/-- Integer `+`: NULL propagates. -/
def addInt : Value → Value → Option Value
  | .int a, .int b => some (.int (a + b))
  | .null, .int _ => some .null
  | .int _, .null => some .null
  | .null, .null => some .null
  | _, _ => none

/-- The type the Typer gives a call, from its argument types. -/
def callType (f : String) (ts : List Ty) : Ty :=
  if f = "concat" then .string
  else if f = "coalesce" then ts.foldr Ty.join .null
  else if f = "if" then
    match ts with
    | [_, t] => t
    | [_, t, e] => t.join e
    | _ => .other
  else .other

/-- The value of a call, from its argument values. `if(...)` has Wvlet's (Trino's) meaning: the
two-argument form has a NULL else branch. -/
def callEval (eng : Engine) (f : String) (vs : List (Option Value)) : Option Value :=
  if f = "concat" then (seq vs).bind (concatValues eng)
  else if f = "coalesce" then (seq vs).map coalesceValues
  else if f = "if" then
    match vs with
    | [c, t] => c.bind fun cv => ite3 cv t (some .null)
    | [c, t, e] => c.bind fun cv => ite3 cv t e
    | _ => none
  else none

/-- Whether `+` is string concatenation: the Typer types it as a string when either side is. -/
def isStrAdd (l r : Ty) : Bool := l = .string || r = .string

mutual
/-- The static type of an expression. -/
def typeOf : Expr → Ty
  | .lit v => v.ty
  | .col _ t => t
  | .add l r => if isStrAdd (typeOf l) (typeOf r) then .string else .int
  | .interp _ _ => .string
  | .part _ => .string
  | .call f args => callType f (argTypes args)
  | .ifExpr _ t e => (typeOf t).join (typeOf e)
  | .cast _ => .string

def argTypes : Args → List Ty
  | .nil => []
  | .cons e rest => typeOf e :: argTypes rest
end

/-- A row binds each column of each type; well-typed rows hold values of the column's type. -/
abbrev Row := Ty → String → Value

def Row.WellTyped (row : Row) : Prop := ∀ t n, (row t n).fits t = true

mutual
/-- The value of an expression on an engine and a row; `none` is an error. -/
def eval (eng : Engine) (row : Row) : Expr → Option Value
  | .lit v => some v
  | .col n t => some (row t n)
  | .add l r =>
    if isStrAdd (typeOf l) (typeOf r) then
      (eval eng row l).bind fun a => (eval eng row r).map fun b => .str (a.text ++ b.text)
    else
      (eval eng row l).bind fun a => (eval eng row r).bind fun b => addInt a b
  | .interp pfx parts =>
    if pfx = "s" then (seq (evalEach eng row parts)).map fun vs => .str (cat (vs.map Value.text))
    else none
  | .part s => some (.str s)
  | .call f args => callEval eng f (evalEach eng row args)
  | .ifExpr c t e =>
    if eng.hasIf then (eval eng row c).bind fun cv => ite3 cv (eval eng row t) (eval eng row e)
    else none
  | .cast e => (eval eng row e).map Value.toVarchar

def evalEach (eng : Engine) (row : Row) : Args → List (Option Value)
  | .nil => []
  | .cons e rest => eval eng row e :: evalEach eng row rest
end

end Wvlet.Rewrite
