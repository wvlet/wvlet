/-
Licensed under the Apache License, Version 2.0.

What the rules did before this model (`Legacy`): concrete expressions whose meaning they changed.
Each finding is fixed by the current rules (`phase_eval`, `phase_clean`); the `fixed_*` theorems
replay the same expressions as a sanity check.
-/
import WvletFormal.Rewrite.Idem

namespace Wvlet.Rewrite.Findings

/-- Every column is NULL. -/
def nulls : Row := fun _ _ => .null

/-- Every int column is 7; other columns are NULL. -/
def sevens : Row := fun t _ => if t = .int then .int 7 else .null

/-- `'a' + x` with a string column `x`. -/
def concatNull : Expr := .add (.lit (.str "a")) (.col "x" .string)

/-- `s"id: ${x}"` with an int column `x`. -/
def interpNull : Expr := .interp "s" (.cons (.part "id: ") (.cons (.col "x" .int) .nil))

/-- `'hello ' + 2024`. -/
def concatInt : Expr := .add (.lit (.str "hello ")) (.lit (.int 2024))

/-- `2024 + ' items'`. -/
def intConcat : Expr := .add (.lit (.int 2024)) (.lit (.str " items"))

/-- `s"${x}"` with an int column `x`. -/
def interpSingle : Expr := .interp "s" (.cons (.col "x" .int) .nil)

/-- F1: with a NULL operand, the old `concat(a, x)` is NULL on Trino (and Snowflake and Hive),
while Wvlet's string `+` skips the NULL. DuckDB's `concat` skips NULLs, so it agreed. -/
theorem f1_concat_null :
    eval .trino nulls concatNull = some (.str "a") ∧
    eval .trino nulls (Legacy.phase concatNull) = some .null ∧
    eval .duckdb nulls (Legacy.phase concatNull) = some (.str "a") := by
  decide

/-- F1 for interpolation: a NULL part made the whole old `concat` NULL on Trino. -/
theorem f1_interp_null :
    eval .trino nulls interpNull = some (.str "id: ") ∧
    eval .trino nulls (Legacy.phase interpNull) = some .null := by
  decide

/-- F2: `'hello ' + 2024` became `concat('hello ', 2024)`, which Trino rejects (no cast). -/
theorem f2_no_cast :
    eval .trino nulls concatInt = some (.str "hello 2024") ∧
    eval .trino nulls (Legacy.phase concatInt) = none := by
  decide

/-- F3: `2024 + ' items'` is typed as a string but was never rewritten, so a string `+` reached
SQL. -/
theorem f3_right_string :
    typeOf intConcat = .string ∧ concatFires (Legacy.phase intConcat) = true := by
  decide

/-- F4: a single-part `s"${x}"` was returned as `x` itself: an int, not a string. -/
theorem f4_single_part :
    typeOf (Legacy.phase interpSingle) = .int ∧
    eval .duckdb sevens (Legacy.phase interpSingle) = some (.int 7) ∧
    eval .duckdb sevens interpSingle = some (.str "7") := by
  decide

/-- The current rules keep the Wvlet meaning on these expressions, on both engines. -/
theorem fixed_examples :
    eval .trino nulls (phase concatNull) = some (.str "a") ∧
    eval .trino nulls (phase interpNull) = some (.str "id: ") ∧
    eval .trino nulls (phase concatInt) = some (.str "hello 2024") ∧
    concatFires (phase intConcat) = false ∧
    eval .duckdb sevens (phase interpSingle) = some (.str "7") := by
  decide

end Wvlet.Rewrite.Findings
