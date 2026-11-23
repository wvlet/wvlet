/-
Licensed under the Apache License, Version 2.0.

Why factoring needs a faithful key and only the laws it uses: inputs where a key that identifies
different conditions (`InList.sig` before this model, and `Const.sig` before the signature audit),
or a rewrite that is a law of two-valued
logic but not of SQL's three-valued logic, returns rows other than SQL's. Columns: `a` (0), `b`
(1), `c` (2), `s` (3).
-/
import WvletFormal.FactorOr.Rule

namespace Wvlet.FactorOr.Necessity

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)

/-- `InList.sig` before the fix: the non-NULL values joined with commas. It forgets a NULL value,
and cannot tell `'x,y'` from `'x', 'y'`. Other conditions keep their structure. -/
def oldSig : Cond → Cond
  | .inList a vs => .inList a [.str (",".intercalate ((vs.filter (· != .null)).map Value.text))]
  | .not p => .not (oldSig p)
  | .and p q => .and (oldSig p) (oldSig q)
  | .or p q => .or (oldSig p) (oldSig q)
  | c => c

private def a : Term := .col 0
private def b : Term := .col 1
private def c : Term := .col 2
private def s : Term := .col 3

private def row (vs : List Value) : Row := fun i => vs.getD i .null

private def isTrue (t : Term) : Cond := .eq t (.lit (.bool true))

/-- The rows of the condition `factorConjuncts` gives with `key`, or of the condition itself when
it does not factor. -/
def factoredRows {K : Type} [DecidableEq K] (key : Cond → K) (e : Cond) (r : Rel) : Rel :=
  filter ((factorConjuncts key e).getD e) r

/-- `(a not in (1, 3) and b) or (a not in (1, 3, null) and c)`. -/
def notInCase : Cond :=
  .or (.and (.not (.inList a [.int 1, .int 3])) (isTrue b))
    (.and (.not (.inList a [.int 1, .int 3, .null])) (isTrue c))

/-- The old signature treats `a not in (1, 3)` and `a not in (1, 3, null)` as one conjunct, so it
factors `a not in (1, 3) and (b or c)`, which keeps `a = 2, c` where SQL's condition is unknown.
The condition itself as the key does not factor. -/
theorem oldSig_drops_null_value :
    factorConjuncts (K := Cond) id notInCase = none ∧
      (factoredRows oldSig notInCase [row [.int 2, .bool false, .bool true]]).length = 1 ∧
      (filter notInCase [row [.int 2, .bool false, .bool true]]).length = 0 := by
  decide

/-- `(s in ('x,y') and b) or (s in ('x', 'y') and c)`. -/
def commaCase : Cond :=
  .or (.and (.inList s [.str "x,y"]) (isTrue b)) (.and (.inList s [.str "x", .str "y"]) (isTrue c))

/-- The old signature also joins `'x,y'` and `'x', 'y'` into the same text. -/
theorem oldSig_joins_values :
    factorConjuncts (K := Cond) id commaCase = none ∧
      (factoredRows oldSig commaCase
        [row [.null, .bool false, .bool true, .str "x,y"]]).length = 1 ∧
      (filter commaCase [row [.null, .bool false, .bool true, .str "x,y"]]).length = 0 := by
  decide

/-- `Const.sig` before the signature audit: a string printed as is, so the text `'null'` reads as
a NULL literal (both were `lit(null:string)`). Other conditions keep their structure. -/
def oldLitTerm : Term → Term
  | .lit (.str "null") => .lit .null
  | t => t

def oldLitSig : Cond → Cond
  | .eq x y => .eq (oldLitTerm x) (oldLitTerm y)
  | .lt x y => .lt (oldLitTerm x) (oldLitTerm y)
  | .not p => .not (oldLitSig p)
  | .and p q => .and (oldLitSig p) (oldLitSig q)
  | .or p q => .or (oldLitSig p) (oldLitSig q)
  | c => c

/-- `(s = 'null' and b) or (s = null and c)`, the NULL typed as a string. -/
def nullTextCase : Cond :=
  .or (.and (.eq s (.lit (.str "null"))) (isTrue b)) (.and (.eq s (.lit .null)) (isTrue c))

/-- The old literal signature treats `s = 'null'` and `s = null` as one conjunct, so it factors
`s = 'null' and (b or c)`, which keeps `s = 'null', c` where SQL's condition is unknown (a
comparison with NULL is never TRUE). -/
theorem oldLitSig_reads_text_as_null :
    factorConjuncts (K := Cond) id nullTextCase = none ∧
      (factoredRows oldLitSig nullTextCase
        [row [.null, .bool false, .bool true, .str "null"]]).length = 1 ∧
      (filter nullTextCase [row [.null, .bool false, .bool true, .str "null"]]).length = 0 := by
  decide

/-- Dropping a conjunct with its negation, `(p and q) or (not p and q) = q`, is a law of
two-valued logic only: with `p` unknown, the `or` is unknown while `q` is TRUE. -/
theorem complement_unsound :
    let p : Cond := .eq a (.lit (.int 1))
    (filter (.or (.and p (isTrue b)) (.and (.not p) (isTrue b)))
        [row [.null, .bool true]]).length = 0 ∧
      (filter (isTrue b) [row [.null, .bool true]]).length = 1 := by
  decide

/-- A conjunct of one branch only is not common: `(a = 1 and b) or c` is not
`a = 1 and (b or c)`. -/
theorem not_common_unsound :
    let p : Cond := .eq a (.lit (.int 1))
    (filter (.or (.and p (isTrue b)) (isTrue c))
        [row [.int 2, .bool false, .bool true]]).length = 1 ∧
      (filter (.and p (.or (isTrue b) (isTrue c)))
        [row [.int 2, .bool false, .bool true]]).length = 0 := by
  decide

end Wvlet.FactorOr.Necessity
