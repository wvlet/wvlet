/-
Licensed under the Apache License, Version 2.0.

Conformance traces for EXISTS / IN decorrelation, exported as JSON so that a Scala test can replay
them against the engine. An outer table with NULL and duplicate rows meets an inner table with
NULL keys, NULL values, and duplicate matches, through subqueries whose WHERE has a key equality,
a key and a residual, a key and a local filter, only a residual, two keys, and an OR of two
correlations. Each case gives the rows of the engine's plan (`planExists`, `planIn`, `markJoin`), which
`Rule.lean` proves equal to SQL's. A mark case also gives each outer row with its flag, which
`markJoin_sound` proves is SQL's EXISTS (never unknown): the value of `exists { ... }` as a select
item.

It also exports the semi and anti join cases of the filter pushdown model (`Pushdown.place` and
`Pushdown.rewrite`, proved by `rewrite_sound`): a WHERE conjunct next to an EXISTS or NOT EXISTS,
over the pushdown fixture's tables.

Columns: `a`, `b` on the outer table (ids 0, 1), `c`, `d` on the inner table (ids 2, 3).
-/
import WvletFormal.Decorrelate.Necessity
import WvletFormal.Pushdown.Traces

namespace Wvlet.Decorrelate.Traces

open Wvlet.Algebra
open Wvlet.Rewrite (Value Tri)
open Wvlet.Pushdown.Traces (jvalue condWv termWv mkRow)

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

def L : List Col := [0, 1]
def R : List Col := [2, 3]

private def int (n : Int) : Term := .lit (.int n)
private def a : Term := .col 0
private def b : Term := .col 1
private def c : Term := .col 2
private def d : Term := .col 3

private def v (n : Int) : Value := .int n

def leftRows : List (List Value) :=
  [[v 1, v 10], [v 1, v 10], [v 2, .null], [.null, v 30], [v 3, v 40], [v 4, v 5]]

/-- Inner rows, at columns 2 and 3. -/
def rightRows : List (List Value) :=
  [[v 1, v 100], [v 1, .null], [v 1, v 100], [v 2, v 5], [v 4, v 5], [.null, v 30], [v 3, .null], [v 5, v 40]]

def ls : Rel := leftRows.map mkRow
def rs : Rel := rightRows.map fun r => mkRow ([.null, .null] ++ r)

def jrow (x : Row) : String := jarr (L.map fun i => jvalue (x i))

def jwhere (ws : List Cond) : String := jstr (" and ".intercalate (ws.map fun w => s!"({condWv w})"))

/-- Subquery WHEREs as conjunct lists. -/
def existsWheres : List (List Cond) :=
  [ [.eq c a]
  , [.eq c a, .lt b d]
  , [.eq c a, .not (.isNull d)]
  , [.lt b d]
  , [.eq d b]
  , [.eq c a, .eq d b]
  , [.eq c a, .not (.eq d (int 100))]
  , [.or (.eq c a) (.eq d b)] ]

def jexists (ws : List Cond) : String :=
  jobj [ ("where", jwhere ws)
       , ("existsRows", jarr ((planExists L R ws false ls rs).map jrow))
       , ("notExistsRows", jarr ((planExists L R ws true ls rs).map jrow)) ]

/-- IN cases: the target, the value column, and the subquery's WHERE. -/
def inCases : List (Term × Col × List Cond) :=
  [ (a, 2, [])
  , (a, 2, [.lt b d])
  , (a, 2, [.eq d b])
  , (b, 3, [.eq c a])
  , (b, 3, [.eq c a, .not (.isNull d)]) ]

def jin : Term × Col × List Cond → String
  | (t, col, ws) =>
    jobj [ ("target", jstr (termWv t))
         , ("value", jstr (termWv (.col col)))
         , ("where", jwhere ws)
         , ("rows", jarr ((planIn L R t col ws ls rs).map jrow)) ]

/-- EXISTS marks: the keys (`outer = inner`), the subquery's local conjuncts, and conditions over
the outer row and the flag. -/
def markWheres : List (Keys × List Cond) :=
  [ ([(a, c)], [])
  , ([(a, c)], [.not (.isNull d)])
  , ([(b, d)], [])
  , ([(a, c), (b, d)], []) ]

def flag (e : Bool) : Tri := if e then .true else .false

private def isNullTri (t : Term) (x : Row) : Tri := (Cond.isNull t).eval x
private def eqTriAt (t : Term) (n : Int) (x : Row) : Tri := (Cond.eq t (int n)).eval x

/-- Conditions with `exists (...)` in them, in Wvlet syntax with `{E}` for the EXISTS. -/
def markConds : List (String × (Row → Bool → Tri)) :=
  [ ("b is null or {E}", fun x e => (isNullTri b x).or (flag e))
  , ("{E} or a = 2", fun x e => (flag e).or (eqTriAt a 2 x))
  , ("not ({E} or b is null)", fun x e => ((flag e).or (isNullTri b x)).not)
  , ("not ({E} and a = 1)", fun x e => ((flag e).and (eqTriAt a 1 x)).not) ]

/-- The subquery's WHERE of a mark: `inner = outer` per key, then the local conjuncts. -/
def markWhere (ks : Keys) (lws : List Cond) : List Cond :=
  ks.map (fun (o, i) => Cond.eq i o) ++ lws

def jmark : Keys × List Cond → String
  | (ks, lws) =>
    let marked := markJoin ks (filter (andAll lws) rs) ls
    jobj [ ("where", jwhere (markWhere ks lws))
         , ("flags", jarr (marked.map fun (x, e) =>
              jarr (L.map (fun i => jvalue (x i)) ++ [if e then "true" else "false"])))
         , ("conds", jarr (markConds.map fun (s, f) =>
              jobj [ ("cond", jstr s)
                   , ("rows", jarr ((marked.filter fun (x, e) => f x e == .true).map (jrow ·.1))) ])) ]

/-! ### The pushdown model's semi and anti join cases -/

def pushdownKinds : List JoinKind := [.semi, .anti]

/-- The pushdown fixture's conjuncts that a semi or anti join's output can have: over the outer
input or no column. -/
def pushdownConjuncts : List Cond :=
  Pushdown.Traces.conjuncts.filter fun p => Pushdown.readsOnly p Pushdown.Traces.L

def jpushdown (k : JoinKind) (on p : Cond) : String :=
  jobj [ ("kind", jstr (Pushdown.Traces.kindName k))
       , ("on", jstr (condWv on))
       , ("where", jstr (condWv p))
       , ("place", jstr (Pushdown.Traces.placeName
            (Pushdown.place k Pushdown.Traces.L Pushdown.Traces.R p)))
       , ("trueConstant", if p.reads.isEmpty && p.holds Row.null then "true" else "false")
       , ("rows", jarr ((Pushdown.rewrite k Pushdown.Traces.L Pushdown.Traces.R on p
            Pushdown.Traces.ls Pushdown.Traces.rs).map jrow)) ]

def fixture : String :=
  let pushdown := pushdownKinds.flatMap fun k =>
    Pushdown.Traces.ons.flatMap fun on => pushdownConjuncts.map fun p => jpushdown k on p
  jobj [ ("left", jarr (leftRows.map fun r => jarr (r.map jvalue)))
       , ("right", jarr (rightRows.map fun r => jarr (r.map jvalue)))
       , ("exists", "[\n  " ++ ",\n  ".intercalate (existsWheres.map jexists) ++ "\n]")
       , ("in", "[\n  " ++ ",\n  ".intercalate (inCases.map jin) ++ "\n]")
       , ("marks", "[\n  " ++ ",\n  ".intercalate (markWheres.map jmark) ++ "\n]")
       , ("pushdownLeft", jarr (Pushdown.Traces.leftRows.map fun r => jarr (r.map jvalue)))
       , ("pushdownRight", jarr (Pushdown.Traces.rightRows.map fun r => jarr (r.map jvalue)))
       , ("pushdown", "[\n  " ++ ",\n  ".intercalate pushdown ++ "\n]") ] ++ "\n"

end Wvlet.Decorrelate.Traces
