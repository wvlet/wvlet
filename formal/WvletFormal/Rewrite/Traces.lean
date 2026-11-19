/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the expression rewrite model, exported as JSON so that a Scala test can
replay them against the shipped `RewriteExpr` rules. Each case gives a typed input expression, the
expression the phase rewrites it to (with the type of every node), and its value on DuckDB for
every row of `rows` (`phase_eval`: the rewritten expression has the same value).
-/
import WvletFormal.Rewrite.Findings

namespace Wvlet.Rewrite.Traces

open Wvlet.Rewrite

private def jstr (s : String) : String :=
  "\"" ++ String.join (s.toList.map fun c =>
    if c = '"' then "\\\"" else if c = '\\' then "\\\\" else c.toString) ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarr (items : List String) : String := "[" ++ ", ".intercalate items ++ "]"

def tyName : Ty → String
  | .string => "string"
  | .int => "int"
  | .bool => "bool"
  | .null => "null"
  | .other => "other"

def jvalue : Value → String
  | .null => "null"
  | .str s => jstr s
  | .int n => toString n
  | .bool b => if b then "true" else "false"

def jresult : Option Value → String
  | some v => jvalue v
  | none => jobj [("error", "true")]

mutual
/-- An expression with the static type of every node. -/
def jexpr : Expr → String
  | e@(.lit v) => jobj [("lit", jvalue v), ("type", jstr (tyName (typeOf e)))]
  | e@(.col n _) => jobj [("col", jstr n), ("type", jstr (tyName (typeOf e)))]
  | e@(.add l r) => jobj [("add", jarr [jexpr l, jexpr r]), ("type", jstr (tyName (typeOf e)))]
  | e@(.interp p ps) =>
    jobj [("interp", jstr p), ("parts", jarr (jargs ps)), ("type", jstr (tyName (typeOf e)))]
  | e@(.part s) => jobj [("part", jstr s), ("type", jstr (tyName (typeOf e)))]
  | e@(.call f as) =>
    jobj [("call", jstr f), ("args", jarr (jargs as)), ("type", jstr (tyName (typeOf e)))]
  | e@(.ifExpr c t f) =>
    jobj [("ifExpr", jarr [jexpr c, jexpr t, jexpr f]), ("type", jstr (tyName (typeOf e)))]
  | e@(.cast x) => jobj [("cast", jexpr x), ("type", jstr (tyName (typeOf e)))]

def jargs : Args → List String
  | .nil => []
  | .cons e rest => jexpr e :: jargs rest
end

/-- The replayed rows: the string column `s`, the int column `n`, and the boolean column `b`, each
NULL or not. -/
structure TraceRow where
  s : Value
  n : Value
  b : Value

def TraceRow.toRow (r : TraceRow) : Row := fun t _ =>
  match t with
  | .string => r.s
  | .int => r.n
  | .bool => r.b
  | _ => .null

def rows : List TraceRow :=
  [Value.null, .str "x"].flatMap fun s =>
    [Value.null, .int 7].flatMap fun n =>
      [Value.null, .bool true, .bool false].map fun b => ⟨s, n, b⟩

def s : Expr := .col "s" .string
def n : Expr := .col "n" .int
def b : Expr := .col "b" .bool
def str (x : String) : Expr := .lit (.str x)
def nul : Expr := .lit .null
def int (x : Int) : Expr := .lit (.int x)
def tru : Expr := .lit (.bool true)
def fls : Expr := .lit (.bool false)
def call (f : String) (as : List Expr) : Expr := .call f (Args.ofList as)
def sInterp (ps : List Expr) : Expr := .interp "s" (Args.ofList ps)

def atoms : List Expr := [str "a", nul, int 1, tru, s, n, b]
def strAtoms : List Expr := [str "a", s, n, nul]
def partAtoms : List Expr := [.part "p", s, n, b, nul, int 1]
def conds : List Expr := [b, tru, fls, nul]
def branches : List Expr := [str "a", s, nul]

def pairs (xs ys : List Expr) : List (Expr × Expr) := xs.flatMap fun x => ys.map (x, ·)

def lists (xs : List Expr) : Nat → List (List Expr)
  | 0 => [[]]
  | k + 1 => (lists xs k).flatMap fun l => xs.map (l ++ [·])

def cases : List Expr :=
  -- `+` on every pair of atoms, and nested `+`
  (pairs atoms atoms).map (fun (x, y) => .add x y) ++
  (pairs strAtoms strAtoms).flatMap (fun (x, y) => strAtoms.map fun z => .add (.add x y) z) ++
  -- `s"..."` with up to two parts of any kind, three parts of some, and a non-`s` prefix
  ((lists partAtoms 0 ++ lists partAtoms 1 ++ lists partAtoms 2).map sInterp) ++
  ((lists [.part "p", s, n] 3).map sInterp) ++
  [.interp "sql" (Args.ofList [.part "p", s])] ++
  -- `if(...)` with two and three arguments, and other arities
  (pairs conds [str "a", s, nul, n]).map (fun (c, t) => call "if" [c, t]) ++
  (pairs conds branches).flatMap (fun (c, t) => branches.map fun e => call "if" [c, t, e]) ++
  [call "if" [b], call "if" [b, str "a", str "z", str "y"]] ++
  -- rules inside each other, user-written functions, casts, and `IfExpr`
  [ .add (call "if" [b, s, str "z"]) (str "!"),
    call "if" [b, .add s (str "!"), .add n (str "?")],
    .add (sInterp [.part "n=", n]) s,
    sInterp [.part "v=", .add s (str "x")],
    sInterp [.part "v=", call "if" [b, s, str "no"]],
    .add (str "x") (.add (int 1) n),
    .add (.add n (int 1)) (str "x"),
    call "concat" [s, str "a"],
    call "coalesce" [s, str "d"],
    .add (call "concat" [s, str "a"]) (str "!"),
    .add (call "concat" [str "a", str "b"]) s,
    .cast n,
    .add (.cast n) (str "x"),
    .add (.cast (int 1)) (str "x"),
    .ifExpr b (str "a") s,
    .add (.ifExpr b s nul) (str "!") ]

def caseJson (e : Expr) : String :=
  let rw := phase e
  jobj [("input", jexpr e), ("rewritten", jexpr rw),
    ("duckdb", jarr (rows.map fun r => jresult (eval .duckdb r.toRow e)))]

def rowJson (r : TraceRow) : String := jobj [("s", jvalue r.s), ("n", jvalue r.n), ("b", jvalue r.b)]

/-- The fixture document. -/
def fixture : String :=
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe rewrite-expr-traces (do not edit by hand)\",\n" ++
  "  \"rows\": " ++ jarr (rows.map rowJson) ++ ",\n" ++
  "  \"cases\": [\n" ++ ",\n".intercalate (cases.map fun e => "    " ++ caseJson e) ++ "\n  ]\n" ++
  "}\n"

end Wvlet.Rewrite.Traces
