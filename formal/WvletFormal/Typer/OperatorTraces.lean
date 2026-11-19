/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the operator typing model, exported as JSON so that a Scala test can replay
them against the shipped `TyperRules`. The operand sample covers every case the rules distinguish:
each `Ty` constructor, two `other` types, decimals below and past the precision cap, and the
deferred operands.
-/
import WvletFormal.Typer.Operators

namespace Wvlet.Typer.OperatorTraces

open Wvlet.Typer
open Wvlet.Typer.Ty
open Wvlet.Typer.Operators
open Wvlet.Typer.Operators.Operand

/-- Fixture names of the `other n` representatives (`OperatorTypingConformanceTest` binds them). -/
def otherNames : List String := ["short", "date"]

def tyName : Ty → String
  | noType => "noType"
  | null => "null"
  | int => "int"
  | long => "long"
  | float => "float"
  | double => "double"
  | boolean => "boolean"
  | string => "string"
  | other n => otherNames.getD n s!"other{n}"
  | error => "error"

def name : Operand → String
  | ty t => tyName t
  | decimal p s => s!"decimal({p},{s})"
  | decimalUnresolved => "decimal(p,s)"
  | unresolved => "unresolved"
  | fnRef => "function"

/-- The replayed operand sample. -/
def domain : List Operand :=
  [noType, null, int, long, float, double, boolean, string].map ty ++
    (List.range otherNames.length).map (ty ∘ other) ++
    [decimal 10 2, decimal 20 4, decimal 30 0, decimal 38 10, decimalUnresolved, unresolved, fnRef,
      ty error]

def ops : List (ArithOp × String) :=
  [(.add, "+"), (.subtract, "-"), (.multiply, "*"), (.divide, "/"), (.divideInt, "//"),
    (.modulus, "%")]

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarrayLines (items : List String) : String :=
  "[\n" ++ ",\n".intercalate (items.map ("    " ++ ·)) ++ "\n  ]"

def pairs : List (Operand × Operand) := domain.flatMap fun a => domain.map (a, ·)

def pairCases (f : Operand → Operand → Operand) : List String :=
  pairs.map fun (a, b) =>
    jobj [("left", jstr (name a)), ("right", jstr (name b)), ("result", jstr (name (f a b)))]

def arithmeticCases : List String :=
  ops.flatMap fun (op, sym) =>
    pairs.map fun (a, b) =>
      jobj [("op", jstr sym), ("left", jstr (name a)), ("right", jstr (name b)),
        ("result", jstr (name (arith op a b)))]

def unaryCases : List String :=
  domain.map fun a => jobj [("operand", jstr (name a)), ("result", jstr (name (unary a)))]

/-- The fixture document. -/
def fixture : String :=
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe operator-traces (do not edit by hand)\",\n" ++
  "  \"types\": [" ++ ", ".intercalate (domain.map (jstr ∘ name)) ++ "],\n" ++
  "  \"unary\": " ++ jarrayLines unaryCases ++ ",\n" ++
  "  \"arithmetic\": " ++ jarrayLines arithmeticCases ++ ",\n" ++
  "  \"equality\": " ++ jarrayLines (pairCases equality) ++ ",\n" ++
  "  \"comparison\": " ++ jarrayLines (pairCases compare) ++ ",\n" ++
  "  \"logical\": " ++ jarrayLines (pairCases logical) ++ "\n" ++
  "}\n"

end Wvlet.Typer.OperatorTraces
