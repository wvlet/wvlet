/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the coercion model, exported as JSON so that a Scala test can replay them
against the shipped `TypeInference`. Each `other n` of the model is bound to one concrete Scala
type that no coercion rule mentions; `error` stands for any `ErrorType`.
-/
import WvletFormal.Typer.Coercion

namespace Wvlet.Typer.Traces

open Wvlet.Typer
open Wvlet.Typer.Ty

/-- Fixture names of the `other n` representatives (`TypeCoercionConformanceTest` binds them). -/
def otherNames : List String := ["short", "date", "decimal(10,2)", "decimal(5,1)", "varchar(10)"]

def name : Ty → String
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

/-- The replayed domain. -/
def domain : List Ty :=
  [noType, null, int, long, float, double, boolean, string] ++
    (List.range otherNames.length).map other ++ [error]

private def jstr (s : String) : String := "\"" ++ s ++ "\""
private def jbool (b : Bool) : String := if b then "true" else "false"

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarrayLines (items : List String) : String :=
  "[\n" ++ ",\n".intercalate (items.map ("    " ++ ·)) ++ "\n  ]"

private def jtypes (ts : List Ty) : String :=
  "[" ++ ", ".intercalate (ts.map (jstr ∘ name)) ++ "]"

def pairs : List (Ty × Ty) := domain.flatMap fun a => domain.map (a, ·)

def unifyCases : List String :=
  pairs.map fun (a, b) =>
    jobj [("a", jstr (name a)), ("b", jstr (name b)), ("result", jstr (name (unify a b)))]

def canCoerceCases : List String :=
  pairs.map fun (a, b) =>
    jobj [("from", jstr (name a)), ("to", jstr (name b)), ("result", jbool (canCoerce a b))]

def widenCases : List String :=
  pairs.map fun (a, b) =>
    jobj [("from", jstr (name a)), ("to", jstr (name b)), ("result", jstr (name (widen a b)))]

/-- Every list of length 0 to 3 over the domain. -/
def lists : List (List Ty) :=
  let l1 := domain.map ([·])
  let l2 := l1.flatMap fun l => domain.map (l ++ [·])
  let l3 := l2.flatMap fun l => domain.map (l ++ [·])
  [[]] ++ l1 ++ l2 ++ l3

def commonTypeCases : List String :=
  lists.map fun ts => jobj [("types", jtypes ts), ("result", jstr (name (findCommonType ts)))]

/-- The fixture document. -/
def fixture : String :=
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe typer-traces (do not edit by hand)\",\n" ++
  "  \"types\": " ++ jtypes domain ++ ",\n" ++
  "  \"unify\": " ++ jarrayLines unifyCases ++ ",\n" ++
  "  \"canCoerce\": " ++ jarrayLines canCoerceCases ++ ",\n" ++
  "  \"widen\": " ++ jarrayLines widenCases ++ ",\n" ++
  "  \"findCommonType\": " ++ jarrayLines commonTypeCases ++ "\n" ++
  "}\n"

end Wvlet.Typer.Traces
