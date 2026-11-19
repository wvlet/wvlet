/-
Licensed under the Apache License, Version 2.0.

Conformance traces for SELECT-block folding, exported as JSON so that a Scala test can replay them
against `SqlGenerator`:
- `decisions`: every block shape that folding can reach, with the operator kinds it accepts.
- `pipelines`: every pipeline of up to three operators with the blocks `fold` builds, innermost
  block first, each block listing its operators in pipeline order.
-/
import WvletFormal.SqlFold.Model

namespace Wvlet.SqlFold.Traces

open Wvlet.SqlFold

private def jstr (s : String) : String := "\"" ++ s ++ "\""
private def jbool (b : Bool) : String := if b then "true" else "false"

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarrayLines (items : List String) : String :=
  "[\n" ++ ",\n".intercalate (items.map ("    " ++ ·)) ++ "\n  ]"

def kindName : Kind → String
  | .filter true => "filter"
  | .filter false => "filterUnsafe"
  | .having => "having"
  | .project => "project"
  | .distinct => "distinct"
  | .dedup => "dedup"
  | .group => "group"
  | .agg => "agg"
  | .sort true => "sort"
  | .sort false => "sortUnsafe"
  | .limit => "limit"
  | .offset => "offset"

def kinds : List Kind :=
  [.filter true, .filter false, .having, .project, .distinct, .dedup, .group, .agg, .sort true,
    .sort false, .limit, .offset]

def shapeJson (s : Shape) : String :=
  jobj [("where", jbool s.where_), ("whereSafe", jbool s.whereSafe), ("group", jbool s.group),
    ("having", jbool s.having), ("select", jbool s.select), ("distinct", jbool s.distinct),
    ("orderBy", jbool s.orderBy), ("offset", jbool s.offset), ("limit", jbool s.limit)]

/-- One fold step: the shapes a block can move to. -/
def successors (s : Shape) : List Shape :=
  kinds.map fun k => if accepts s k then s.add k else ({} : Shape).add k

/-- Shapes reachable from the empty block (the shape space is finite, so fuel suffices). -/
def reachable : Nat → List Shape → List Shape → List Shape
  | 0, seen, _ => seen
  | _ + 1, seen, [] => seen
  | fuel + 1, seen, s :: todo =>
    let next := (successors s).filter (· ∉ seen)
    let next := next.foldl (fun acc t => if t ∈ acc then acc else acc ++ [t]) []
    reachable fuel (seen ++ next) (todo ++ next)

def shapes : List Shape := reachable 10000 [{}] [{}]

private def jkinds (ks : List Kind) : String :=
  "[" ++ ", ".intercalate (ks.map (jstr ∘ kindName)) ++ "]"

/-- Each reachable shape with the operator kinds it accepts; it refuses all others. -/
def decisionCases : List String :=
  shapes.map fun s =>
    jobj [("shape", shapeJson s), ("accepts", jkinds (kinds.filter (accepts s)))]

/-- Blocks of a pipeline (last operator first), outermost first, operators in pipeline order. -/
def partitionRev : Shape → List Kind → List Kind → List (List Kind)
  | _, cur, [] => [cur]
  | s, cur, k :: rest =>
    if accepts s k then partitionRev (s.add k) (k :: cur) rest
    else cur :: partitionRev (({} : Shape).add k) [k] rest

/-- Blocks of a pipeline, innermost block first. -/
def partition (ks : List Kind) : List (List Kind) := (partitionRev {} [] ks.reverse).reverse

/-- HAVING stands for a filter directly over a bare group by. -/
def validAfter : Option Kind → List Kind → Bool
  | _, [] => true
  | prev, k :: rest => (k != .having || prev == some .group) && validAfter (some k) rest

def valid (ks : List Kind) : Bool := validAfter none ks

def pipelines : List (List Kind) :=
  let one := kinds.map ([·])
  let two := kinds.flatMap fun a => kinds.map fun b => [a, b]
  let three := kinds.flatMap fun a => kinds.flatMap fun b => kinds.map fun c => [a, b, c]
  (one ++ two ++ three).filter valid

def pipelineCases : List String :=
  pipelines.map fun ks =>
    jobj [("ops", jkinds ks),
      ("blocks", "[" ++ ", ".intercalate ((partition ks).map jkinds) ++ "]")]

def fixture : String :=
  "{\n  \"decisions\": " ++ jarrayLines decisionCases ++ ",\n  \"pipelines\": " ++
    jarrayLines pipelineCases ++ "\n}\n"

end Wvlet.SqlFold.Traces
